
# -*- coding: utf-8 -*-
import os
import sys
import json
import re
import datetime
from pathlib import Path

import requests
from PIL import Image, ImageDraw
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

BASE_DIR = Path(__file__).resolve().parent
SNAP_DIR = BASE_DIR / "snapshots"
CONFIG_PATH = BASE_DIR / "config.json"

# GitHub Secrets（必須）
BASE_URL = os.getenv("BASE_URL")  # 例: "https://saitama.rsv.ws-scs.jp/web/"
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

TITLE_FOR_DISCORD = "岩槻0"  # Discordの投稿タイトル

STATUS_RANK = {"×": 0, "△": 1, "○": 2, "〇": 2}  # 改善度評価（○/〇は同値）

def ensure_dirs():
    SNAP_DIR.mkdir(parents=True, exist_ok=True)

def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def try_click_labels(page, labels, timeout_ms=15000):
    for label in labels:
        clicked = False
        for locator in [
            page.get_by_role("link", name=label, exact=True),
            page.get_by_role("button", name=label, exact=True),
            page.get_by_text(label, exact=True),
            page.locator(f"text={label}")
        ]:
            try:
                locator.wait_for(timeout=timeout_ms)
                locator.click(timeout=timeout_ms)
                page.wait_for_load_state("networkidle", timeout=30000)
                clicked = True
                break
            except Exception:
                continue
        if not clicked:
            raise RuntimeError(f"クリック対象が見つかりませんでした：『{label}』")

def try_click_optional(page, labels, timeout_ms=3000):
    for label in labels:
        for locator in [
            page.get_by_role("button", name=label, exact=True),
            page.get_by_text(label, exact=True),
            page.locator(f"text={label}")
        ]:
            try:
                locator.click(timeout=timeout_ms)
                page.wait_for_load_state("networkidle", timeout=10000)
                break
            except Exception:
                pass

def get_current_year_month_text(page):
    """
    ページ上の見出しから 'YYYY年M月' を推定して返す（存在しない場合は None）。
    """
    text = page.inner_text("body")
    m = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月", text)
    if m:
        return f"{m.group(1)}年{int(m.group(2))}月"
    return None

def locate_calendar_root(page, hint):
    """
    カレンダー本体（当月の空き状況）と思しきコンテナ要素を返す。
    - よくあるパターンとして、表（table）やグリッド（role=grid）を探索。
    """
    candidates = []
    # 役割 or 構造で探す
    for sel in [
        "[role='grid']",
        "table",
        "div.calendar",
        "section"
    ]:
        loc = page.locator(sel)
        count = loc.count()
        for i in range(count):
            el = loc.nth(i)
            try:
                t = el.inner_text()
                if hint in t or re.search(r"空き|状況|予約|カレンダー", t):
                    candidates.append(el)
            except Exception:
                continue

    # 最終的に最もテキスト量が多いものを選ぶ（粗いが実用的）
    best = None
    best_len = -1
    for el in candidates:
        try:
            t = el.inner_text()
            if len(t) > best_len:
                best_len = len(t)
                best = el
        except Exception:
            continue

    # 見つからなければドキュメント全体を返す（フォールバック）
    return best or page.locator("body")

def extract_status_cells(page, calendar_root, status_chars):
    """
    カレンダー内から『× / △ / ○ / 〇』を含むセル（またはスロット）を抽出。
    戻り値：[{key, status, bbox, text}]
      - key: セル識別子（テキスト断片＋座標で擬似キー化）
      - status: 記号
      - bbox: カレンダーコンテナ基準の矩形（x, y, width, height）
      - text: セル表示テキスト（日時などの断片）
    """
    # まずカレンダー要素の座標
    cal_bbox = calendar_root.bounding_box() or {"x": 0, "y": 0}
    cal_x, cal_y = cal_bbox.get("x", 0), cal_bbox.get("y", 0)

    cells = []

    # 1) 文字として表れる記号
    pattern = "|".join([re.escape(ch) for ch in status_chars])
    text_locator = page.locator(f"xpath=//*[contains(text(), '{status_chars[0]}') or contains(text(), '{status_chars[1]}') or contains(text(), '{status_chars[2]}') or contains(text(), '{status_chars[3]}')]")
    # ElementHandle の配列で取る
    text_elems = text_locator.element_handles()

    # 2) alt属性に記号を含む画像
    alt_locator = page.locator("img[alt*='×'], img[alt*='△'], img[alt*='○'], img[alt*='〇']")
    alt_elems = alt_locator.element_handles()

    all_elems = text_elems + alt_elems

    for el in all_elems:
        try:
            raw_text = (el.inner_text() or "") + " " + (el.get_attribute("alt") or "")
            # どの記号が含まれているかを決める（優先順：○/〇 > △ > × の見つかった最初）
            found = None
            for ch in ["○", "〇", "△", "×"]:
                if ch in raw_text:
                    found = ch
                    break
            if not found:
                continue

            # 近いセルコンテナ（td/gridcell/liなど）を取得
            container = el.evaluate_handle("el => el.closest('td,div[role=\"gridcell\"],li,div')")
            base = el
            if container:
                try:
                    base = container.as_element()
                except Exception:
                    pass

            bbox = base.bounding_box()
            if not bbox:
                continue

            # カレンダー基準へ変換
            rel_x = max(0, bbox["x"] - cal_x)
            rel_y = max(0, bbox["y"] - cal_y)

            # セル表示テキスト（断片）
            text_snippet = (base.inner_text() or raw_text).strip()
            text_snippet = re.sub(r"\s+", " ", text_snippet)
            # 擬似キー（位置を粗く量子化＋テキスト断片）
            key = f"{int(rel_x/10)}-{int(rel_y/10)}:{text_snippet[:40]}"

            cells.append({
                "key": key,
                "status": found,
                "bbox": {"x": rel_x, "y": rel_y, "w": bbox["width"], "h": bbox["height"]},
                "text": text_snippet
            })
        except Exception:
            continue

    return cells, cal_bbox

def take_calendar_screenshot(calendar_root, out_path):
    """
    カレンダー要素のみをスクリーンショット（画像ファイル）に保存。
    """
    calendar_root.scroll_into_view_if_needed()
    calendar_root.screenshot(path=str(out_path))

def draw_highlights_on_image(image_path, cells_to_highlight, alpha=160, border_width=3):
    """
    画像に黄色ハイライトを描画（cells_to_highlight の bbox は画像基準）。
    """
    img = Image.open(image_path).convert("RGBA")
    overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)

    for c in cells_to_highlight:
        x, y = int(c["bbox"]["x"]), int(c["bbox"]["y"])
        w, h = int(c["bbox"]["w"]), int(c["bbox"]["h"])
        # 半透明黄色塗り＋境界線
        draw.rectangle([x, y, x + w, y + h], fill=(255, 255, 0, alpha), outline=(255, 255, 0, 255), width=border_width)

    out_img = Image.alpha_composite(img, overlay).convert("RGB")
    out_img.save(image_path)

def load_latest_state():
    state_path = SNAP_DIR / "latest_state.json"
    month_path = SNAP_DIR / "latest_month.txt"
    state = {}
    month = None
    if state_path.exists():
        try:
            state = json.loads(state_path.read_text("utf-8"))
        except Exception:
            state = {}
    if month_path.exists():
        try:
            month = month_path.read_text("utf-8").strip()
        except Exception:
            month = None
    return state, month

def save_latest_state(state, month):
    (SNAP_DIR / "latest_state.json").write_text(json.dumps(state, ensure_ascii=False, indent=2), "utf-8")
    (SNAP_DIR / "latest_month.txt").write_text(month or "", "utf-8")

def send_to_discord(image_path, improved_cells_summary):
    if not DISCORD_WEBHOOK_URL:
        raise RuntimeError("DISCORD_WEBHOOK_URL が未設定です。GitHub Secrets または環境変数で設定してください。")
    now = datetime.datetime.now()
    jst_str = now.strftime("%Y-%m-%d %H:%M:%S JST")
    content = TITLE_FOR_DISCORD + "\n" + f"改善を検知しました（{jst_str}）。\n" + improved_cells_summary

    files = {"file": open(image_path, "rb")}
    data = {"content": content}
    resp = requests.post(DISCORD_WEBHOOK_URL, data=data, files=files, timeout=30)
    if resp.status_code >= 300:
        raise RuntimeError(f"Discord送信に失敗しました: HTTP {resp.status_code} {resp.text}")

def main():
    if not BASE_URL:
        raise RuntimeError("BASE_URL が未設定です。GitHub Secrets で https://saitama.rsv.ws-scs.jp/web/ を設定してください。")
    ensure_dirs()
    config = load_config()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1600, "height": 1200})
        page = context.new_page()

        # トップへ
        page.goto(BASE_URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_load_state("networkidle", timeout=60000)

        # 必要に応じて同意/確認ダイアログを閉じる
        try_click_optional(page, config.get("optional_clicks", []))

        # 手順に従って遷移
        try_click_labels(page, config["click_sequence"], timeout_ms=15000)

        # 当月の年月テキスト取得
        current_month_text = get_current_year_month_text(page) or ""

        # カレンダーコンテナ抽出
        cal_root = locate_calendar_root(page, config.get("calendar_root_hint", "空き状況"))

        # ステータス抽出
        cells, cal_bbox = extract_status_cells(page, cal_root, config["status_chars"])

        # スクショ（カレンダー要素のみ）
        calendar_png = SNAP_DIR / "latest_calendar.png"
        take_calendar_screenshot(cal_root, calendar_png)

        # 前回状態読み込み
        latest_state, latest_month = load_latest_state()

        # 月が変わっていたら、通知せず初期化（当月のみ監視）
        if latest_month and current_month_text and (latest_month != current_month_text):
            # 上書き保存して終了
            new_state = {c["key"]: c["status"] for c in cells}
            save_latest_state(new_state, current_month_text)
            context.close()
            browser.close()
            return

        # 改善判定（×→△/○、△→○）
        improvements = []
        for c in cells:
            prev = latest_state.get(c["key"])
            curr = c["status"]
            if prev is None:
                # 新規セルは「月跨ぎ」や構成変化の可能性があるので通知対象外（安定後に検知させる）
                continue
            try:
                if STATUS_RANK[curr] > STATUS_RANK[prev]:
                    # 改善のみ検知
                    improvements.append(c)
            except KeyError:
                # 未知記号は無視
                continue

        # 状態更新（常に最新へ）
        new_state = {c["key"]: c["status"] for c in cells}
        save_latest_state(new_state, current_month_text or (latest_month or ""))

        if improvements:
            # 黄色ハイライト描画（カレンダー画像上の座標で描画）
            draw_highlights_on_image(str(calendar_png), improvements,
                                     alpha=config.get("highlight_alpha", 160),
                                     border_width=config.get("highlight_border_width", 3))

            # 概要テキスト（件数と、上限数件のテキスト抜粋）
            summaries = []
            for c in improvements[:10]:
                # セル断片（短縮）
                t = c["text"]
                t = re.sub(r"\s+", " ", t)
                summaries.append(f"- {t[:60]} ...：{latest_state.get(c['key'])} → {c['status']}")
            summary_text = f"改善セル数: {len(improvements)}\n" + "\n".join(summaries)

            # Discordへ送付（タイトルは『岩槻0』）
            send_to_discord(str(calendar_png), summary_text)

        context.close()
        browser.close()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)
