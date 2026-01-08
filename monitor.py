# -*- coding: utf-8 -*-
"""
さいたま市 施設予約システムの空き状況監視（改善のみ通知）
- .jsp 直リンク禁止のため毎回トップからクリック遷移
- 施設×月を巡回し、×→△/○、△→○ の「改善」だけ検知
- 改善セルは黄色ハイライトで強調した画像を Discord に投稿（タイトル：施設短縮名+月番号）
- 認識ロジック：テキスト直記号／img属性／ARIA／CSS背景画像／CSSクラス（多段検知）
- ヘッダ・大見出しを除外する「実セル判定」を追加（looks_like_day_cell）
- カレンダー HTML を保存（snapshots/.../calendar.html）して、検知根拠を可視化
- 進捗ログ（施設到達・月抽出開始・件数サマリ）を必ず出力
- 「翌月」ボタンの表記揺れや画像ボタン、プルダウン選択までフォールバック
"""

import os
import sys
import json
import re
import datetime
from pathlib import Path

import requests
from PIL import Image, ImageDraw
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# （任意）JST時間帯チェック用
try:
    import pytz
except Exception:
    pytz = None

# === 環境変数（GitHub Secrets で設定） ===
BASE_URL = os.getenv("BASE_URL")  # 例: "https://saitama.rsv.ws-scs.jp/web/"
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

# === ファイルパス ===
BASE_DIR = Path(__file__).resolve().parent
SNAP_DIR = BASE_DIR / "snapshots"
CONFIG_PATH = BASE_DIR / "config.json"

# === 施設短縮名（Discordタイトル用） ===
FACILITY_TITLE_ALIAS = {
    "岩槻南部公民館": "岩槻",
    "南浦和コミュニティセンター": "南浦和",
    "岸町公民館": "岸町",
    "鈴谷公民館": "鈴谷",
}

# === ステータスの序列（改善判定に使用） ===
STATUS_RANK = {"×": 0, "△": 1, "○": 2, "〇": 2}


# --------------------------------------------------------------------------------
# 基本ユーティリティ
# --------------------------------------------------------------------------------
def ensure_dirs():
    SNAP_DIR.mkdir(parents=True, exist_ok=True)


def load_config():
    try:
        return json.loads(CONFIG_PATH.read_text("utf-8"))
    except Exception as e:
        print(f"[ERROR] config.json の読み込みに失敗: {e}", flush=True)
        raise


def jst_now():
    if pytz is None:
        return datetime.datetime.now()
    jst = pytz.timezone("Asia/Tokyo")
    return datetime.datetime.now(jst)


def is_within_monitoring_window(start_hour=5, end_hour=23):
    """
    JSTで 05:00〜23:59 を監視対象にする
    - Workflow側でも時刻制御しているため、ここは保険（True固定でも可）
    """
    try:
        now = jst_now()
        return start_hour <= now.hour <= end_hour
    except Exception:
        return True  # 失敗時は実行


# --------------------------------------------------------------------------------
# Playwright 操作（遷移）
# --------------------------------------------------------------------------------


def try_click_text(page, label, timeout_ms=15000, quiet=True):
    """
    指定ラベルのリンク／ボタン／テキストをクリック。
    厳密一致を優先しつつ、フォールバックで text= を使う。
    quiet=True のとき、見つからない・待機タイムアウトでも WARN を出さない（通常ケースのノイズ抑制）。
    """
    locators = [
        page.get_by_role("link", name=label, exact=True),
        page.get_by_role("button", name=label, exact=True),
        page.get_by_text(label, exact=True),
        page.locator(f"text={label}"),
    ]
    for locator in locators:
        try:
            locator.wait_for(timeout=timeout_ms)
            locator.click(timeout=timeout_ms)
            page.wait_for_load_state("networkidle", timeout=30000)
            return True
        except Exception as e:
            if not quiet:
                print(f"[WARN] try_click_text: 例外 {e}（label='{label}'）", flush=True)
            continue
    return False


def navigate_to_facility(page, facility):
    """
    トップへ → click_sequence の順で施設の当月ページまで到達
    （鈴谷は click_sequence に「すべて」を含める）
    """
    if not BASE_URL:
        raise RuntimeError("BASE_URL が未設定です。Secrets の BASE_URL に https://saitama.rsv.ws-scs.jp/web/ を入れてください。")

    # トップへ
    page.goto(BASE_URL, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_load_state("networkidle", timeout=60000)

    # 任意のダイアログ（同意など）がある場合のフォールバック
    for opt in ["同意する", "OK", "確認", "閉じる"]:
        try_click_text(page, opt, timeout_ms=2000)

    # 施設のクリック手順
    for label in facility["click_sequence"]:
        ok = try_click_text(page, label)
        if not ok:
            raise RuntimeError(f"クリック対象が見つかりません：『{label}』（施設: {facility['name']}）")


def get_current_year_month_text(page):
    """
    ページ本文から 'YYYY年M月' を抽出（例：2026年1月）
    見つからない場合は None
    """
    try:
        text = page.inner_text("body")
        m = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月", text)
        if m:
            return f"{m.group(1)}年{int(m.group(2))}月"
    except Exception:
        pass
    return None


def locate_calendar_root(page, hint):
    """
    カレンダー本体らしき要素（grid/table等）を探索して最もテキスト量が多いものを選ぶ。
    """
    candidates = []
    for sel in ["[role='grid']", "table", "section", "div.calendar"]:
        loc = page.locator(sel)
        cnt = loc.count()
        for i in range(cnt):
            el = loc.nth(i)
            try:
                t = el.inner_text()
                if hint in t or re.search(r"空き|状況|予約|カレンダー", t):
                    candidates.append(el)
            except Exception:
                continue

    if not candidates:
        return page.locator("body")

    best = None
    best_len = -1
    for el in candidates:
        try:
            t = el.inner_text()
            if len(t) > best_len:
                best_len = len(t)
                best = el
        except Exception as e:
            print(f"[WARN] extract_status_cells: 例外 {e}", flush=True)
            continue
    return best or page.locator("body")


def dump_calendar_html(calendar_root, out_path):
    """
    デバッグ用にカレンダー要素の outerHTML を保存
    """
    try:
        html = calendar_root.evaluate("el => el.outerHTML")
        Path(out_path).write_text(html, "utf-8")
    except Exception as e:
        print(f"[WARN] calendar HTML dump 失敗: {e}", flush=True)


def take_calendar_screenshot(calendar_root, out_path):
    """
    カレンダー要素のみスクリーンショット
    """
    calendar_root.scroll_into_view_if_needed()
    calendar_root.screenshot(path=str(out_path))


# --------------------------------------------------------------------------------
# ステータス認識（×／△／○／〇）
# --------------------------------------------------------------------------------
def status_from_text(raw_text, patterns):
    """
    ログ強化版：テキストからステータスを判断し、検出経路をログに出力
    """
    """
    テキストからステータスを判断（直書き記号優先）
    ※「空き状況」「空き」などの一般語で誤○にならないよう、広い語彙は使わない
    """
    txt = raw_text or ""
    # 直書き記号のみ
    for ch in ["○", "〇", "△", "×"]:
        if ch in txt:
            return ch

    # （必要な場合のみ）英語／日本語キーワード
    t = txt.lower()
    for kw in patterns["circle"]:
        if kw in t:
            return "○"
    for kw in patterns["triangle"]:
        if kw in t:
            return "△"
    for kw in patterns["cross"]:
        if kw in t:
            return "×"
    return None


def status_from_img(el, patterns):
    """
    ログ強化版：<img> の alt/title/src からの検出経路をログに出力
    """
    """
    <img> の alt / title / src から判断
    """
    alt = el.get_attribute("alt") or ""
    title = el.get_attribute("title") or ""
    src = el.get_attribute("src") or ""
    s = status_from_text(alt + " " + title, patterns)
    if s:
        print(f"[DEBUG] status_from_img: alt/titleで検出 status='{s}' alt='{alt[:40]}' title='{title[:40]}'", flush=True)
        return s
    s = status_from_text(src, patterns)
    if s:
        print(f"[DEBUG] status_from_img: srcで検出 status='{s}' src='{src[:80]}'", flush=True)
    return s


def status_from_aria(el, patterns):
    """
    ログ強化版：aria-label / title からの検出経路をログに出力
    """
    """
    aria-label / title から判断
    """
    aria = el.get_attribute("aria-label") or ""
    title = el.get_attribute("title") or ""
    s = status_from_text(aria + " " + title, patterns)
    if s:
        print(
            f"[DEBUG] status_from_aria: 検出 status='{s}' "
            f"aria='{(aria or '')[:60]}' title='{(title or '')[:40]}'",
            flush=True)
    return s


def status_from_css(el, page, config):
    """
    ログ強化版：CSS background-image と class 名からの検出をログ出力
    """
    """
    CSSの background-image と class 名から判断
    """
    patterns = config["status_patterns"]
    try:
        bg = el.evaluate("e => getComputedStyle(e).backgroundImage") or ""
    except Exception:
        bg = ""
    cls = el.get_attribute("class") or ""

    # 背景画像URLにキーワード
    s = status_from_text(bg, patterns)
    if s:
        return s

    # クラス名パターン
    cl = (cls or "").lower()
    for kw in config["css_class_patterns"]["circle"]:
        if kw in cl:
            return "○"
    for kw in config["css_class_patterns"]["triangle"]:
        if kw in cl:
            return "△"
    for kw in config["css_class_patterns"]["cross"]:
        if kw in cl:
            return "×"
    return None


def looks_like_day_cell(base):
    """
    日付セルっぽいかの簡易判定：
    - テキストに 1〜31 の数字、または「○/△/×」記号が含まれる
    - もしくは class に 'day' 'cell' 'calendar' 'fc-daygrid-day' を含む
    - 曜日ヘッダ（「日曜/月曜/...」）は除外
    """
    try:
        txt = (base.inner_text() or "").strip()
        cls = (base.get_attribute("class") or "").lower()
        # 曜日ヘッダは除外（簡略化）
        if re.search(r"(日曜|月曜|火曜|水曜|木曜|金曜|土曜)", txt):
            return False
        # 1〜31 の数字 or 直記号
        if re.search(r"([1-9]|[12]\d|3[01])", txt):
            return True
        if any(ch in txt for ch in ["○", "〇", "△", "×"]):
            return True
        # クラス名ヒント（少し広め）
        if any(k in cls for k in ["day", "cell", "calendar", "fc-daygrid-day"]):
            return True
    except Exception:
        pass
    return False
def extract_status_cells(page, calendar_root, config):
    """
    カレンダー内のセル（tbody td / gridcell）を広く走査し、ステータスを判定。
    ヘッダ・大見出しを looks_like_day_cell で除外。
    戻り値：(cells, cal_bbox)
      cells = [{key, status, bbox{x,y,w,h}, text}]
    """
    print("[INFO] extract_status_cells: start", flush=True)
    patterns = config["status_patterns"]
    debug_top = int(config.get("debug", {}).get("log_top_samples", 10) or 10)

    cal_bbox = calendar_root.bounding_box() or {"x": 0, "y": 0, "width": 1600, "height": 1200}
    cal_x, cal_y = cal_bbox.get("x", 0), cal_bbox.get("y", 0)

    cells, samples = [], []

    
    # 候補拡張：divベースや日付グリッド系にも対応（FullCalendar等）
    candidates = calendar_root.locator("td, [role='gridcell'], .fc-daygrid-day, .calendar-day, .day")
    cnt = candidates.count()
    print(f"[INFO] candidate cells count={cnt}", flush=True)

    for i in range(cnt):
        base = candidates.nth(i)

        # 実セル判定（ヘッダ・空セルは弾く）
        if not looks_like_day_cell(base):
            continue

        try:
            bbox = base.bounding_box()
            if not bbox:
                continue

            rel_x = max(0, bbox["x"] - cal_x)
            rel_y = max(0, bbox["y"] - cal_y)
            txt = (base.inner_text() or "").strip()

            # 1) テキスト（直記号優先）
            s = status_from_text(txt, patterns)

            # 2) 子<img>
            if not s:
                imgs = base.locator("img")
                jcnt = imgs.count()
                for j in range(jcnt):
                    s = status_from_img(imgs.nth(j), patterns)
                    if s:
                        break

            # 3) ARIA/title
            if not s:
                s = status_from_aria(base, patterns)

            # 4) CSS背景／クラス
            if not s:
                s = status_from_css(base, page, config)

            if not s:
                # 記号やステータスが判定できないセルは対象外（誤○にしない）
                continue

            key = f"{int(rel_x/10)}-{int(rel_y/10)}:{txt[:40]}"
            cells.append({
                "key": key,
                "status": s,
                "bbox": {"x": rel_x, "y": rel_y, "w": bbox["width"], "h": bbox["height"]},
                "text": txt
            })

            if len(samples) < debug_top:
                samples.append({
                    "status": s,
                    "text": txt,
                    "bbox": [int(rel_x), int(rel_y), int(bbox["width"]), int(bbox["height"])]
                })
        except Exception as e:
            print(f"[WARN] extract_status_cells: 例外 {e}", flush=True)
            continue

    # デバッグ出力
    summary = {"○": 0, "△": 0, "×": 0}
    for c in cells:
        summary[c["status"]] += 1
    print(f"[DEBUG] status counts: ○={summary['○']} △={summary['△']} ×={summary['×']}", flush=True)
    if samples:
        print("[DEBUG] top samples:", flush=True)
        for s in samples:
            print(f"  - {s['status']} | {s['text'][:60]} | bbox={s['bbox']}", flush=True)

    return cells, cal_bbox


# --------------------------------------------------------------------------------
# 改善判定・ハイライト・Discord送付
# --------------------------------------------------------------------------------
def draw_highlights_on_image(image_path, cells_to_highlight, alpha=160, border_width=3):
    """
    画像に黄色半透明ハイライト
    """
    img = Image.open(image_path).convert("RGBA")
    overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)

    for c in cells_to_highlight:
        x, y = int(c["bbox"]["x"]), int(c["bbox"]["y"])
        w, h = int(c["bbox"]["w"]), int(c["bbox"]["h"])
        draw.rectangle([x, y, x + w, y + h],
                       fill=(255, 255, 0, alpha),
                       outline=(255, 255, 0, 255),
                       width=border_width)

    out_img = Image.alpha_composite(img, overlay).convert("RGB")
    out_img.save(image_path)


def send_to_discord(image_path, title_text, improved_cells_summary):
    """
    Discord Webhookへ画像+メッセージ送信
    """
    if not DISCORD_WEBHOOK_URL:
        raise RuntimeError("DISCORD_WEBHOOK_URL が未設定です。Secrets の DISCORD_WEBHOOK_URL を設定してください。")

    now = jst_now()
    jst_str = now.strftime("%Y-%m-%d %H:%M:%S JST")

    content = f"{title_text}\n改善を検知しました（{jst_str}）。\n{improved_cells_summary}"

    files = {"file": open(image_path, "rb")}
    data = {"content": content}
    resp = requests.post(DISCORD_WEBHOOK_URL, data=data, files=files, timeout=30)
    if resp.status_code >= 300:
        raise RuntimeError(f"Discord送信に失敗しました: HTTP {resp.status_code} {resp.text}")


# --------------------------------------------------------------------------------
# 状態保存（施設×年月）
# --------------------------------------------------------------------------------
def facility_month_dir(f_short, month_text):
    safe_fac = re.sub(r"[\\/:*?\"<>|]+", "_", f_short)
    safe_month = re.sub(r"[\\/:*?\"<>|]+", "_", month_text or "unknown_month")
    d = SNAP_DIR / safe_fac / safe_month
    d.mkdir(parents=True, exist_ok=True)
    return d


def load_latest_state(dirpath: Path):
    state_path = dirpath / "latest_state.json"
    state = {}
    if state_path.exists():
        try:
            state = json.loads(state_path.read_text("utf-8"))
        except Exception as e:
            print(f"[WARN] latest_state.json の読み込みに失敗: {e}", flush=True)
            state = {}
    return state


def save_latest_state(dirpath: Path, state: dict):
    (dirpath / "latest_state.json").write_text(json.dumps(state, ensure_ascii=False, indent=2), "utf-8")


# --------------------------------------------------------------------------------
# 月遷移（フォールバック付き）
# --------------------------------------------------------------------------------


def click_next_month(page, label_primary="翌月", calendar_root=None):
    """
    翌月遷移を強化：
      - カレンダー「ヘッダ」領域にスコープを絞って検索
      - 文字候補が複数ある場合は .first で一意にクリック
      - プルダウン（月セレクタ）が存在する場合は次月へ変更するフォールバック
    """
    scope = (calendar_root.locator("thead, .calendar-header, .fc-toolbar, nav").first
             if calendar_root else page)

    # 1) テキスト／ボタン／リンク（厳密一致＋近似）
    for cand in [label_primary, "次月", "次へ", "＞", ">>", "次月へ", "翌月へ"]:
        # scope 内で検索し、複数ヒット時は first をクリック
        try:
            scope.get_by_role("button", name=cand, exact=True).first.click(timeout=2000)
            page.wait_for_load_state("networkidle", timeout=30000)
            return True
        except Exception:
            pass
        try:
            scope.get_by_text(cand, exact=True).first.click(timeout=2000)
            page.wait_for_load_state("networkidle", timeout=30000)
            return True
        except Exception:
            pass

    # 2) aria-label / title をもつ要素を総当り
    for cand in [label_primary, "次月", "次へ", "翌月"]:
        for sel in ["[aria-label]", "[title]"]:
            try:
                loc = scope.locator(sel)
                cnt = loc.count()
                for i in range(cnt):
                    el = loc.nth(i)
                    aria = el.get_attribute("aria-label") or ""
                    title = el.get_attribute("title") or ""
                    if cand in aria or cand in title:
                        try:
                            el.click(timeout=3000)
                            page.wait_for_load_state("networkidle", timeout=30000)
                            return True
                        except Exception:
                            pass
            except Exception:
                pass

    # 3) 画像ボタン（img alt/src に翌月キーワード）
    try:
        imgs = scope.locator("img")
        icnt = imgs.count()
        for i in range(icnt):
            el = imgs.nth(i)
            alt = el.get_attribute("alt") or ""
            src = el.get_attribute("src") or ""
            if any(k in (alt + " " + src) for k in [label_primary, "次月", "次へ", "翌月"]):
                try:
                    el.click(timeout=3000)
                    page.wait_for_load_state("networkidle", timeout=30000)
                    return True
                except Exception:
                    pass
    except Exception:
        pass

    # 4) プルダウン（月セレクタ）で次月へ切り替え（存在する場合）
    try:
        selects = scope.locator("select")
        scnt = selects.count()
        for si in range(scnt):
            sel = selects.nth(si)
            # 現在の選択肢から「翌月 / 次月」を含む option を選択
            opts = sel.locator("option")
            ocnt = opts.count()
            for oi in range(ocnt):
                txt = (opts.nth(oi).inner_text() or "").strip()
                if any(k in txt for k in [label_primary, "次月", "翌月"]):
                    value = opts.nth(oi).get_attribute("value")
                    if value is not None:
                        try:
                            sel.select_option(value=value)
                            page.wait_for_load_state("networkidle", timeout=30000)
                            return True
                        except Exception:
                            pass
    except Exception:
        pass

    return False


# --------------------------------------------------------------------------------
# メイン処理（施設→当月/翌月の検出とスナップショット保存）
# --------------------------------------------------------------------------------

def run_monitor():
    print("[INFO] run_monitor: start", flush=True)
    ensure_dirs()
    try:
        config = load_config()
    except Exception as e:
        print(f"[ERROR] config load failed: {e}", flush=True)
        return

    facilities = config.get("facilities", [])
    if not facilities:
        print("[WARN] config['facilities'] が空です。何も処理できません。", flush=True)
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        for facility in facilities:
            try:
                print(f"[INFO] navigate_to_facility: {facility.get('name','unknown')}", flush=True)
                navigate_to_facility(page, facility)
                month_text = get_current_year_month_text(page) or "unknown"
                print(f"[INFO] current month: {month_text}", flush=True)

                # 当月
                calendar_root = locate_calendar_root(page, month_text or "予約カレンダー")
                try:
                    bbox = calendar_root.bounding_box() or {"width": None, "height": None}
                except Exception:
                    bbox = {"width": None, "height": None}
                print(f"[INFO] calendar_root acquired: bbox={bbox}", flush=True)
                print("[INFO] about to call extract_status_cells", flush=True)
                cells, cal_bbox = extract_status_cells(page, calendar_root, config)

                # スナップショット保存
                fshort = FACILITY_TITLE_ALIAS.get(facility.get('name',''), facility.get('name',''))
                outdir = facility_month_dir(fshort or 'unknown_facility', month_text)
                dump_calendar_html(calendar_root, outdir / 'calendar.html')
                shot = outdir / 'calendar.png'
                take_calendar_screenshot(calendar_root, shot)

                
                # 翌月（config.jsonの next_month_label を優先）
                next_label = config.get("next_month_label", "翌月")
                if click_next_month(page, label_primary=next_label, calendar_root=calendar_root):
                    next_month_text = get_current_year_month_text(page) or "unknown"
                    print(f"[INFO] next month: {next_month_text}", flush=True)
                    calendar_root2 = locate_calendar_root(page, next_month_text or "予約カレンダー")

                    try:
                        bbox2 = calendar_root2.bounding_box() or {"width": None, "height": None}
                    except Exception:
                        bbox2 = {"width": None, "height": None}
                    print(f"[INFO] calendar_root(next) acquired: bbox={bbox2}", flush=True)
                    print("[INFO] about to call extract_status_cells (next)", flush=True)
                    cells2, cal_bbox2 = extract_status_cells(page, calendar_root2, config)
                    dump_calendar_html(calendar_root2, outdir / 'calendar_next.html')
                    shot2 = outdir / 'calendar_next.png'
                    take_calendar_screenshot(calendar_root2, shot2)

            except Exception as e:
                print(f"[WARN] run_monitor: facility処理中に例外: {e}", flush=True)
                continue
        browser.close()


if __name__ == "__main__":
    # 監視時間帯フィルタ（念のため）
    if not is_within_monitoring_window():
        print("[INFO] outside monitoring window. exit.", flush=True)
    else:
        run_monitor()
