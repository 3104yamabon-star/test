# -*- coding: utf-8 -*-
"""
さいたま市 施設予約システムの空き状況監視（改善のみ通知/キャプチャ保存）

【この版の主な仕様（高速化対応）】
- 施設ごとの month_shifts（例：当月=0、翌月=1、+2ヶ月=2、+3ヶ月=3）に従い、
  初回のみ施設ページへ到達し、以降は「次の月」を連続クリックして各月をキャプチャ保存。
  - 南浦和/岩槻: [0,1,2,3]（当月〜3ヶ月後まで）
  - 岸町/鈴谷:   [0,1]      （当月〜翌月まで）
- 各月のカレンダー要素を抽出し、HTML/PNG を snapshots/<施設短縮名>/<YYYY年M月>/ に保存。
- 「次の月」は javascript:moveCalender(...) による同一ページ DOM 差し替えのため、
  待機は networkidle ではなく「月テキスト（YYYY年M月）の変化」を指標とする。
"""

import os
import sys
import json
import re
import time
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

# === 施設短縮名（保存ディレクトリ用） ===
FACILITY_TITLE_ALIAS = {
    "岩槻南部公民館": "岩槻",
    "南浦和コミュニティセンター": "南浦和",
    "岸町公民館": "岸町",
    "鈴谷公民館": "鈴谷",
}

# === ステータスの序列（改善判定に使用：必要に応じて） ===
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
    """JSTで 05:00〜23:59 を監視対象にする"""
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
    指定ラベルのリンク/ボタン/テキストをクリック。
    厳密一致を優先しつつ、フォールバックで text= を使う。
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
            # 最初の遷移時はページロードが走る可能性が高い
            page.wait_for_load_state("networkidle", timeout=30000)
            return True
        except Exception as e:
            if not quiet:
                print(f"[WARN] try_click_text: 例外 {e}（label='{label}'）", flush=True)
            continue
    return False

def navigate_to_facility(page, facility):
    """
    トップへ → click_sequence の順で施設の当月ページまで到達（初回のみ）
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
    for sel in ["[role='grid']", "table", "section", "div.calendar", "div"]:
        loc = page.locator(sel)
        cnt = loc.count()
        for i in range(cnt):
            el = loc.nth(i)
            try:
                t = (el.inner_text() or "").strip()
                # ヒント一致 or カレンダーらしい語句を含む
                if (hint and hint in t) or re.search(r"(空き状況|予約あり|一部空き|カレンダー)", t):
                    candidates.append((len(t), el))
            except Exception:
                continue
    if not candidates:
        return page.locator("body")
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]

def dump_calendar_html(calendar_root, out_path):
    """デバッグ用にカレンダー要素の outerHTML を保存"""
    try:
        html = calendar_root.evaluate("el => el.outerHTML")
        Path(out_path).write_text(html, "utf-8")
    except Exception as e:
        print(f"[WARN] calendar HTML dump 失敗: {e}", flush=True)

def take_calendar_screenshot(calendar_root, out_path):
    """カレンダー要素のみスクリーンショット"""
    calendar_root.scroll_into_view_if_needed()
    calendar_root.screenshot(path=str(out_path))

# --------------------------------------------------------------------------------
# 月遷移（高速化対応版）
# --------------------------------------------------------------------------------
def _compute_next_month_text(prev_month_text: str) -> str:
    """'YYYY年M月' → 次月の 'YYYY年M月' を返す（待機の目標に使う）"""
    try:
        m = re.match(r"(\d{4})年(\d{1,2})月", prev_month_text or "")
        if not m:
            return ""
        y, mo = int(m.group(1)), int(m.group(2))
        if mo == 12:
            y += 1
            mo = 1
        else:
            mo += 1
        return f"{y}年{mo}月"
    except Exception:
        return ""

def click_next_month(page, label_primary="次の月", calendar_root=None,
                     prev_month_text=None, wait_timeout_ms=20000):
    """
    「次の月」へ遷移（強化版）：
    - 探索スコープ：calendar_root優先 → 失敗時は page 全体
    - 一致方法：厳密一致→部分一致（has-text）、href=moveCalender を直接クリック
    - 待機方法：ページ遷移ではなく「月テキストの変化」を指標に wait_for_function
    - 最終フォールバック：リンクの href を eval 実行（javascript:moveCalender(...)）
    """
    scopes = []
    if calendar_root is not None:
        scopes.append(calendar_root)
    scopes.append(page)

    next_month_goal = _compute_next_month_text(prev_month_text or "")

    # クリック候補（順に試す）
    selectors = [
        "a:has-text('次の月')",
        "a:has-text('次')",
        "a[href*='moveCalender']",
    ]

    for scope in scopes:
        clicked = False

        # 1) has-text（部分一致）
        for sel in selectors[:2]:
            try:
                el = scope.locator(sel).first
                el.scroll_into_view_if_needed()
                el.click(timeout=2000)
                clicked = True
                break
            except Exception:
                pass

        # 2) href*='moveCalender' を直接クリック
        if not clicked:
            try:
                el = scope.locator("a[href*='moveCalender']").first
                el.scroll_into_view_if_needed()
                el.click(timeout=2000)
                clicked = True
            except Exception:
                pass

        # 3) 最終フォールバック：href の JavaScript を直接 eval 実行
        if not clicked:
            try:
                el = scope.locator("a[href*='moveCalender']").first
                href = el.get_attribute("href") or ""
                if href.startswith("javascript:"):
                    js = href[len("javascript:"):].strip()
                    page.evaluate(js)  # 同一ページ内で moveCalender(...) を直接呼ぶ
                    clicked = True
            except Exception:
                pass

        if not clicked:
            # 次のスコープで再試行
            continue

        # === クリック成功後の待機：月テキストの変化を待つ ===
        try:
            if next_month_goal:
                page.wait_for_function(
                    """goal => {
                        const txt = document.body.innerText || '';
                        return txt.includes(goal);
                    }""",
                    arg=next_month_goal,
                    timeout=wait_timeout_ms
                )
            else:
                page.wait_for_function(
                    """prev => {
                        const txt = document.body.innerText || '';
                        const m = txt.match(/(\\d{4})\\s*年\\s*(\\d{1,2})\\s*月/);
                        if (!m) return false;
                        const cur = `${m[1]}年${parseInt(m[2], 10)}月`;
                        return prev && cur !== prev;
                    }""",
                    arg=prev_month_text or "",
                    timeout=wait_timeout_ms
                )
            return True
        except Exception:
            # このスコープでは変化を検知できず → 他スコープで再試行
            pass

    return False

# --------------------------------------------------------------------------------
# 施設→当月/翌月/…のキャプチャ保存（高速化）
# --------------------------------------------------------------------------------
def facility_month_dir(f_short, month_text):
    safe_fac = re.sub(r"[\\/:*?\"<>|]+", "_", f_short)
    safe_month = re.sub(r"[\\/:*?\"<>|]+", "_", month_text or "unknown_month")
    d = SNAP_DIR / safe_fac / safe_month
    d.mkdir(parents=True, exist_ok=True)
    return d

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
                # 初回のみ施設ページへ到達
                navigate_to_facility(page, facility)

                # 監視対象の月ステップ
                shifts = facility.get("month_shifts", [0, 1])
                shifts = sorted(set(int(s) for s in shifts if isinstance(s, (int, float))))
                if 0 not in shifts:
                    shifts.insert(0, 0)

                # 当月キャプチャ
                month_text = get_current_year_month_text(page) or "unknown"
                print(f"[INFO] current month: {month_text}", flush=True)
                cal_root = locate_calendar_root(page, month_text or "予約カレンダー")

                fshort = FACILITY_TITLE_ALIAS.get(facility.get('name',''), facility.get('name',''))
                outdir = facility_month_dir(fshort or 'unknown_facility', month_text)
                dump_calendar_html(cal_root, outdir / 'calendar.html')
                take_calendar_screenshot(cal_root, outdir / 'calendar.png')
                print(f"[INFO] saved: {facility.get('name','')} - {month_text}", flush=True)

                # 「次の月」を連続クリック → 各月キャプチャ（高速化本体）
                max_shift = max(shifts)
                next_label = config.get("next_month_label", "次の月")
                prev_month_text = month_text

                for step in range(1, max_shift + 1):
                    ok = click_next_month(
                        page,
                        label_primary=next_label,
                        calendar_root=cal_root,
                        prev_month_text=prev_month_text,
                        wait_timeout_ms=20000
                    )
                    if not ok:
                        # 失敗時の全画面スクリーンショット（原因調査用）
                        failed = SNAP_DIR / f"failed_next_month_step{step}_{fshort}.png"
                        try:
                            page.screenshot(path=str(failed))
                        except Exception:
                            pass
                        print(f"[WARN] next-month click failed at step={step} (full-page captured)", flush=True)
                        break

                    # 月テキスト更新後に再取得・キャプチャ
                    month_text2 = get_current_year_month_text(page) or f"shift_{step}"
                    print(f"[INFO] month(step={step}): {month_text2}", flush=True)
                    cal_root2 = locate_calendar_root(page, month_text2 or "予約カレンダー")

                    if step in shifts:  # 監視対象なら保存
                        outdir2 = facility_month_dir(fshort or 'unknown_facility', month_text2)
                        dump_calendar_html(cal_root2, outdir2 / 'calendar.html')
                        take_calendar_screenshot(cal_root2, outdir2 / 'calendar.png')
                        print(f"[INFO] saved: {facility.get('name','')} - {month_text2}", flush=True)

                    # 次ループの基準
                    cal_root = cal_root2
                    prev_month_text = month_text2

            except Exception as e:
                print(f"[WARN] run_monitor: facility処理中に例外: {e}", flush=True)
                continue

        browser.close()

# --------------------------------------------------------------------------------
# エントリポイント
# --------------------------------------------------------------------------------
if __name__ == "__main__":
    if not is_within_monitoring_window():
        print("[INFO] outside monitoring window. exit.", flush=True)
    else:
        run_monitor()
