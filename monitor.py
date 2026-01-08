
# -*- coding: utf-8 -*-
"""
さいたま市 施設予約システムの空き状況監視（改善のみ通知/キャプチャ保存）

【この版の主な仕様】
- 施設ごとの month_shifts（例：当月=0、翌月=1、+2ヶ月=2、+3ヶ月=3）に従い、
  当月から指定された月まで「次の月」を連続クリックして到達・キャプチャ保存。
  - 南浦和/岩槻: [0,1,2,3]（当月〜3ヶ月後まで）
  - 岸町/鈴谷:   [0,1]      （当月〜翌月まで）
- 各月のカレンダー要素を抽出し、HTML/PNG を snapshots/<施設短縮名>/<YYYY年M月>/ に保存。
- ステータス検出は文字（「全て空き」「一部空き」「予約あり」「受付期間外」「休館日」「保守日」「雨天」など）
  を config.json の patterns に基づき記号（○/△/×）へ正規化して利用可能。

【必須環境変数（GitHub Secretsなど）】
- BASE_URL: 例 "https://saitama.rsv.ws-scs.jp/web/"
- DISCORD_WEBHOOK_URL: Discord に通知を送る場合のみ必須（本コードでは通知は任意）

【設定ファイル】
- config.json（本物の JSON 形式）
  - facilities[].name / click_sequence / month_shifts（例: [0,1,2,3]）
  - next_month_label: "次の月"（サイトUIの表記に合わせる）
  - status_patterns / css_class_patterns / debug など
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

# === 施設短縮名（Discordタイトルや保存ディレクトリ用） ===
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
# 月遷移（フォールバック付き）
# --------------------------------------------------------------------------------
def click_next_month(page, label_primary="次の月", calendar_root=None):
    """
    翌月遷移を強化：
    - カレンダー「ヘッダ」領域にスコープを絞って検索
    - 文字候補が複数ある場合は .first で一意にクリック
    """
    scope = (calendar_root.locator("thead, .calendar-header, .fc-toolbar, nav").first
             if calendar_root else page)
    for cand in [label_primary, "次の月", "次月", "次へ", "＞", ">>", "次月へ", "翌月へ"]:
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
    return False

# --------------------------------------------------------------------------------
# 施設→当月/翌月/…の検出とスナップショット保存
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

                # 施設ごとの監視対象（月シフト）
                shifts = facility.get("month_shifts", [0, 1])
                shifts = sorted(set(int(s) for s in shifts if isinstance(s, (int, float))))
                if 0 not in shifts:
                    shifts.insert(0, 0)

                # 当月
                month_text = get_current_year_month_text(page) or "unknown"
                print(f"[INFO] current month: {month_text}", flush=True)
                cal_root = locate_calendar_root(page, month_text or "予約カレンダー")

                # 保存（当月）
                fshort = FACILITY_TITLE_ALIAS.get(facility.get('name',''), facility.get('name',''))
                outdir = facility_month_dir(fshort or 'unknown_facility', month_text)
                dump_calendar_html(cal_root, outdir / 'calendar.html')
                take_calendar_screenshot(cal_root, outdir / 'calendar.png')
                print(f"[INFO] saved: {facility.get('name','')} - {month_text}", flush=True)

                # 以降、最大シフトまで「次の月」を順にクリック
                max_shift = max(shifts)
                next_label = config.get("next_month_label", "次の月")
                for step in range(1, max_shift + 1):
                    ok = click_next_month(page, label_primary=next_label, calendar_root=cal_root)
                    if not ok:
                        print(f"[WARN] next-month click failed at step={step}", flush=True)
                        break
                    month_text2 = get_current_year_month_text(page) or f"shift_{step}"
                    print(f"[INFO] month(step={step}): {month_text2}", flush=True)
                    cal_root2 = locate_calendar_root(page, month_text2 or "予約カレンダー")

                    # この step が監視対象なら保存
                    if step in shifts:
                        outdir2 = facility_month_dir(fshort or 'unknown_facility', month_text2)
                        dump_calendar_html(cal_root2, outdir2 / 'calendar.html')
                        take_calendar_screenshot(cal_root2, outdir2 / 'calendar.png')
                        print(f"[INFO] saved: {facility.get('name','')} - {month_text2}", flush=True)

                    # 次ループの基準
                    cal_root = cal_root2

            except Exception as e:
                print(f"[WARN] run_monitor: facility処理中に例外: {e}", flush=True)
                continue

        browser.close()

# --------------------------------------------------------------------------------
# 状態保存（施設×年月）
# --------------------------------------------------------------------------------
def facility_month_dir(f_short, month_text):
    safe_fac = re.sub(r"[\\/:*?\"<>|]+", "_", f_short)
    safe_month = re.sub(r"[\\/:*?\"<>|]+", "_", month_text or "unknown_month")
    d = SNAP_DIR / safe_fac / safe_month
    d.mkdir(parents=True, exist_ok=True)
    return d

# --------------------------------------------------------------------------------
# エントリポイント
# --------------------------------------------------------------------------------
if __name__ == "__main__":
    if not is_within_monitoring_window():
        print("[INFO] outside monitoring window. exit.", flush=True)
    else:
        run_monitor()
