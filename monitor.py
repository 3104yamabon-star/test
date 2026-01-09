
# -*- coding: utf-8 -*-
"""
さいたま市 施設予約システムの空き状況監視（改善通知/キャプチャ保存/空き状況集計）

【主な仕様（高速化＋堅牢化＋集計対応）】
- 施設ごとの month_shifts（例：当月=0、翌月=1、+2ヶ月=2、+3ヶ月=3）に従い、
  初回のみ施設ページへ到達し、以降は「次の月」を連続クリックして各月をキャプチャ保存。
- 「次の月」は javascript:moveCalender(...) による同一ページ DOM 差し替えのため、
  待機は networkidle ではなく「月テキスト（YYYY年M月）の変化」を指標に wait_for_function で行う。
- 各月のカレンダー要素から「空き状況（◯／△／×）」を集計し、JSON/CSVで保存＋ログに要約を出力。

【前提】
- config.json（純正 JSON）
  - facilities[].name / click_sequence / month_shifts
  - next_month_label: "次の月"
  - status_patterns（語句→記号の正規化）/ css_class_patterns / debug
- GitHub Actions の Workflow は python -u monitor.py を実行（既存の構成を想定）
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


from datetime import datetime
import json
from pathlib import Path

def load_last_summary(outdir: Path) -> dict:
    """過去の status_counts.json を読み、直近 summary を返す。無ければ None。"""
    fp = outdir / "status_counts.json"
    if not fp.exists():
        return None
    try:
        data = json.loads(fp.read_text("utf-8"))
        return data.get("summary")
    except Exception:
        return None

def summaries_changed(prev: dict, cur: dict) -> bool:
    """summary(dict) の変化判定。キー欠損も考慮して厳密比較。"""
    if prev is None and cur is not None:
        return True
    if prev is None and cur is None:
        return False
    # 期待キー（○/△/×/未判定）が揃っているかを保証
    keys = {"○", "△", "×", "未判定"}
    for k in keys:
        if prev.get(k, 0) != cur.get(k, 0):
            return True
    return False

def save_calendar_assets(cal_root, outdir: Path, save_timestamped: bool):
    """
    カレンダーHTML/PNGを保存する。
    save_timestamped=True のとき、履歴用にタイムスタンプ付きファイルも作成。
    """
    # 最新の別名は常に上書きしておく（運用により False でもOK）
    latest_html = outdir / "calendar.html"
    latest_png = outdir / "calendar.png"

    # 履歴用タイムスタンプ
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    html_ts = outdir / f"calendar_{ts}.html"
    png_ts = outdir / f"calendar_{ts}.png"

    # 最新を更新
    dump_calendar_html(cal_root, latest_html)
    take_calendar_screenshot(cal_root, latest_png)

    if save_timestamped:
        dump_calendar_html(cal_root, html_ts)
        take_calendar_screenshot(cal_root, png_ts)
        print(f"[INFO] saved (timestamped): {html_ts.name}, {png_ts.name}", flush=True)
    else:
        print(f"[INFO] saved (latest only): {latest_html.name}, {latest_png.name}", flush=True)


# （任意）JST時間帯チェック用
try:
    import pytz
except Exception:
    pytz = None

# === 環境変数（GitHub Secrets） ===
BASE_URL = os.getenv("BASE_URL")  # 例: "https://saitama.rsv.ws-scs.jp/web/"
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

# === パス ===
BASE_DIR = Path(__file__).resolve().parent
SNAP_DIR = BASE_DIR / "snapshots"
CONFIG_PATH = BASE_DIR / "config.json"

# === 施設短縮名 ===
FACILITY_TITLE_ALIAS = {
    "岩槻南部公民館": "岩槻",
    "南浦和コミュニティセンター": "南浦和",
    "岸町公民館": "岸町",
    "鈴谷公民館": "鈴谷",
}

# === ステータス序列（必要に応じて比較用） ===
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
            # 初回遷移はページロードが走ることがある。networkidle待機はここだけ。
            page.wait_for_load_state("networkidle", timeout=30000)
            return True
        except Exception as e:
            if not quiet:
                print(f"[WARN] try_click_text: 例外 {e}（label='{label}'）", flush=True)
            continue
    return False

def navigate_to_facility(page, facility):
    """
    トップ → click_sequence の順で施設の当月ページまで到達（初回のみ）
    """
    if not BASE_URL:
        raise RuntimeError("BASE_URL が未設定です。Secrets の BASE_URL に https://saitama.rsv.ws-scs.jp/web/ を入れてください。")
    # トップへ
    page.goto(BASE_URL, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_load_state("networkidle", timeout=60000)
    # 任意ダイアログがあれば閉じる
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


def locate_calendar_root(page, hint: str, facility: dict = None):
    """
    カレンダー枠を厳密に特定する:
    - configのcalendar_selectorがあればそれを最優先
    - グリッド候補は role=grid / table / div.calendar 等だが、
      '曜日文字'と'十分なセル数'を持つもののみ採用
    - 失敗時は body にフォールバックせず例外を送出
    """
    # 1) facility固有セレクタがあれば最優先
    sel_cfg = (facility or {}).get("calendar_selector")
    if sel_cfg:
        loc = page.locator(sel_cfg)
        if loc.count() > 0:
            el = loc.first
            return el

    # 2) 汎用探索（曜日ヘッダとセル数チェック）
    candidates = []
    weekday_markers = ["月", "火", "水", "木", "金", "土", "日"]

    for sel in ["[role='grid']", "table", "section", "div.calendar", "div"]:
        loc = page.locator(sel)
        cnt = loc.count()
        for i in range(cnt):
            el = loc.nth(i)
            try:
                t = (el.inner_text() or "").strip()
            except Exception:
                continue

            # ヒント文字（例：空き状況）や月表示の付近にありそうなもののみ
            score = 0
            if hint and hint in t:
                score += 2

            # 曜日が揃っているか（7種のうち4種以上含む）
            wk = sum(1 for w in weekday_markers if w in t)
            if wk >= 4:
                score += 3

            # セル数判定（td / gridcell が28以上ある）
            try:
                cells = el.locator("td, [role='gridcell'], .fc-daygrid-day, .calendar-day, .day")
                cell_cnt = cells.count()
                if cell_cnt >= 28:  # 月表示らしい最低ライン
                    score += 3
            except Exception:
                pass

            if score >= 5:  # 閾値
                candidates.append((score, el))

    if not candidates:
        raise RuntimeError("カレンダー枠の特定に失敗しました（候補が見つからないため監視を中止）。")

    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def dump_calendar_html(calendar_root, out_path):
    """デバッグ用：カレンダー要素の outerHTML を保存"""
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
# 月遷移（高速化対応）
# --------------------------------------------------------------------------------
def _compute_next_month_text(prev_month_text: str) -> str:
    """'YYYY年M月' → 次月の 'YYYY年M月' を返す（待機の目標に使う）"""
    try:
        m = re.match(r"(\d{4})年(\d{1,2})月", prev_month_text or "")
        if not m:
            return ""
        y, mo = int(m.group(1)), int(m.group(2))
        if mo == 12:
            y += 1; mo = 1
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
    - 一致方法：部分一致（has-text）、href=moveCalender を直接クリック
    - 待機方法：ページ遷移でなく「月テキストの変化」を wait_for_function で検知
    - 最終フォールバック：href の JavaScript を eval 実行（javascript:moveCalender(...)）
    """
    scopes = []
    if calendar_root is not None:
        scopes.append(calendar_root)
    scopes.append(page)

    next_month_goal = _compute_next_month_text(prev_month_text or "")

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
            continue  # 次のスコープで再試行

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
# 空き状況集計（◯／△／×）
# --------------------------------------------------------------------------------

def summarize_vacancies(page, calendar_root, config):
    """
    カレンダー要素から日別の空き状況を抽出し ○/△/× を集計する。
    日付の無いセル（ヘッダや説明セル）はスキップする。
    """
    import re as _re

    patterns = config["status_patterns"]
    summary = {"○": 0, "△": 0, "×": 0, "未判定": 0}
    details = []

    def _status_from_text(raw):
        txt = (raw or "").strip()
        txt_norm = txt.replace("　", " ").lower()

        # 記号の直接検出
        for ch in ["○", "〇", "△", "×"]:
            if ch in txt:
                return {"〇": "○"}.get(ch, ch)

        # パターン（キーワード）で検出
        for kw in patterns["circle"]:
            if kw.lower() in txt_norm:
                return "○"
        for kw in patterns["triangle"]:
            if kw.lower() in txt_norm:
                return "△"
        for kw in patterns["cross"]:
            if kw.lower() in txt_norm:
                return "×"

        return None

    # ----- 対象セル：tbody 内の td に限定（ヘッダ th を除外） -----
    candidates = calendar_root.locator(
        ":scope tbody td, :scope [role='gridcell']"
    )

    cnt = candidates.count()

    for i in range(cnt):
        el = candidates.nth(i)

        # セル内テキスト
        try:
            txt = (el.inner_text() or "").strip()
        except Exception:
            continue

        # 先頭付近で日付（1日, 2日 ...）を探す
        head = txt[:40]
        mday = _re.search(r"^([1-9]|[12]\d|3[01])\s*日", head, flags=_re.MULTILINE)

        # aria-label/title に日付がある場合も補助検出
        if not mday:
            try:
                aria = el.get_attribute("aria-label") or ""
                title = el.get_attribute("title") or ""
                mday = _re.search(r"([1-9]|[12]\d|3[01])\s*日", aria + " " + title)
            except Exception:
                pass

        # img の alt/title も一応確認
        if not mday:
            try:
                imgs = el.locator("img")
                jcnt = imgs.count()
                for j in range(jcnt):
                    alt = imgs.nth(j).get_attribute("alt") or ""
                    tit = imgs.nth(j).get_attribute("title") or ""
                    mm = _re.search(r"([1-9]|[12]\d|3[01])\s*日", alt + " " + tit)
                    if mm:
                        mday = mm
                        break
            except Exception:
                pass

        # ★ 日付が無いセルはスキップ（ヘッダ・説明セル除外）
        if not mday:
            continue

        day_label = f"{mday.group(0)}"

        # 状態判定：テキスト → 画像 → aria/title → class
        st = _status_from_text(txt)

        if not st:
            try:
                imgs = el.locator("img")
                jcnt = imgs.count()
                for j in range(jcnt):
                    alt = imgs.nth(j).get_attribute("alt") or ""
                    tit = imgs.nth(j).get_attribute("title") or ""
                    src = imgs.nth(j).get_attribute("src") or ""
                    st = _status_from_text(alt + " " + tit) or _status_from_text(src)
                    if st:
                        break
            except Exception:
                pass

        if not st:
            try:
                aria = el.get_attribute("aria-label") or ""
                tit = el.get_attribute("title") or ""
                cls = (el.get_attribute("class") or "").lower()
                st = _status_from_text(aria + " " + tit)

                if not st:
                    for kw in config["css_class_patterns"]["circle"]:
                        if kw in cls:
                            st = "○"; break
                if not st:
                    for kw in config["css_class_patterns"]["triangle"]:
                        if kw in cls:
                            st = "△"; break
                if not st:
                    for kw in config["css_class_patterns"]["cross"]:
                        if kw in cls:
                            st = "×"; break
            except Exception:
                pass

        if not st:
            st = "未判定"

        summary[st] += 1
        details.append({"day": day_label, "status": st, "text": txt})

    return summary, details


# --------------------------------------------------------------------------------
# 保存ユーティリティ
# --------------------------------------------------------------------------------
def facility_month_dir(f_short, month_text):
    safe_fac = re.sub(r"[\\/:*?\"<>|]+", "_", f_short)
    safe_month = re.sub(r"[\\/:*?\"<>|]+", "_", month_text or "unknown_month")
    d = SNAP_DIR / safe_fac / safe_month
    d.mkdir(parents=True, exist_ok=True)
    return d

# --------------------------------------------------------------------------------
# メイン処理（高速化：初回だけ施設到達→以降は次の月連打）
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

                shifts = facility.get("month_shifts", [0, 1])
                shifts = sorted(set(int(s) for s in shifts if isinstance(s, (int, float))))
                if 0 not in shifts:
                    shifts.insert(0, 0)

                # 当月
                month_text = get_current_year_month_text(page) or "unknown"
                print(f"[INFO] current month: {month_text}", flush=True)
                cal_root = locate_calendar_root(page, month_text or "予約カレンダー", facility)

                fshort = FACILITY_TITLE_ALIAS.get(facility.get('name',''), facility.get('name',''))
                outdir = facility_month_dir(fshort or 'unknown_facility', month_text)
                dump_calendar_html(cal_root, outdir / 'calendar.html')
                take_calendar_screenshot(cal_root, outdir / 'calendar.png')
                print(f"[INFO] saved: {facility.get('name','')} - {month_text}", flush=True)

                # ★ 当月の空き状況集計（JSON/CSV/ログ）
                try:
                    summary, details = summarize_vacancies(page, cal_root, config)
                    (outdir / "status_counts.json").write_text(
                        json.dumps({"month": month_text, "facility": facility.get('name',''),
                                    "summary": summary, "details": details},
                                   ensure_ascii=False, indent=2),
                        "utf-8"
                    )
                    import csv
                    with (outdir / "status_details.csv").open("w", newline="", encoding="utf-8") as fcsv:
                        w = csv.writer(fcsv)
                        w.writerow(["day", "status", "text"])
                        for row in details:
                            w.writerow([row["day"], row["status"], row["text"]])
                    print(f"[INFO] summary({facility.get('name','')} - {month_text}): "
                          f"○={summary['○']} △={summary['△']} ×={summary['×']} 未判定={summary['未判定']}",
                          flush=True)
                except Exception as e:
                    print(f"[WARN] summarize (current) failed: {e}", flush=True)

                # 以降、「次の月」を連続クリック → キャプチャ＆集計
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
                        failed = SNAP_DIR / f"failed_next_month_step{step}_{fshort}.png"
                        try:
                            page.screenshot(path=str(failed))
                        except Exception:
                            pass
                        print(f"[WARN] next-month click failed at step={step} (full-page captured)", flush=True)
                        break

                    month_text2 = get_current_year_month_text(page) or f"shift_{step}"
                    print(f"[INFO] month(step={step}): {month_text2}", flush=True)
                    cal_root2 = locate_calendar_root(page, month_text2 or "予約カレンダー", facility)

                    if step in shifts:
                        outdir2 = facility_month_dir(fshort or 'unknown_facility', month_text2)
                        dump_calendar_html(cal_root2, outdir2 / 'calendar.html')
                        take_calendar_screenshot(cal_root2, outdir2 / 'calendar.png')
                        print(f"[INFO] saved: {facility.get('name','')} - {month_text2}", flush=True)

                        # ★ 各月の空き状況集計
                        try:
                            summary2, details2 = summarize_vacancies(page, cal_root2, config)
                            (outdir2 / "status_counts.json").write_text(
                                json.dumps({"month": month_text2, "facility": facility.get('name',''),
                                            "summary": summary2, "details": details2},
                                           ensure_ascii=False, indent=2),
                                "utf-8"
                            )
                            import csv
                            with (outdir2 / "status_details.csv").open("w", newline="", encoding="utf-8") as fcsv:
                                w = csv.writer(fcsv)
                                w.writerow(["day", "status", "text"])
                                for row in details2:
                                    w.writerow([row["day"], row["status"], row["text"]])
                            print(f"[INFO] summary({facility.get('name','')} - {month_text2}): "
                                  f"○={summary2['○']} △={summary2['△']} ×={summary2['×']} 未判定={summary2['未判定']}",
                                  flush=True)
                        except Exception as e:
                            print(f"[WARN] summarize (step={step}) failed: {e}", flush=True)

                    # 次ループ基準更新
                    cal_root = cal_root2
                    prev_month_text = month_text2

            except Exception as e:
                print(f"[WARN] run_monitor: facility処理中に例外: {e}", flush=True)
                continue

        browser.close()

# --------------------------------------------------------------------------------
# CLI：並列実行用 施設フィルタ（Actionsのmatrix対応）
# --------------------------------------------------------------------------------
def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--facility", default=None, help="監視対象施設名を指定（並列実行用）")
    args = parser.parse_args()

    if not is_within_monitoring_window():
        print("[INFO] outside monitoring window. exit.", flush=True)
        sys.exit(0)

    # facility名でフィルタ（必要な場合のみ）
    cfg = load_config()
    if args.facility:
        target = [f for f in cfg.get("facilities", []) if f.get("name") == args.facility]
        if not target:
            print(f"[WARN] facility '{args.facility}' not found in config.json", flush=True)
            sys.exit(0)
        cfg["facilities"] = target
        tmp = BASE_DIR / "config.temp.json"
        tmp.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), "utf-8")
        global CONFIG_PATH
        CONFIG_PATH = tmp

    run_monitor()

# --------------------------------------------------------------------------------
# エントリポイント
# --------------------------------------------------------------------------------
if __name__ == "__main__":
    main()
