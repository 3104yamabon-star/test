
# -*- coding: utf-8 -*-
"""
さいたま市 施設予約システムの空き状況監視（計測版）

目的：
- どのステップで時間が掛かっているかを特定するため、画面遷移ごと＆主要処理ごとに
  開始・終了・経過秒を詳細ログ出力。

主な変更点：
- 高精度タイマー（time.perf_counter）を使った区間計測ユーティリティを追加。
- 初回 goto / click_sequence の各クリック / カレンダー枠の特定 / 集計 / 保存 /
  次の月クリック / outerHTML待機 / 月テキスト待機の各ポイントで計測ログ。
- 環境変数 TIMING_VERBOSE=1 でクリック単位の詳細計測を有効化（未設定なら主要区間のみ）。

既存機能は維持：
- 監視時間帯制御（--force / MONITOR_FORCE）
- 施設ページ遷移（click_sequence）
- カレンダー枠の厳密特定（calendar_selector 優先、曜日＋セル数チェック）
- セル抽出は枠内限定、日付が無いセルはスキップ
- 「次の月」遷移の厳密化（'次の月' 厳密一致 → moveCalender(..., YYYYMMDD) の日付一致）
- summary 変化時のみタイムスタンプ付き履歴保存（最新は常に更新）
- 出力先はレポジトリ直下 snapshots/（OUTPUT_DIR 環境変数で変更可）
"""

import os
import sys
import json
import re
import datetime
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Tuple, Dict, Any

from playwright.sync_api import sync_playwright

# JST時間帯ヘルパ
try:
    import pytz
except Exception:
    pytz = None

# === 環境変数 ===
BASE_URL = os.getenv("BASE_URL")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
MONITOR_FORCE = os.getenv("MONITOR_FORCE", "0").strip() == "1"
MONITOR_START_HOUR = int(os.getenv("MONITOR_START_HOUR", "5"))
MONITOR_END_HOUR = int(os.getenv("MONITOR_END_HOUR", "23"))
TIMING_VERBOSE = os.getenv("TIMING_VERBOSE", "0").strip() == "1"

# === 出力パス ===
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_ROOT = Path(os.getenv("OUTPUT_DIR", str(BASE_DIR / "snapshots"))).resolve()
CONFIG_PATH = BASE_DIR / "config.json"

# === 施設短縮名 ===
FACILITY_TITLE_ALIAS = {
    "岩槻南部公民館": "岩槻",
    "南浦和コミュニティセンター": "南浦和",
    "岸町公民館": "岸町",
    "鈴谷公民館": "鈴谷",
}

# ------------------------------
# 計測ユーティリティ
# ------------------------------
@contextmanager
def time_section(title: str):
    """区間計測：開始→終了で経過秒をログ出力"""
    start = time.perf_counter()
    print(f"[TIMER] {title}: start", flush=True)
    try:
        yield
    finally:
        end = time.perf_counter()
        elapsed = end - start
        print(f"[TIMER] {title}: end ({elapsed:.3f}s)", flush=True)

def log_point(label: str):
    """瞬間計測（時刻ポイント）"""
    print(f"[TIMER] point: {label} at {time.perf_counter():.3f}s", flush=True)

# ------------------------------
# 基本ユーティリティ
# ------------------------------
def jst_now() -> datetime.datetime:
    if pytz is None:
        return datetime.datetime.now()
    jst = pytz.timezone("Asia/Tokyo")
    return datetime.datetime.now(jst)

def is_within_monitoring_window(start_hour: int = 5, end_hour: int = 23) -> Tuple[bool, Optional[datetime.datetime]]:
    try:
        now = jst_now()
        return (start_hour <= now.hour <= end_hour), now
    except Exception:
        return True, None

def load_config() -> Dict[str, Any]:
    text = CONFIG_PATH.read_text("utf-8")
    cfg = json.loads(text)
    for key in ["facilities", "status_patterns", "css_class_patterns"]:
        if key not in cfg:
            raise RuntimeError(f"config.json の '{key}' が不足しています")
    return cfg

def ensure_root_dir(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    test_file = root / ".write_test"
    test_file.write_text(f"ok {jst_now().isoformat()} \n", encoding="utf-8")
    if not test_file.exists():
        raise RuntimeError(f"OUTPUT_ROOT 書込みテストに失敗: {test_file}")
    try:
        test_file.unlink()
    except Exception:
        pass

def safe_mkdir(dirpath: Path) -> None:
    dirpath.mkdir(parents=True, exist_ok=True)
    if not dirpath.exists():
        raise RuntimeError(f"ディレクトリ作成に失敗: {dirpath}")

def safe_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)
    if not path.exists() or path.stat().st_size == 0:
        raise RuntimeError(f"ファイル書込みに失敗: {path}")

def safe_element_screenshot(el, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    el.scroll_into_view_if_needed()
    el.screenshot(path=str(out_path))
    if not out_path.exists() or out_path.stat().st_size == 0:
        raise RuntimeError(f"スクリーンショット作成に失敗: {out_path}")

# ------------------------------
# Playwright 操作（遷移）
# ------------------------------
def try_click_text(page, label: str, timeout_ms: int = 15000, quiet: bool = True) -> bool:
    """指定ラベルのリンク/ボタン/テキストをクリック（厳密一致優先）"""
    locators = [
        page.get_by_role("link", name=label, exact=True),
        page.get_by_role("button", name=label, exact=True),
        page.get_by_text(label, exact=True),
        page.locator(f"text={label}"),
    ]
    for locator in locators:
        try:
            if TIMING_VERBOSE:
                with time_section(f"click '{label}' (wait_for)"):
                    locator.wait_for(timeout=timeout_ms)
                with time_section(f"click '{label}' (click)"):
                    locator.scroll_into_view_if_needed()
                    locator.click(timeout=timeout_ms)
            else:
                locator.wait_for(timeout=timeout_ms)
                locator.scroll_into_view_if_needed()
                locator.click(timeout=timeout_ms)
            return True
        except Exception as e:
            if not quiet:
                print(f"[WARN] try_click_text: 例外 {e}（label='{label}'）", flush=True)
            continue
    return False

def navigate_to_facility(page, facility: Dict[str, Any]) -> None:
    """
    トップ → click_sequence の順で施設の当月ページまで到達
    （初回 goto は DOMContentLoaded のみ待機）
    """
    if not BASE_URL:
        raise RuntimeError("BASE_URL が未設定です。Secrets の BASE_URL に https://saitama.rsv.ws-scs.jp/web/ を入れてください。")

    with time_section("goto BASE_URL"):
        page.goto(BASE_URL, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_load_state("domcontentloaded", timeout=30000)

    for opt in ["同意する", "OK", "確認", "閉じる"]:
        try_click_text(page, opt, timeout_ms=2000)

    for label in facility.get("click_sequence", []):
        with time_section(f"click_sequence: '{label}'"):
            ok = try_click_text(page, label)
            if not ok:
                raise RuntimeError(f"クリック対象が見つかりません：『{label}』（施設: {facility.get('name','')}）")

def get_current_year_month_text(page, calendar_root=None) -> Optional[str]:
    pattern = re.compile(r"(\d{4})\s*年\s*(\d{1,2})\s*月")
    targets = []
    try:
        if calendar_root is not None:
            targets.append(calendar_root.inner_text())
    except Exception:
        pass
    try:
        targets.append(page.inner_text("body"))
    except Exception:
        pass

    for txt in targets:
        if not txt:
            continue
        m = pattern.search(txt)
        if m:
            y, mo = int(m.group(1)), int(m.group(2))
            return f"{y}年{mo}月"
    return None

def locate_calendar_root(page, hint: str, facility: Dict[str, Any] = None):
    """カレンダー枠の厳密特定（計測付き）"""
    with time_section("locate_calendar_root"):
        sel_cfg = (facility or {}).get("calendar_selector")
        if sel_cfg:
            loc = page.locator(sel_cfg)
            if loc.count() > 0:
                return loc.first

        candidates = []
        weekday_markers = ["日曜日", "月曜日", "火曜日", "水曜日", "木曜日", "金曜日", "土曜日", "日", "月", "火", "水", "木", "金", "土"]

        for sel in ["[role='grid']", "table", "section", "div.calendar", "div"]:
            loc = page.locator(sel)
            cnt = loc.count()
            for i in range(cnt):
                el = loc.nth(i)
                try:
                    t = (el.inner_text() or "").strip()
                except Exception:
                    continue

                score = 0
                if hint and hint in t:
                    score += 2

                wk = sum(1 for w in weekday_markers if w in t)
                if wk >= 4:
                    score += 3

                try:
                    cells = el.locator(":scope tbody td, :scope [role='gridcell'], :scope .fc-daygrid-day, :scope .calendar-day")
                    cell_cnt = cells.count()
                    if cell_cnt >= 28:
                        score += 3
                except Exception:
                    pass

                if score >= 5:
                    candidates.append((score, el))

        if not candidates:
            raise RuntimeError("カレンダー枠の特定に失敗しました（候補が見つからないため監視を中止）。")

        candidates.sort(key=lambda x: x[0], reverse=True)
        return candidates[0][1]

def dump_calendar_html(calendar_root, out_path: Path) -> None:
    html = calendar_root.evaluate("el => el.outerHTML")
    safe_write_text(out_path, html)

def take_calendar_screenshot(calendar_root, out_path: Path) -> None:
    safe_element_screenshot(calendar_root, out_path)

# ------------------------------
# 月遷移（絶対日付クリック＋待機計測）
# ------------------------------
def _compute_next_month_text(prev_month_text: str) -> str:
    try:
        m = re.match(r"(\d{4})年(\d{1,2})月", prev_month_text or "")
        if not m:
            return ""
        y = int(m.group(1)); mo = int(m.group(2))
        if mo == 12:
            y += 1; mo = 1
        else:
            mo += 1
        return f"{y}年{mo}月"
    except Exception:
        return ""

def _next_yyyymm01(prev_month_text: str) -> Optional[str]:
    m = re.match(r"(\d{4})年(\d{1,2})月", prev_month_text or "")
    if not m:
        return None
    y = int(m.group(1)); mo = int(m.group(2))
    if mo == 12:
        y += 1; mo = 1
    else:
        mo += 1
    return f"{y:04d}{mo:02d}01"

def _ym_from_text(text: Optional[str]) -> Optional[Tuple[int, int]]:
    if not text:
        return None
    m = re.match(r"(\d{4})年(\d{1,2})月", text)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))

def _is_forward(prev: str, cur: str) -> bool:
    p = _ym_from_text(prev); c = _ym_from_text(cur)
    if not p or not c:
        return False
    py, pm = p; cy, cm = c
    if pm == 12:
        return cy == py + 1 and cm == 1
    return cy == py and cm == pm + 1

def click_next_month(page,
                     label_primary: str = "次の月",
                     calendar_root=None,
                     prev_month_text: Optional[str] = None,
                     wait_timeout_ms: int = 20000,
                     facility: Dict[str, Any] = None) -> bool:
    """「次の月」へ遷移（計測付き）"""
    def _safe_click(el, note=""):
        if TIMING_VERBOSE:
            with time_section(f"next-month click {note}"):
                el.scroll_into_view_if_needed()
                el.click(timeout=2000)
        else:
            el.scroll_into_view_if_needed()
            el.click(timeout=2000)

    with time_section("next-month: find & click"):
        clicked = False

        # 1) '次の月' 厳密一致（施設固有セレクタがあるならそれも利用）
        sel_cfg = (facility or {}).get("next_month_selector")
        candidates = []
        if sel_cfg:
            candidates.append(sel_cfg)
        candidates += ["a:has-text('次の月')", "a:has-text('翌月')"]

        for sel in candidates:
            try:
                el = page.locator(sel).first
                if el and el.count() > 0:
                    _safe_click(el, sel)
                    clicked = True
                    break
            except Exception:
                pass

        # 2) moveCalender(..., ..., YYYYMMDD) の日付一致
        if not clicked and prev_month_text:
            try:
                target = _next_yyyymm01(prev_month_text)
                els = page.locator("a[href*='moveCalender']").all()
                chosen = None
                chosen_date = None
                m = re.match(r"(\d{4})年(\d{1,2})月", prev_month_text)
                cur_yyyymm01 = None
                if m:
                    cur_yyyymm01 = f"{int(m.group(1)):04d}{int(m.group(2)):02d}01"

                for e in els:
                    href = e.get_attribute("href") or ""
                    m2 = re.search(r"moveCalender\([^,]+,[^,]+,\s*(\d{8})\)", href)
                    if not m2:
                        continue
                    ymd = m2.group(1)  # YYYYMMDD
                    if target and ymd == target:
                        chosen = e
                        chosen_date = ymd
                        break
                    if cur_yyyymm01 and ymd > cur_yyyymm01:
                        if chosen_date is None or ymd < chosen_date:
                            chosen = e
                            chosen_date = ymd

                if chosen:
                    _safe_click(chosen, f"href {chosen_date}")
                    clicked = True
            except Exception:
                pass

        if not clicked:
            return False

    # 待機：outerHTML の変化
    with time_section("next-month: wait outerHTML change"):
        old_html = None
        if calendar_root is not None:
            try:
                old_html = calendar_root.evaluate("el => el.outerHTML")
            except Exception:
                old_html = None

        try:
            if old_html:
                page.wait_for_function(
                    """(old) => {
                        const root =
                            document.querySelector('table.reservation-calendar')
                            || document.querySelector('[role="grid"]')
                            || document.querySelector('table');
                        if (!root) return false;
                        return root.outerHTML !== old;
                    }""",
                    arg=old_html,
                    timeout=wait_timeout_ms
                )
        except Exception:
            pass

    # 補助：月テキストが +1 方向へ行ったか
    with time_section("next-month: wait month text change (+1)"):
        next_goal = _compute_next_month_text(prev_month_text or "")
        try:
            if next_goal:
                page.wait_for_function(
                    """(goal) => {
                        const txt = document.body.innerText || '';
                        return txt.includes(goal);
                    }""",
                    arg=next_goal,
                    timeout=wait_timeout_ms
                )
        except Exception:
            pass

    # 実際の現在テキストを取得して方向検知
    with time_section("next-month: confirm direction"):
        cur_text = None
        try:
            cur_text = get_current_year_month_text(page, calendar_root=None)
        except Exception:
            cur_text = None

        if prev_month_text and cur_text and not _is_forward(prev_month_text, cur_text):
            print(f"[WARN] next-month moved backward: {prev_month_text} -> {cur_text}", flush=True)
            return False

    return True

# ------------------------------
# 空き状況集計（○/△/×、計測付き）
# ------------------------------
def summarize_vacancies(page, calendar_root, config) -> Tuple[Dict[str, int], list]:
    with time_section("summarize_vacancies"):
        import re as _re

        patterns = config["status_patterns"]
        summary = {"○": 0, "△": 0, "×": 0, "未判定": 0}
        details = []

        def _status_from_text(raw: str) -> Optional[str]:
            txt = (raw or "").strip()
            txt_norm = txt.replace("　", " ").lower()

            for ch in ["○", "〇", "△", "×"]:
                if ch in txt:
                    return {"〇": "○"}.get(ch, ch)

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

        candidates = calendar_root.locator(":scope tbody td, :scope [role='gridcell']")
        cnt = candidates.count()

        for i in range(cnt):
            el = candidates.nth(i)
            try:
                txt = (el.inner_text() or "").strip()
            except Exception:
                continue

            head = txt[:40]
            mday = _re.search(r"^([1-9]|[12]\d|3[01])\s*日", head, flags=_re.MULTILINE)

            if not mday:
                try:
                    aria = el.get_attribute("aria-label") or ""
                    title = el.get_attribute("title") or ""
                    mday = _re.search(r"([1-9]|[12]\d|3[01])\s*日", aria + " " + title)
                except Exception:
                    pass

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

            if not mday:
                continue

            day_label = f"{mday.group(0)}"
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

# ------------------------------
# 保存ユーティリティ（履歴は変化時のみ、計測付き）
# ------------------------------
from datetime import datetime as _dt

def facility_month_dir(f_short: str, month_text: str) -> Path:
    safe_fac = re.sub(r"[\\/:*?\"<>|]+", "_", f_short)
    safe_month = re.sub(r"[\\/:*?\"<>|]+", "_", month_text or "unknown_month")
    d = OUTPUT_ROOT / safe_fac / safe_month
    with time_section(f"mkdir outdir: {d}"):
        safe_mkdir(d)
    return d

def load_last_summary(outdir: Path) -> Optional[Dict[str, int]]:
    fp = outdir / "status_counts.json"
    if not fp.exists():
        return None
    try:
        data = json.loads(fp.read_text("utf-8"))
        return data.get("summary")
    except Exception:
        return None

def summaries_changed(prev: Optional[Dict[str, int]], cur: Dict[str, int]) -> bool:
    if prev is None and cur is not None:
        return True
    if prev is None and cur is None:
        return False
    keys = {"○", "△", "×", "未判定"}
    for k in keys:
        if prev.get(k, 0) != cur.get(k, 0):
            return True
    return False

def dump_calendar_html(calendar_root, out_path: Path) -> None:
    with time_section(f"dump_html: {out_path.name}"):
        html = calendar_root.evaluate("el => el.outerHTML")
        safe_write_text(out_path, html)

def take_calendar_screenshot(calendar_root, out_path: Path) -> None:
    with time_section(f"screenshot: {out_path.name}"):
        safe_element_screenshot(calendar_root, out_path)

def save_calendar_assets(cal_root, outdir: Path, save_timestamped: bool) -> Tuple[Path, Path, Optional[Path], Optional[Path]]:
    """
    最新（calendar.html / calendar.png）は常に上書き。
    変化時のみタイムスタンプ付き履歴を追加。
    戻り値: (latest_html, latest_png, ts_html or None, ts_png or None)
    """
    latest_html = outdir / "calendar.html"
    latest_png = outdir / "calendar.png"

    ts = _dt.now().strftime("%Y%m%d_%H%M%S")
    html_ts = outdir / f"calendar_{ts}.html"
    png_ts  = outdir / f"calendar_{ts}.png"

    dump_calendar_html(cal_root, latest_html)
    take_calendar_screenshot(cal_root, latest_png)

    ts_html_out = None
    ts_png_out = None
    if save_timestamped:
        dump_calendar_html(cal_root, html_ts)
        take_calendar_screenshot(cal_root, png_ts)
        ts_html_out = html_ts
        ts_png_out  = png_ts

    for p in [latest_html, latest_png] + ([html_ts, png_ts] if save_timestamped else []):
        if not p.exists() or p.stat().st_size == 0:
            raise RuntimeError(f"保存確認に失敗（存在/サイズゼロ）: {p}")

    return latest_html, latest_png, ts_html_out, ts_png_out

# ------------------------------
# メイン処理
# ------------------------------
def run_monitor() -> None:
    print("[INFO] run_monitor: start", flush=True)
    print(f"[INFO] BASE_DIR={BASE_DIR}  cwd={Path.cwd()}  OUTPUT_ROOT={OUTPUT_ROOT}", flush=True)

    with time_section("ensure_root_dir"):
        ensure_root_dir(OUTPUT_ROOT)

    try:
        with time_section("load_config"):
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

                with time_section("get_current_year_month_text"):
                    month_text = get_current_year_month_text(page) or "unknown"
                print(f"[INFO] current month: {month_text}", flush=True)

                cal_root = locate_calendar_root(page, month_text or "予約カレンダー", facility)
                fshort = FACILITY_TITLE_ALIAS.get(facility.get('name',''), facility.get('name',''))
                outdir = facility_month_dir(fshort or 'unknown_facility', month_text)
                print(f"[INFO] outdir={outdir}", flush=True)

                # 当月の集計＆保存
                summary, details = summarize_vacancies(page, cal_root, config)
                prev_summary = load_last_summary(outdir)
                changed = summaries_changed(prev_summary, summary)

                latest_html, latest_png, ts_html, ts_png = save_calendar_assets(cal_root, outdir, save_timestamped=changed)

                payload = {
                    "month": month_text,
                    "facility": facility.get('name',''),
                    "summary": summary,
                    "details": details,
                    "run_at": jst_now().strftime("%Y-%m-%d %H:%M:%S JST")
                }
                with time_section("write status_counts.json"):
                    safe_write_text(outdir / "status_counts.json", json.dumps(payload, ensure_ascii=False, indent=2))

                print(
                    f"[INFO] summary({facility.get('name','')} - {month_text}): "
                    f"○={summary['○']} △={summary['△']} ×={summary['×']} 未判定={summary['未判定']}",
                    flush=True
                )
                if ts_html and ts_png:
                    print(f"[INFO] saved (timestamped): {ts_html.name}, {ts_png.name}", flush=True)
                print(f"[INFO] saved: {facility.get('name','')} - {month_text}  latest=({latest_html.name},{latest_png.name})", flush=True)

                # 以降、「次の月」を連続クリック → キャプチャ＆集計
                shifts = facility.get("month_shifts", [0, 1])
                shifts = sorted(set(int(s) for s in shifts if isinstance(s, (int, float))))
                if 0 not in shifts:
                    shifts.insert(0, 0)
                max_shift = max(shifts)
                next_label = config.get("next_month_label", "次の月")
                prev_month_text = month_text

                for step in range(1, max_shift + 1):
                    ok = click_next_month(
                        page,
                        label_primary=next_label,
                        calendar_root=cal_root,
                        prev_month_text=prev_month_text,
                        wait_timeout_ms=20000,
                        facility=facility
                    )
                    if not ok:
                        failed_dir = OUTPUT_ROOT / "_debug"
                        safe_mkdir(failed_dir)
                        failed = failed_dir / f"failed_next_month_step{step}_{fshort}.png"
                        with time_section(f"screenshot fail step={step}"):
                            page.screenshot(path=str(failed))
                        print(f"[WARN] next-month click failed at step={step} (full-page captured: {failed})", flush=True)
                        break

                    with time_section(f"get_current_month_text(step={step})"):
                        month_text2 = get_current_year_month_text(page, calendar_root=None) or f"shift_{step}"
                    print(f"[INFO] month(step={step}): {month_text2}", flush=True)

                    cal_root2 = locate_calendar_root(page, month_text2 or "予約カレンダー", facility)
                    outdir2 = facility_month_dir(fshort or 'unknown_facility', month_text2)
                    print(f"[INFO] outdir(step={step})={outdir2}", flush=True)

                    if step in shifts:
                        summary2, details2 = summarize_vacancies(page, cal_root2, config)
                        prev_summary2 = load_last_summary(outdir2)
                        changed2 = summaries_changed(prev_summary2, summary2)

                        latest_html2, latest_png2, ts_html2, ts_png2 = save_calendar_assets(cal_root2, outdir2, save_timestamped=changed2)

                        payload2 = {
                            "month": month_text2,
                            "facility": facility.get('name',''),
                            "summary": summary2,
                            "details": details2,
                            "run_at": jst_now().strftime("%Y-%m-%d %H:%M:%S JST")
                        }
                        with time_section("write status_counts.json (step)"):
                            safe_write_text(outdir2 / "status_counts.json", json.dumps(payload2, ensure_ascii=False, indent=2))

                        print(
                            f"[INFO] summary({facility.get('name','')} - {month_text2}): "
                            f"○={summary2['○']} △={summary2['△']} ×={summary2['×']} 未判定={summary2['未判定']}",
                            flush=True
                        )
                        if ts_html2 and ts_png2:
                            print(f"[INFO] saved (timestamped): {ts_html2.name}, {ts_png2.name}", flush=True)
                        print(f"[INFO] saved: {facility.get('name','')} - {month_text2}  latest=({latest_html2.name},{latest_png2.name})", flush=True)

                    cal_root = cal_root2
                    prev_month_text = month_text2

            except Exception as e:
                dbg_dir = OUTPUT_ROOT / "_debug"
                safe_mkdir(dbg_dir)
                shot = dbg_dir / f"exception_{FACILITY_TITLE_ALIAS.get(facility.get('name',''), facility.get('name',''))}_{_dt.now().strftime('%Y%m%d_%H%M%S')}.png"
                try:
                    with time_section("screenshot exception"):
                        page.screenshot(path=str(shot))
                except Exception:
                    pass
                print(f"[ERROR] run_monitor: 施設処理中に例外: {e}  (debug: {shot})", flush=True)
                continue

        browser.close()

# ------------------------------
# CLI
# ------------------------------
def main() -> None:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--facility", default=None, help="監視対象施設名を指定（並列実行用）")
    parser.add_argument("--force", action="store_true", help="監視時間帯ガードを無効化して強制実行")
    args = parser.parse_args()

    force = MONITOR_FORCE or args.force
    within, now = is_within_monitoring_window(MONITOR_START_HOUR, MONITOR_END_HOUR)

    if not force:
        if now:
            print(f"[INFO] JST now: {now.strftime('%Y-%m-%d %H:%M:%S')} (window {MONITOR_START_HOUR}:00-{MONITOR_END_HOUR}:59)", flush=True)
        if not within:
            print("[INFO] outside monitoring window. exit.", flush=True)
            sys.exit(0)
    else:
        if now:
            print(f"[INFO] FORCE RUN enabled. JST now: {now.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

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

# ------------------------------
# エントリポイント
# ------------------------------
if __name__ == "__main__":
    print("[INFO] Starting monitor.py ...", flush=True)
    print(f"[INFO] BASE_DIR={BASE_DIR}  cwd={Path.cwd()}  OUTPUT_ROOT={OUTPUT_ROOT}", flush=True)
    main()
    print("[INFO] monitor.py finished.", flush=True)
