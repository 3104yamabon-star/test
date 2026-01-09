
# -*- coding: utf-8 -*-
"""
さいたま市 施設予約システムの空き状況監視（計測＋任意ダイアログ高速版）
追加・変更点（今回の主目的）:
- goto直後の「任意ダイアログ（同意/OK/確認/閉じる）」処理を超軽量化
  * 存在チェック(count)→クリック(timeout 300–500ms) の高速ルート
  * ラベルごとの区間計測ログを追加（どこで何秒使ったか可視化）
- Playwrightの既定タイムアウトを5秒に短縮（page.set_default_timeout(5000)）
- それ以外の機能（カレンダー枠の限定、日付セルのみ、次月への絶対日付遷移、差分時のみ履歴保存、詳細タイマー）は現状維持

★ 今回のご要望対応:
- 空き状況を表示した段階（月の遷移も含む）で約1.5秒の猶予を追加
  * 環境変数 GRACE_MS（既定 1500ms）で調整可能
  * navigate_to_facility() のクリックシーケンス完了直後に待機
  * click_next_month() でDOM更新待ちが済んだ直後に待機
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

# ====== 環境 ======
try:
    import pytz
except Exception:
    pytz = None

BASE_URL = os.getenv("BASE_URL")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
MONITOR_FORCE = os.getenv("MONITOR_FORCE", "0").strip() == "1"
MONITOR_START_HOUR = int(os.getenv("MONITOR_START_HOUR", "5"))
MONITOR_END_HOUR = int(os.getenv("MONITOR_END_HOUR", "23"))
TIMING_VERBOSE = os.getenv("TIMING_VERBOSE", "0").strip() == "1"

# ★ 追加: 画面安定化のための猶予（ミリ秒）
GRACE_MS = int(os.getenv("GRACE_MS", "1500"))

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_ROOT = Path(os.getenv("OUTPUT_DIR", str(BASE_DIR / "snapshots"))).resolve()
CONFIG_PATH = BASE_DIR / "config.json"

FACILITY_TITLE_ALIAS = {
    "岩槻南部公民館": "岩槻",
    "南浦和コミュニティセンター": "南浦和",
    "岸町公民館": "岸町",
    "鈴谷公民館": "鈴谷",
}

# ====== 計測ユーティリティ ======
@contextmanager
def time_section(title: str):
    start = time.perf_counter()
    print(f"[TIMER] {title}: start", flush=True)
    try:
        yield
    finally:
        end = time.perf_counter()
        print(f"[TIMER] {title}: end ({end - start:.3f}s)", flush=True)

def jst_now() -> datetime.datetime:
    if pytz is None:
        return datetime.datetime.now()
    jst = pytz.timezone("Asia/Tokyo")
    return datetime.datetime.now(jst)

def is_within_monitoring_window(start_hour=5, end_hour=23):
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
    test = root / ".write_test"
    test.write_text(f"ok {jst_now().isoformat()}\n", encoding="utf-8")
    try:
        test.unlink()
    except Exception:
        pass

def safe_mkdir(d: Path): d.mkdir(parents=True, exist_ok=True)
def safe_write_text(p: Path, s: str):
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp"); tmp.write_text(s, "utf-8"); tmp.replace(p)

def safe_element_screenshot(el, out: Path):
    out.parent.mkdir(parents=True, exist_ok=True)
    el.scroll_into_view_if_needed(); el.screenshot(path=str(out))

# ====== 画面安定化用ヘルパー（★追加） ======
def grace_pause(page, label: str = "grace wait"):
    """画面の安定化待ち（既定 1.5s）。GRACE_MS で調整可能。"""
    try:
        ms = max(0, int(GRACE_MS))
    except Exception:
        ms = 1500
    if ms <= 0:
        return
    with time_section(f"{label} ({ms}ms)"):
        page.wait_for_timeout(ms)

# ====== Playwright操作 ======
def try_click_text(page, label: str, timeout_ms: int = 15000, quiet=True) -> bool:
    locators = [
        page.get_by_role("link", name=label, exact=True),
        page.get_by_role("button", name=label, exact=True),
        page.get_by_text(label, exact=True),
        page.locator(f"text={label}"),
    ]
    for locator in locators:
        try:
            if TIMING_VERBOSE:
                with time_section(f"click '{label}' (wait+click)"):
                    locator.wait_for(timeout=timeout_ms)
                    locator.scroll_into_view_if_needed()
                    locator.click(timeout=timeout_ms)
            else:
                locator.wait_for(timeout=timeout_ms)
                locator.scroll_into_view_if_needed()
                locator.click(timeout=timeout_ms)
            return True
        except Exception as e:
            if not quiet:
                print(f"[WARN] try_click_text: {e} (label='{label}')", flush=True)
            continue
    return False

# --- ★ 任意ダイアログの超軽量処理 ---
OPTIONAL_DIALOG_LABELS = ["同意する", "OK", "確認", "閉じる"]

def click_optional_dialogs_fast(page) -> None:
    """
    - 1件あたり <= 0.5s 程度で存在確認+クリック
    - 見つからないなら即スキップ（無駄待ちしない）
    - 詳細な区間計測ログを出す
    """
    for label in OPTIONAL_DIALOG_LABELS:
        with time_section(f"optional-dialog: '{label}'"):
            clicked = False
            # まず role=link / role=button / exact text を素早く count()
            probes = [
                page.get_by_role("link", name=label, exact=True),
                page.get_by_role("button", name=label, exact=True),
                page.get_by_text(label, exact=True),
                page.locator(f"text={label}"),
            ]
            for probe in probes:
                try:
                    c = probe.count()
                    if c > 0:
                        # クリックは短い timeout で
                        try:
                            probe.first.scroll_into_view_if_needed()
                            probe.first.click(timeout=500)  # ★ 500ms
                            clicked = True
                            break
                        except Exception:
                            # 可視化されていない等…次のプローブを試す
                            pass
                except Exception:
                    pass
            # それでも未クリックなら、部分一致をさらに素早く試す（300ms）
            if not clicked:
                try:
                    cand = page.locator(f"a:has-text('{label}')").first
                    if cand.count() > 0:
                        cand.scroll_into_view_if_needed()
                        cand.click(timeout=300)  # ★ 300ms
                        clicked = True
                except Exception:
                    pass
            # クリックできてもできなくても「短時間で」抜けるのが目的

def navigate_to_facility(page, facility: Dict[str, Any]) -> None:
    if not BASE_URL:
        raise RuntimeError("BASE_URL が未設定です。Secrets の BASE_URL に https://saitama.rsv.ws-scs.jp/web/ を設定してください。")
    with time_section("goto BASE_URL"):
        page.goto(BASE_URL, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_load_state("domcontentloaded", timeout=30000)
    # 既定タイムアウトを短縮（局所で延ばす方針）
    page.set_default_timeout(5000)  # ★ 30s → 5s

    # ★ 任意ダイアログを超軽量で処理（合計でも≤数秒で抜ける想定）
    click_optional_dialogs_fast(page)

    # 以降は click_sequence を順に（各セクションは計測ログを出す）
    for label in facility.get("click_sequence", []):
        with time_section(f"click_sequence: '{label}'"):
            ok = try_click_text(page, label, timeout_ms=5000)
            if not ok:
                raise RuntimeError(f"クリック対象が見つかりません：『{label}』（施設: {facility.get('name','')}）")

    # ★ 追加: 空き状況画面を開いた直後の安定化猶予
    grace_pause(page, label="after availability view shown")

def get_current_year_month_text(page, calendar_root=None) -> Optional[str]:
    pat = re.compile(r"(\d{4})\s*年\s*(\d{1,2})\s*月")
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
        if not txt: continue
        m = pat.search(txt)
        if m:
            y, mo = int(m.group(1)), int(m.group(2))
            return f"{y}年{mo}月"
    return None

def locate_calendar_root(page, hint: str, facility: Dict[str, Any] = None):
    with time_section("locate_calendar_root"):
        sel_cfg = (facility or {}).get("calendar_selector")
        if sel_cfg:
            loc = page.locator(sel_cfg)
            if loc.count() > 0:
                return loc.first

        candidates = []
        weekday_markers = ["日曜日","月曜日","火曜日","水曜日","木曜日","金曜日","土曜日","日","月","火","水","木","金","土"]
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
                if hint and hint in t: score += 2
                wk = sum(1 for w in weekday_markers if w in t)
                if wk >= 4: score += 3
                try:
                    cells = el.locator(":scope tbody td, :scope [role='gridcell'], :scope .fc-daygrid-day, :scope .calendar-day")
                    if cells.count() >= 28: score += 3
                except Exception:
                    pass
                if score >= 5:
                    candidates.append((score, el))
        if not candidates:
            raise RuntimeError("カレンダー枠の特定に失敗しました（候補が見つからないため監視を中止）。")
        candidates.sort(key=lambda x:x[0], reverse=True)
        return candidates[0][1]

def dump_calendar_html(calendar_root, out_path: Path):
    with time_section(f"dump_html: {out_path.name}"):
        html = calendar_root.evaluate("el => el.outerHTML")
        safe_write_text(out_path, html)

def take_calendar_screenshot(calendar_root, out_path: Path):
    with time_section(f"screenshot: {out_path.name}"):
        safe_element_screenshot(calendar_root, out_path)

# ====== 月遷移（絶対日付） ======
def _compute_next_month_text(prev: str) -> str:
    try:
        m = re.match(r"(\d{4})年(\d{1,2})月", prev or "")
        if not m: return ""
        y, mo = int(m.group(1)), int(m.group(2))
        if mo == 12: y += 1; mo = 1
        else: mo += 1
        return f"{y}年{mo}月"
    except Exception:
        return ""

def _next_yyyymm01(prev: str) -> Optional[str]:
    m = re.match(r"(\d{4})年(\d{1,2})月", prev or "")
    if not m: return None
    y, mo = int(m.group(1)), int(m.group(2))
    if mo == 12: y += 1; mo = 1
    else: mo += 1
    return f"{y:04d}{mo:02d}01"

def _ym(text: Optional[str]) -> Optional[Tuple[int,int]]:
    if not text: return None
    m = re.match(r"(\d{4})年(\d{1,2})月", text)
    return (int(m.group(1)), int(m.group(2))) if m else None

def _is_forward(prev: str, cur: str) -> bool:
    p, c = _ym(prev), _ym(cur)
    if not p or not c: return False
    (py, pm), (cy, cm) = p, c
    return (pm == 12 and cy == py + 1 and cm == 1) or (cy == py and cm == pm + 1)

def click_next_month(page, label_primary="次の月", calendar_root=None, prev_month_text=None, wait_timeout_ms=20000, facility=None) -> bool:
    def _safe_click(el, note=""):
        if TIMING_VERBOSE:
            with time_section(f"next-month click {note}"):
                el.scroll_into_view_if_needed(); el.click(timeout=2000)
        else:
            el.scroll_into_view_if_needed(); el.click(timeout=2000)

    with time_section("next-month: find & click"):
        clicked = False
        sel_cfg = (facility or {}).get("next_month_selector")
        cands = [sel_cfg] if sel_cfg else []
        cands += ["a:has-text('次の月')", "a:has-text('翌月')"]
        for sel in cands:
            if not sel: continue
            try:
                el = page.locator(sel).first
                if el and el.count() > 0:
                    _safe_click(el, sel); clicked = True; break
            except Exception: pass

        if not clicked and prev_month_text:
            try:
                target = _next_yyyymm01(prev_month_text)
                els = page.locator("a[href*='moveCalender']").all()
                chosen = None; chosen_date = None
                cur01 = None
                m = re.match(r"(\d{4})年(\d{1,2})月", prev_month_text)
                if m: cur01 = f"{int(m.group(1)):04d}{int(m.group(2)):02d}01"
                for e in els:
                    href = e.get_attribute("href") or ""
                    m2 = re.search(r"moveCalender\([^\,]+,[^\,]+,\s*(\d{8})\)", href)
                    if not m2: continue
                    ymd = m2.group(1)
                    if target and ymd == target: chosen, chosen_date = e, ymd; break
                    if cur01 and ymd > cur01 and (chosen_date is None or ymd < chosen_date):
                        chosen, chosen_date = e, ymd
                if chosen:
                    _safe_click(chosen, f"href {chosen_date}"); clicked = True
            except Exception: pass

        if not clicked: return False

    with time_section("next-month: wait outerHTML change"):
        old_html = None
        if calendar_root is not None:
            try: old_html = calendar_root.evaluate("el => el.outerHTML")
            except Exception: old_html = None
        try:
            if old_html:
                page.wait_for_function(
                    """(old)=>{const r=document.querySelector('table.reservation-calendar')

document.querySelector('[role="grid"]')

document.querySelector('table');return r && r.outerHTML!==old;}""",
                    arg=old_html, timeout=wait_timeout_ms
                )
        except Exception: pass

    with time_section("next-month: wait month text change (+1)"):
        goal = _compute_next_month_text(prev_month_text or "")
        try:
            if goal:
                page.wait_for_function(
                    """(g)=>{const t=document.body.innerText

'';return t.includes(g);}""",
                    arg=goal, timeout=wait_timeout_ms
                )
        except Exception: pass

    with time_section("next-month: confirm direction"):
        cur = None
        try: cur = get_current_year_month_text(page, calendar_root=None)
        except Exception: pass
        if prev_month_text and cur and not _is_forward(prev_month_text, cur):
            print(f"[WARN] next-month moved backward: {prev_month_text} -> {cur}", flush=True)
            return False

    # ★ 追加: 月遷移直後の安定化猶予
    grace_pause(page, label="after month transition")

    return True

# ====== 集計 / 保存 ======
from datetime import datetime as _dt
def summarize_vacancies(page, calendar_root, config):
    with time_section("summarize_vacancies"):
        import re as _re
        patterns = config["status_patterns"]
        summary = {"○":0,"△":0,"×":0,"未判定":0}; details=[]
        def _st(raw:str)->Optional[str]:
            txt=(raw or "").strip(); n=txt.replace("　"," ").lower()
            for ch in ["○","〇","△","×"]:
                if ch in txt: return {"〇":"○"}.get(ch,ch)
            for kw in patterns["circle"]:
                if kw.lower() in n: return "○"
            for kw in patterns["triangle"]:
                if kw.lower() in n: return "△"
            for kw in patterns["cross"]:
                if kw.lower() in n: return "×"
            return None
        cands = calendar_root.locator(":scope tbody td, :scope [role='gridcell']")
        for i in range(cands.count()):
            el = cands.nth(i)
            try: txt=(el.inner_text() or "").strip()
            except Exception: continue
            head=txt[:40]
            m=_re.search(r"^([1-9]|[12]\d|3[01])\s*日", head, flags=_re.MULTILINE)
            if not m:
                try:
                    aria=el.get_attribute("aria-label") or ""; title=el.get_attribute("title") or ""
                    m=_re.search(r"([1-9]|[12]\d|3[01])\s*日", aria+" "+title)
                except Exception: pass
            if not m:
                try:
                    imgs=el.locator("img"); jcnt=imgs.count()
                    for j in range(jcnt):
                        alt=imgs.nth(j).get_attribute("alt") or ""; tit=imgs.nth(j).get_attribute("title") or ""
                        mm=_re.search(r"([1-9]|[12]\d|3[01])\s*日", alt+" "+tit)
                        if mm: m=mm; break
                except Exception: pass
            if not m: continue
            day=f"{m.group(0)}"; st=_st(txt)
            if not st:
                try:
                    imgs=el.locator("img"); jcnt=imgs.count()
                    for j in range(jcnt):
                        alt=imgs.nth(j).get_attribute("alt") or ""; tit=imgs.nth(j).get_attribute("title") or ""; src=imgs.nth(j).get_attribute("src") or ""
                        st=_st(alt+" "+tit) or _st(src)
                        if st: break
                except Exception: pass
            if not st:
                try:
                    aria=el.get_attribute("aria-label") or ""; tit=el.get_attribute("title") or ""; cls=(el.get_attribute("class") or "").lower()
                    st=_st(aria+" "+tit)
                    if not st:
                        for kw in config["css_class_patterns"]["circle"]:
                            if kw in cls: st="○"; break
                    if not st:
                        for kw in config["css_class_patterns"]["triangle"]:
                            if kw in cls: st="△"; break
                    if not st:
                        for kw in config["css_class_patterns"]["cross"]:
                            if kw in cls: st="×"; break
                except Exception: pass
            if not st: st="未判定"
            summary[st]+=1; details.append({"day":day,"status":st,"text":txt})
        return summary, details

def facility_month_dir(short: str, month_text: str) -> Path:
    safe_fac = re.sub(r"[\\/:*?\"<>|]+","_", short)
    safe_month = re.sub(r"[\\/:*?\"<>|]+","_", month_text or "unknown_month")
    d = OUTPUT_ROOT / safe_fac / safe_month
    with time_section(f"mkdir outdir: {d}"): safe_mkdir(d)
    return d

def load_last_summary(outdir: Path):
    p = outdir / "status_counts.json"
    if not p.exists(): return None
    try: return json.loads(p.read_text("utf-8")).get("summary")
    except Exception: return None

def summaries_changed(prev, cur) -> bool:
    if prev is None and cur is not None: return True
    if prev is None and cur is None: return False
    for k in ["○","△","×","未判定"]:
        if (prev or {}).get(k,0) != (cur or {}).get(k,0): return True
    return False

def save_calendar_assets(cal_root, outdir: Path, save_ts: bool):
    latest_html = outdir / "calendar.html"
    latest_png = outdir / "calendar.png"
    ts = _dt.now().strftime("%Y%m%d_%H%M%S")
    html_ts = outdir / f"calendar_{ts}.html"
    png_ts = outdir / f"calendar_{ts}.png"
    dump_calendar_html(cal_root, latest_html)
    take_calendar_screenshot(cal_root, latest_png)
    ts_html=ts_png=None
    if save_ts:
        dump_calendar_html(cal_root, html_ts)
        take_calendar_screenshot(cal_root, png_ts)
        ts_html, ts_png = html_ts, png_ts
    return latest_html, latest_png, ts_html, ts_png

# ====== メイン ======
def run_monitor():
    print("[INFO] run_monitor: start", flush=True)
    print(f"[INFO] BASE_DIR={BASE_DIR} cwd={Path.cwd()} OUTPUT_ROOT={OUTPUT_ROOT}", flush=True)
    with time_section("ensure_root_dir"): ensure_root_dir(OUTPUT_ROOT)
    try:
        with time_section("load_config"): config = load_config()
    except Exception as e:
        print(f"[ERROR] config load failed: {e}", flush=True); return
    facilities = config.get("facilities", [])
    if not facilities:
        print("[WARN] config['facilities'] が空です。", flush=True); return

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
                short = FACILITY_TITLE_ALIAS.get(facility.get('name',''), facility.get('name',''))
                outdir = facility_month_dir(short or 'unknown_facility', month_text)
                print(f"[INFO] outdir={outdir}", flush=True)

                summary, details = summarize_vacancies(page, cal_root, config)
                prev = load_last_summary(outdir)
                changed = summaries_changed(prev, summary)
                latest_html, latest_png, ts_html, ts_png = save_calendar_assets(cal_root, outdir, save_ts=changed)

                payload = {
                    "month": month_text, "facility": facility.get('name',''),
                    "summary": summary, "details": details,
                    "run_at": jst_now().strftime("%Y-%m-%d %H:%M:%S JST")
                }
                with time_section("write status_counts.json"):
                    safe_write_text(outdir / "status_counts.json", json.dumps(payload, ensure_ascii=False, indent=2))
                print(f"[INFO] summary({facility.get('name','')} - {month_text}): ○={summary['○']} △={summary['△']} ×={summary['×']} 未判定={summary['未判定']}", flush=True)
                if ts_html and ts_png: print(f"[INFO] saved (timestamped): {ts_html.name}, {ts_png.name}", flush=True)
                print(f"[INFO] saved: {facility.get('name','')} - {month_text} latest=({latest_html.name},{latest_png.name})", flush=True)

                shifts = facility.get("month_shifts", [0,1])
                shifts = sorted(set(int(s) for s in shifts if isinstance(s,(int,float))))
                if 0 not in shifts: shifts.insert(0,0)
                max_shift = max(shifts); prev_month_text = month_text

                for step in range(1, max_shift+1):
                    ok = click_next_month(page, calendar_root=cal_root, prev_month_text=prev_month_text, wait_timeout_ms=20000, facility=facility)
                    if not ok:
                        dbg = OUTPUT_ROOT / "_debug"; safe_mkdir(dbg)
                        with time_section(f"screenshot fail step={step}"):
                            page.screenshot(path=str(dbg / f"failed_next_month_step{step}_{short}.png"))
                        print(f"[WARN] next-month click failed at step={step}", flush=True)
                        break

                    with time_section(f"get_current_month_text(step={step})"):
                        month_text2 = get_current_year_month_text(page) or f"shift_{step}"
                    print(f"[INFO] month(step={step}): {month_text2}", flush=True)

                    cal_root2 = locate_calendar_root(page, month_text2 or "予約カレンダー", facility)
                    outdir2 = facility_month_dir(short or 'unknown_facility', month_text2)
                    print(f"[INFO] outdir(step={step})={outdir2}", flush=True)

                    if step in shifts:
                        summary2, details2 = summarize_vacancies(page, cal_root2, config)
                        prev2 = load_last_summary(outdir2)
                        changed2 = summaries_changed(prev2, summary2)
                        latest_html2, latest_png2, ts_html2, ts_png2 = save_calendar_assets(cal_root2, outdir2, save_ts=changed2)

                        payload2 = {
                            "month": month_text2, "facility": facility.get('name',''),
                            "summary": summary2, "details": details2,
                            "run_at": jst_now().strftime("%Y-%m-%d %H:%M:%S JST")
                        }
                        with time_section("write status_counts.json (step)"):
                            safe_write_text(outdir2 / "status_counts.json", json.dumps(payload2, ensure_ascii=False, indent=2))
                        print(f"[INFO] summary({facility.get('name','')} - {month_text2}): ○={summary2['○']} △={summary2['△']} ×={summary2['×']} 未判定={summary2['未判定']}", flush=True)
                        if ts_html2 and ts_png2: print(f"[INFO] saved (timestamped): {ts_html2.name}, {ts_png2.name}", flush=True)
                        print(f"[INFO] saved: {facility.get('name','')} - {month_text2} latest=({latest_html2.name},{latest_png2.name})", flush=True)

                    cal_root = cal_root2; prev_month_text = month_text2

            except Exception as e:
                dbg = OUTPUT_ROOT / "_debug"; safe_mkdir(dbg)
                shot = dbg / f"exception_{FACILITY_TITLE_ALIAS.get(facility.get('name',''), facility.get('name',''))}_{_dt.now().strftime('%Y%m%d_%H%M%S')}.png"
                with time_section("screenshot exception"):
                    try: page.screenshot(path=str(shot))
                    except Exception: pass
                print(f"[ERROR] run_monitor: 施設処理中に例外: {e} (debug: {shot})", flush=True)
                continue

        browser.close()

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--facility", default=None)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    force = MONITOR_FORCE or args.force
    within, now = is_within_monitoring_window(MONITOR_START_HOUR, MONITOR_END_HOUR)
    if not force:
        if now: print(f"[INFO] JST now: {now.strftime('%Y-%m-%d %H:%M:%S')} (window {MONITOR_START_HOUR}:00-{MONITOR_END_HOUR}:59)", flush=True)
        if not within: print("[INFO] outside monitoring window. exit.", flush=True); sys.exit(0)
    else:
        if now: print(f"[INFO] FORCE RUN enabled. JST now: {now.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

    cfg = load_config()
    if args.facility:
        targets = [f for f in cfg.get("facilities", []) if f.get("name")==args.facility]
        if not targets:
            print(f"[WARN] facility '{args.facility}' not found in config.json", flush=True); sys.exit(0)
        cfg["facilities"] = targets
        tmp = BASE_DIR / "config.temp.json"
        tmp.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), "utf-8")
        global CONFIG_PATH; CONFIG_PATH = tmp

    run_monitor()

if __name__ == "__main__":
    print("[INFO] Starting monitor.py ...", flush=True)
    print(f"[INFO] BASE_DIR={BASE_DIR} cwd={Path.cwd()} OUTPUT_ROOT={OUTPUT_ROOT}", flush=True)
    main()
    print("[INFO] monitor.py finished.", flush=True)
