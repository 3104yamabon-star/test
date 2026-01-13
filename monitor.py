
# -*- coding: utf-8 -*-
"""
さいたま市 施設予約システム 空き状況監視（フル版 + 時間帯表示の詳細ログ）
- 月間カレンダー（○/△/×）の差分通知
- 改善した日を「週表示（時間帯表示）」へ遷移し、時間帯（例：15時～17時）まで抽出・通知
- 施設別UIに対応する facility-aware 設計（special_selectors / special_pre_actions / step_hints / detail_view）
- スナップショット保存・ローテーション、Discord通知（embed/text両対応）
- 週表示への遷移・抽出の各段階でログ＆証跡（HTML/PNG）を保存（TS_DEBUG=1）
"""

import os
import sys
import re
import json
import time
import datetime
from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List
from contextlib import contextmanager

# Playwright (sync API)
from playwright.sync_api import sync_playwright

# ====== 環境 ======
try:
    import pytz
except Exception:
    pytz = None

try:
    import jpholiday  # 祝日判定（任意）
except Exception:
    jpholiday = None

BASE_URL = os.getenv("BASE_URL")  # 例: https://saitama.rsv.ws-scs.jp/web/
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
MONITOR_FORCE = os.getenv("MONITOR_FORCE", "0").strip() == "1"
MONITOR_START_HOUR = int(os.getenv("MONITOR_START_HOUR", "5"))
MONITOR_END_HOUR = int(os.getenv("MONITOR_END_HOUR", "23"))
TIMING_VERBOSE = os.getenv("TIMING_VERBOSE", "0").strip() == "1"
FAST_ROUTES = os.getenv("FAST_ROUTES", "0").strip() == "1"

GRACE_MS_DEFAULT = 1000
try:
    GRACE_MS = max(0, int(os.getenv("GRACE_MS", str(GRACE_MS_DEFAULT))))
except Exception:
    GRACE_MS = GRACE_MS_DEFAULT

INCLUDE_HOLIDAY_FLAG = os.getenv("DISCORD_INCLUDE_HOLIDAY", "1").strip() == "1"

# 時間帯ログ（Transition Step Debug）
DEBUG_TS = os.getenv("TS_DEBUG", "1").strip() == "1"  # 1=詳細ログON / 0=OFF

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_ROOT = Path(os.getenv("OUTPUT_DIR", str(BASE_DIR / "snapshots"))).resolve()
CONFIG_PATH = BASE_DIR / "config.json"

# 施設名 → 短縮表示
FACILITY_TITLE_ALIAS = {
    "岩槻南部公民館": "岩槻",
    "南浦和コミュニティセンター": "南浦和",
    "岸町公民館": "岸町",
    "鈴谷公民館": "鈴谷",
    "浦和駒場体育館": "駒場",
}

# ====== ログ補助 ======
def _log(msg: str):
    if DEBUG_TS:
        print(f"[TS] {msg}", flush=True)

def _screenshot(page, path: Path):
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        page.screenshot(path=str(path))
        _log(f"screenshot saved: {path.name}")
    except Exception as e:
        _log(f"shot failed: {e}")

def _dump_week_html(page, path: Path):
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        # 週表示テーブルがあればそれ、なければ body 全体
        if page.locator("table.akitablelist").count() > 0:
            html = page.locator("table.akitablelist").first.evaluate("el => el.outerHTML")
        else:
            html = page.evaluate("() => document.documentElement.outerHTML")
        safe_write_text(path, html)
        _log(f"html saved: {path.name} ({len(html)} chars)")
    except Exception as e:
        _log(f"html dump failed: {e}")

# ====== ユーティリティ ======
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

def safe_mkdir(d: Path):
    d.mkdir(parents=True, exist_ok=True)

def safe_write_text(p: Path, s: str):
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(s, "utf-8")
    tmp.replace(p)

def safe_element_screenshot(el, out: Path):
    out.parent.mkdir(parents=True, exist_ok=True)
    el.scroll_into_view_if_needed()
    el.screenshot(path=str(out))

# ====== Playwright 操作 ======
def enable_fast_routes(page):
    """不要フォント/解析ブロックを抑止（UIに不要な範囲のみ）"""
    block_exts = (".woff", ".woff2", ".ttf")
    block_hosts = ("www.google-analytics.com", "googletagmanager.com")
    def handler(route):
        url = route.request.url
        if url.endswith(block_exts) or any(h in url for h in block_hosts):
            return route.abort()
        return route.continue_()
    page.route("**/*", handler)

def try_click_text(page, label: str, timeout_ms: int = 5000, quiet=True) -> bool:
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
        except Exception:
            if not quiet:
                print(f"[WARN] try_click_text failed (label='{label}')", flush=True)
            continue
    return False

OPTIONAL_DIALOG_LABELS = ["同意する", "OK", "確認", "閉じる"]
def click_optional_dialogs_fast(page) -> None:
    for label in OPTIONAL_DIALOG_LABELS:
        clicked = False
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
                    try:
                        probe.first.scroll_into_view_if_needed()
                        probe.first.click(timeout=500)
                        clicked = True
                        break
                    except Exception:
                        pass
            except Exception:
                pass
        if not clicked:
            try:
                cand = page.locator(f"a:has-text('{label}')").first
                if cand.count() > 0:
                    cand.scroll_into_view_if_needed()
                    cand.click(timeout=300)
            except Exception:
                pass

# クリック後の「次ステップ準備」を race で待つ
HINTS: Dict[str, str] = {
    "施設の空き状況": ".availability-grid, #availability, .facility-list",
    "利用目的から": ".category-cards, .purpose-list",
    "屋内スポーツ": ".sport-list, .sport-cards",
    "バドミントン": ".facility-list, .results-grid",
}
def wait_next_step_ready(page, css_hint: Optional[str] = None) -> None:
    deadline = time.perf_counter() + 0.9
    last_url = page.url
    while time.perf_counter() < deadline:
        try:
            if page.url != last_url:
                return
            if css_hint and page.locator(css_hint).count() > 0:
                return
        except Exception:
            pass
        page.wait_for_timeout(120)

# ====== facility-aware ヘルパ ======
def _run_pre_actions(page, actions: List[str]):
    if not actions:
        return
    for act in actions:
        try:
            if isinstance(act, str) and act.startswith("SCROLL:"):
                xy = act.split(":", 1)[1]
                x_str, y_str = xy.split(",", 1)
                x, y = int(x_str.strip()), int(y_str.strip())
                page.evaluate("window.scrollTo(arguments[0], arguments[1]);", x, y)
            elif isinstance(act, str) and act.startswith("WAIT_MS:"):
                ms = int(act.split(":", 1)[1].strip())
                page.wait_for_timeout(ms)
        except Exception:
            pass

def _get_step_hint(facility: Dict[str, Any], label: str) -> str:
    hints = (facility or {}).get("step_hints") or {}
    if label in hints:
        return hints.get(label) or ""
    return HINTS.get(label) or ""

def _try_click_with_special_selector(page, facility: Dict[str, Any], label: str) -> bool:
    spec = (facility or {}).get("special_selectors") or {}
    sels: List[str] = spec.get(label) or []
    for sel in sels:
        try:
            el = page.locator(sel).first
            if el and el.count() > 0:
                el.scroll_into_view_if_needed()
                el.click(timeout=2000)
                return True
        except Exception:
            continue
    return False

def click_sequence_fast(page, labels: List[str], facility: Dict[str, Any] = None) -> None:
    for i, label in enumerate(labels):
        with time_section(f"click_sequence: '{label}'"):
            pre_actions_all = (facility or {}).get("special_pre_actions") or {}
            _run_pre_actions(page, pre_actions_all.get(label) or [])

            clicked = _try_click_with_special_selector(page, facility, label)
            if not clicked:
                ok = try_click_text(page, label, timeout_ms=5000)
                if not ok:
                    raise RuntimeError(f"クリック対象が見つかりません：『{label}』")

            if i + 1 < len(labels):
                hint = _get_step_hint(facility, label)
                with time_section("wait next step ready (race)"):
                    wait_next_step_ready(page, css_hint=hint)

# ====== ナビゲーション ======
def navigate_to_facility(page, facility: Dict[str, Any]) -> None:
    if not BASE_URL:
        raise RuntimeError("BASE_URL が未設定です。Secrets の BASE_URL に https://saitama.rsv.ws-scs.jp/web/ を設定してください。")
    with time_section("goto BASE_URL"):
        page.goto(BASE_URL, wait_until="domcontentloaded", timeout=30000)
    if FAST_ROUTES:
        enable_fast_routes(page)
    page.add_style_tag(content="*{animation-duration:0s !important; transition-duration:0s !important;}")
    page.set_default_timeout(5000)
    click_optional_dialogs_fast(page)
    click_sequence_fast(page, facility.get("click_sequence", []), facility)
    wait_calendar_ready(page, facility)

def wait_calendar_ready(page, facility: Dict[str, Any]) -> None:
    with time_section("wait calendar root ready"):
        deadline = time.perf_counter() + 1.5
        while time.perf_counter() < deadline:
            try:
                cells = page.locator("[role='gridcell'], table.reservation-calendar tbody td, .fc-daygrid-day, .calendar-day")
                if cells.count() >= 28:
                    return
            except Exception:
                pass
            page.wait_for_timeout(150)
    sel_cfg = facility.get("calendar_selector") or "table.reservation-calendar"
    try:
        page.locator(sel_cfg).first.wait_for(state="visible", timeout=300)
        return
    except Exception:
        for alt in ("[role='grid']", "table.reservation-calendar", "table"):
            try:
                page.locator(alt).first.wait_for(state="visible", timeout=300)
                return
            except Exception:
                continue
    print("[WARN] calendar ready check timed out; proceeding optimistically.", flush=True)

def get_current_year_month_text(page, calendar_root=None) -> Optional[str]:
    pat = re.compile(r"(\d{4})\s*年\s*(\d{1,2})\s*月")
    targets: List[str] = []
    if calendar_root is None:
        locs = [page.locator("table.reservation-calendar").first, page.locator("[role='grid']").first]
        for loc in locs:
            try:
                if loc and loc.count() > 0:
                    calendar_root = loc
                    break
            except Exception:
                continue
    if calendar_root is not None:
        try:
            targets.append(calendar_root.inner_text())
        except Exception:
            pass
    if not targets:
        try:
            targets.append(page.inner_text("body"))
        except Exception:
            pass
    for txt in targets:
        if not txt:
            continue
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
        for sel in ("[role='grid']", "table", "section", "div.calendar", "div"):
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
            raise RuntimeError("カレンダー枠の特定に失敗（候補が見つからないため監視を中止）。")
        candidates.sort(key=lambda x: x[0], reverse=True)
        return candidates[0][1]

# ====== 月移動（moveCalender ベース） ======
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
                    m2 = re.search(r"moveCalender\([^,]+,[^,]+,\s*(\d{8})\)", href)
                    if not m2: continue
                    ymd = m2.group(1)
                    if target and ymd == target: chosen, chosen_date = e, ymd; break
                    if cur01 and ymd > cur01 and (chosen_date is None or ymd < chosen_date):
                        chosen, chosen_date = e, ymd
                if chosen:
                    _safe_click(chosen, f"href {chosen_date}"); clicked = True
            except Exception: pass

        if not clicked: return False

    with time_section("next-month: wait month text change (+1)"):
        goal = _compute_next_month_text(prev_month_text or "")
        try:
            if goal:
                page.wait_for_function(
                    """(g)=>{ return document.body.innerText.includes(g); }""",
                    arg=goal, timeout=wait_timeout_ms
                )
        except Exception:
            pass

    with time_section("next-month: confirm direction"):
        cur = None
        try: cur = get_current_year_month_text(page, calendar_root=None)
        except Exception: pass
        if prev_month_text and cur and not _is_forward(prev_month_text, cur):
            print(f"[WARN] next-month moved backward: {prev_month_text} -> {cur}", flush=True)
            return False
    return True

# ====== 集計 / HTML解析 ======
from datetime import datetime as _dt

def _st_from_text_and_src(raw: str, patterns: Dict[str, List[str]]) -> Optional[str]:
    if raw is None: return None
    txt = raw.strip()
    n = txt.replace("　", " ").lower()
    for ch in ["○", "〇", "△", "×"]:
        if ch in txt: return {"〇": "○"}.get(ch, ch)
    for kw in patterns["circle"]:
        if kw.lower() in n: return "○"
    for kw in patterns["triangle"]:
        if kw.lower() in n: return "△"
    for kw in patterns["cross"]:
        if kw.lower() in n: return "×"
    return None

def _status_from_class(cls: str, css_class_patterns: Dict[str, List[str]]) -> Optional[str]:
    if not cls: return None
    c = cls.lower()
    for kw in css_class_patterns["circle"]:
        if kw in c: return "○"
    for kw in css_class_patterns["triangle"]:
        if kw in c: return "△"
    for kw in css_class_patterns["cross"]:
        if kw in c: return "×"
    return None

def _extract_td_blocks(html: str) -> List[Dict[str, str]]:
    td_blocks: List[Dict[str, str]] = []
    for m in re.finditer(r"<td\b([^>]*)>(.*?)</td>", html, flags=re.IGNORECASE | re.DOTALL):
        attrs = m.group(1) or ""
        inner = m.group(2) or ""
        cls = ""
        title = ""
        aria = ""
        mcls = re.search(r'class\s*=\s*"([^"]*)"', attrs, flags=re.IGNORECASE)
        if mcls: cls = mcls.group(1)
        mtitle = re.search(r'title\s*=\s*"([^"]*)"', attrs, flags=re.IGNORECASE)
        if mtitle: title = mtitle.group(1)
        maria = re.search(r'aria-label\s*=\s*"([^"]*)"', attrs, flags=re.IGNORECASE)
        if maria: aria = maria.group(1)
        td_blocks.append({"attrs": attrs, "class": cls, "title": title, "aria": aria, "inner": inner})
    return td_blocks

def _inner_text_like(html_fragment: str) -> str:
    s = re.sub(r"<br\s*/?>", " ", html_fragment, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def _find_day_in_text(text: str) -> Optional[str]:
    m = re.search(r"([1-9]\d?|1\d|2\d|3[01])\s*日", text)
    return m.group(0) if m else None

def summarize_vacancies(page, calendar_root, config):
    with time_section("summarize_vacancies(html-parse)"):
        patterns = config["status_patterns"]
        css_class_patterns = config["css_class_patterns"]
        summary = {"○": 0, "△": 0, "×": 0, "未判定": 0}
        details: List[Dict[str, str]] = []
        html = ""
        try:
            html = calendar_root.evaluate("el => el.outerHTML")
        except Exception:
            return _summarize_vacancies_fallback(page, calendar_root, config)

        td_blocks = _extract_td_blocks(html)
        for td in td_blocks:
            inner = td["inner"]
            text_like = _inner_text_like(inner)
            day = _find_day_in_text(text_like)
            if not day:
                attr_text = " ".join([td.get("title", ""), td.get("aria", "")])
                day = _find_day_in_text(attr_text)
            if not day:
                for mm in re.finditer(r"<img\b([^>]*)>", inner, flags=re.IGNORECASE):
                    img_attrs = mm.group(1) or ""
                    alt = ""
                    ititle = ""
                    malt = re.search(r'alt\s*=\s*"([^"]*)"', img_attrs, flags=re.IGNORECASE)
                    if malt: alt = malt.group(1) or ""
                    mti = re.search(r'title\s*=\s*"([^"]*)"', img_attrs, flags=re.IGNORECASE)
                    if mti: ititle = mti.group(1) or ""
                    dd = _find_day_in_text(f"{alt} {ititle}")
                    if dd:
                        day = dd
                        break
            if not day:
                continue

            st = _st_from_text_and_src(text_like, patterns)
            if not st:
                for mm in re.finditer(r"<img\b([^>]*)>", inner, flags=re.IGNORECASE):
                    img_attrs = mm.group(1) or ""
                    alt = ""
                    ititle = ""
                    src = ""
                    malt = re.search(r'alt\s*=\s*"([^"]*)"', img_attrs, flags=re.IGNORECASE)
                    if malt: alt = malt.group(1) or ""
                    mti = re.search(r'title\s*=\s*"([^"]*)"', img_attrs, flags=re.IGNORECASE)
                    if mti: ititle = mti.group(1) or ""
                    msrc = re.search(r'src\s*=\s*"([^"]*)"', img_attrs, flags=re.IGNORECASE)
                    if msrc: src = msrc.group(1) or ""
                    st = _st_from_text_and_src(f"{alt} {ititle} {src}", patterns)
                    if st:
                        break
            if not st:
                st = _status_from_class(td.get("class", ""), css_class_patterns)
            if not st:
                st = "未判定"
            summary[st] += 1
            details.append({"day": day, "status": st, "text": text_like})
        return summary, details

def _summarize_vacancies_fallback(page, calendar_root, config):
    with time_section("summarize_vacancies(fallback)"):
        import re as _re
        patterns = config["status_patterns"]
        summary = {"○": 0, "△": 0, "×": 0, "未判定": 0}
        details: List[Dict[str, str]] = []
        def _st(raw: str) -> Optional[str]:
            return _st_from_text_and_src(raw, patterns)

        cands = calendar_root.locator(":scope tbody td, :scope [role='gridcell']")
        for i in range(cands.count()):
            el = cands.nth(i)
            try:
                txt = (el.inner_text() or "").strip()
            except Exception:
                continue
            head = txt[:40]
            m = _re.search(r"^([1-9]\d?|1\d|2\d|3[01])\s*日", head, flags=_re.MULTILINE)
            if not m:
                try:
                    aria = el.get_attribute("aria-label") or ""
                    title = el.get_attribute("title") or ""
                    m = _re.search(r"([1-9]\d?|1\d|2\d|3[01])\s*日", aria + " " + title)
                except Exception:
                    pass
            if not m:
                try:
                    imgs = el.locator("img"); jcnt = imgs.count()
                    for j in range(jcnt):
                        alt = imgs.nth(j).get_attribute("alt") or ""
                        tit = imgs.nth(j).get_attribute("title") or ""
                        mm = _re.search(r"([1-9]\d?|1\d|2\d|3[01])\s*日", alt + " " + tit)
                        if mm:
                            m = mm
                            break
                except Exception:
                    pass
            if not m:
                continue
            day = f"{m.group(0)}"
            st = _st(txt)
            if not st:
                try:
                    imgs = el.locator("img"); jcnt = imgs.count()
                    for j in range(jcnt):
                        alt = imgs.nth(j).get_attribute("alt") or ""
                        tit = imgs.nth(j).get_attribute("title") or ""
                        src = imgs.nth(j).get_attribute("src") or ""
                        st = _st(alt + " " + tit) or _st(src)
                        if st:
                            break
                except Exception:
                    pass
            if not st:
                try:
                    aria = el.get_attribute("aria-label") or ""
                    tit = el.get_attribute("title") or ""
                    cls = (el.get_attribute("class") or "").lower()
                    st = _st(aria + " " + tit)
                    if not st:
                        for kw in config["css_class_patterns"]["circle"]:
                            if kw in cls: st = "○"; break
                        if not st:
                            for kw in config["css_class_patterns"]["triangle"]:
                                if kw in cls: st = "△"; break
                        if not st:
                            for kw in config["css_class_patterns"]["cross"]:
                                if kw in cls: st = "×"; break
                except Exception:
                    pass
            if not st:
                st = "未判定"
            summary[st] += 1
            details.append({"day": day, "status": st, "text": txt})
        return summary, details

def facility_month_dir(short: str, month_text: str) -> Path:
    safe_fac = re.sub(r"[\\/:*?\"<>\n]+","_", short)
    safe_month = re.sub(r"[\\/:*?\"<>\n]+","_", month_text or "unknown_month")
    d = OUTPUT_ROOT / safe_fac / safe_month
    with time_section(f"mkdir outdir: {d}"): safe_mkdir(d)
    return d

def load_last_payload(outdir: Path) -> Optional[Dict[str, Any]]:
    p = outdir / "status_counts.json"
    if not p.exists(): return None
    try:
        return json.loads(p.read_text("utf-8"))
    except Exception:
        return None

def load_last_summary(outdir: Path):
    payload = load_last_payload(outdir)
    return (payload or {}).get("summary")

def summaries_changed(prev, cur) -> bool:
    if prev is None and cur is not None: return True
    if prev is None and cur is None: return False
    for k in ["○","△","×","未判定"]:
        if (prev or {}).get(k,0) != (cur or {}).get(k,0): return True
    return False

def dump_calendar_html(calendar_root, out_path: Path):
    with time_section(f"dump_html: {out_path.name}"):
        html = calendar_root.evaluate("el => el.outerHTML")
        safe_write_text(out_path, html)

def take_calendar_screenshot(calendar_root, out_path: Path):
    with time_section(f"screenshot: {out_path.name}"):
        safe_element_screenshot(calendar_root, out_path)

def save_calendar_assets(cal_root, outdir: Path, save_ts: bool):
    latest_html = outdir / "calendar.html"
    latest_png = outdir / "calendar.png"
    ts = _dt.now().strftime("%Y%m%d_%H%M%S")
    html_ts = outdir / f"calendar_{ts}.html"
    png_ts = outdir / f"calendar_{ts}.png"
    dump_calendar_html(cal_root, latest_html)
    take_calendar_screenshot(cal_root, latest_png)
    ts_html = ts_png = None
    if save_ts:
        dump_calendar_html(cal_root, html_ts)
        take_calendar_screenshot(cal_root, png_ts)
        ts_html, ts_png = html_ts, png_ts
    return latest_html, latest_png, ts_html, ts_png

# ====== 差分通知（日） ======
IMPROVE_TRANSITIONS = {
    ("×", "△"),
    ("△", "○"),
    ("×", "○"),
    ("未判定", "△"),
    ("未判定", "○"),
}

def _parse_month_text(month_text: str) -> Optional[Tuple[int, int]]:
    m = re.match(r"(\d{4})年(\d{1,2})月", month_text or "")
    if not m: return None
    return int(m.group(1)), int(m.group(2))

def _day_str_to_int(day_str: str) -> Optional[int]:
    m = re.search(r"([1-9]\d?|1\d|2\d|3[01])\s*日", day_str or "")
    return int(m.group(1)) if m else None

def _weekday_jp(dt: datetime.date) -> str:
    names = ["月","火","水","木","金","土","日"]
    return names[dt.weekday()]

def _is_japanese_holiday(dt: datetime.date) -> bool:
    if not INCLUDE_HOLIDAY_FLAG: return False
    if jpholiday is None: return False
    try: return jpholiday.is_holiday(dt)
    except Exception: return False

_STATUS_EMOJI = {
    "×": "✖️",
    "△": "🔼",
    "○": "⭕️",
    "未判定": "❓",
}
def _decorate_status(st: str) -> str:
    st = st or "未判定"
    return _STATUS_EMOJI.get(st, "❓")

def build_aggregate_lines(month_text: str, prev_details: List[Dict[str,str]], cur_details: List[Dict[str,str]]) -> List[str]:
    ym = _parse_month_text(month_text)
    if not ym: return []
    y, mo = ym
    prev_map: Dict[int, str] = {}
    cur_map: Dict[int, str] = {}
    for d in (prev_details or []):
        di = _day_str_to_int(d.get("day",""))
        if di is not None:
            prev_map[di] = d.get("status","未判定")
    for d in (cur_details or []):
        di = _day_str_to_int(d.get("day",""))
        if di is not None:
            cur_map[di] = d.get("status","未判定")
    lines: List[str] = []
    for di, cur_st in sorted(cur_map.items()):
        prev_st = prev_map.get(di)
        if prev_st is None:
            continue
        if (prev_st, cur_st) in IMPROVE_TRANSITIONS:
            dt = datetime.date(y, mo, di)
            wd = _weekday_jp(dt)
            wd_part = f"{wd}・祝" if _is_japanese_holiday(dt) else wd
            prev_fmt = _decorate_status(prev_st)
            cur_fmt = _decorate_status(cur_st)
            line = f"{y}年{mo}月{di}日（{wd_part}）: {prev_fmt} → {cur_fmt}"
            lines.append(line)
    return lines

# ====== Discord クライアント ======
DISCORD_CONTENT_LIMIT = 2000
DISCORD_EMBED_DESC_LIMIT = 4096

def _split_content(s: str, limit: int = DISCORD_CONTENT_LIMIT) -> List[str]:
    out: List[str] = []
    cur = (s or "").strip()
    while len(cur) > limit:
        cut = cur.rfind("\n", 0, limit)
        if cut < 0: cut = cur.rfind(" ", 0, limit)
        if cut < 0: cut = limit
        out.append(cur[:cut].rstrip())
        cur = cur[cut:].lstrip()
    if cur:
        out.append(cur)
    return out

def _truncate_embed_description(desc: str) -> str:
    if desc is None: return ""
    if len(desc) <= DISCORD_EMBED_DESC_LIMIT: return desc
    return desc[:DISCORD_EMBED_DESC_LIMIT - 3] + "..."

def _build_mention_and_allowed() -> Tuple[str, Dict[str, Any]]:
    mention = ""
    allowed: Dict[str, Any] = {}
    uid = os.getenv("DISCORD_MENTION_USER_ID", "").strip()
    use_everyone = os.getenv("DISCORD_USE_EVERYONE", "0").strip() == "1"
    use_here = os.getenv("DISCORD_USE_HERE", "0").strip() == "1"
    if uid:
        mention = f"<@{uid}>"
        allowed = {"allowed_mentions": {"parse": [], "users": [uid]}}
    elif use_everyone:
        mention = "@everyone"
        allowed = {"allowed_mentions": {"parse": ["everyone"]}}
    elif use_here:
        mention = "@here"
        allowed = {"allowed_mentions": {"parse": []}}
    else:
        allowed = {"allowed_mentions": {"parse": []}}
    return mention, allowed

class DiscordWebhookClient:
    def __init__(self, webhook_url: str, thread_id: Optional[str] = None, wait: bool = True, user_agent: Optional[str] = None, timeout_sec: int = 10):
        if not webhook_url:
            raise ValueError("webhook_url is required")
        self.webhook_url = webhook_url
        self.thread_id = thread_id
        self.wait = wait
        self.timeout_sec = timeout_sec
        self.user_agent = user_agent or "facility-monitor/1.0 (+python-urllib)"

    @staticmethod
    def from_env() -> "DiscordWebhookClient":
        url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
        th = os.getenv("DISCORD_THREAD_ID", "").strip() or None
        wt = os.getenv("DISCORD_WAIT", "1").strip() == "1"
        ua = os.getenv("DISCORD_USER_AGENT", "").strip() or None
        return DiscordWebhookClient(webhook_url=url, thread_id=th, wait=wt, user_agent=ua)

    def _post(self, payload: Dict[str, Any]) -> Tuple[int, str, Dict[str, Any]]:
        import urllib.request, urllib.error, ssl
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        url = self.webhook_url
        params = []
        if self.wait: params.append("wait=true")
        if self.thread_id: params.append(f"thread_id={self.thread_id}")
        if params: url = f"{url}?{'&'.join(params)}"
        req = urllib.request.Request(url=url, data=data, headers={"Content-Type": "application/json", "User-Agent": self.user_agent})
        ctx = ssl.create_default_context()
        tries = 0
        max_tries = 3
        while True:
            tries += 1
            try:
                with urllib.request.urlopen(req, context=ctx, timeout=self.timeout_sec) as resp:
                    body = resp.read().decode("utf-8", errors="ignore")
                    status = getattr(resp, "status", 200)
                    headers = dict(resp.headers) if resp.headers else {}
                    return status, body, headers
            except urllib.error.HTTPError as e:
                status = e.code
                try:
                    body = e.read().decode("utf-8", errors="ignore")
                except Exception:
                    body = ""
                headers = dict(e.headers) if e.headers else {}
                if status == 429 and tries < max_tries:
                    retry_after = float(headers.get("Retry-After", "1.0"))
                    print(f"[WARN] Discord 429: retry_after={retry_after}s; body={body}", flush=True)
                    time.sleep(max(0.5, retry_after))
                    continue
                return status, body, headers
            except Exception as e:
                return -1, f"Exception: {e}", {}

    def send_embed(self, title: str, description: str, color: int = 0x00B894, footer_text: str = "Facility monitor") -> bool:
        mention, allowed = _build_mention_and_allowed()
        one_line = (description or "").splitlines()[0] if description else ""
        content = f"{mention} **{title}** — {one_line}".strip() if (mention or one_line or title) else ""
        embed = {
            "title": title,
            "description": _truncate_embed_description(description or ""),
            "color": color,
            "timestamp": jst_now().isoformat(),
            "footer": {"text": footer_text},
        }
        payload = {"content": content, "embeds": [embed], **allowed}
        status, body, headers = self._post(payload)
        if status in (200, 204):
            print(f"[INFO] Discord notified (embed): title='{title}' len={len(description or '')} body={body}", flush=True)
            return True
        print(f"[WARN] Embed failed: HTTP {status}; body={body}. Falling back to plain text.", flush=True)
        text = f"**{title}**\n{description or ''}"
        return self.send_text(text)

    def send_text(self, content: str) -> bool:
        mention, allowed = _build_mention_and_allowed()
        pages = _split_content(content or "", limit=DISCORD_CONTENT_LIMIT)
        ok_all = True
        for i, page in enumerate(pages, 1):
            page_with_mention = f"{mention} {page}".strip() if mention else page
            payload = {"content": page_with_mention, **allowed}
            status, body, headers = self._post(payload)
            if status in (200, 204):
                print(f"[INFO] Discord notified (text p{i}/{len(pages)}): {len(page_with_mention)} chars body={body}", flush=True)
            else:
                ok_all = False
                print(f"[ERROR] Discord text failed (p{i}/{len(pages)}): HTTP {status} body={body}", flush=True)
        return ok_all

# 施設ごとの色
_FACILITY_ALIAS_COLOR_HEX = {
    "南浦和": "0x3498DB",  # Blue
    "岩槻":   "0x2ECC71",  # Green
    "鈴谷":   "0xF1C40F",  # Yellow
    "岸町":   "0xE74C3C",  # Red
    "駒場":   "0x8E44AD",  # Purple-ish
}
_DEFAULT_COLOR_HEX = "0x00B894"
def _hex_to_int(hex_str: str) -> int:
    try:
        return int(hex_str, 16)
    except Exception:
        return int(_DEFAULT_COLOR_HEX, 16)

def send_aggregate_lines(webhook_url: Optional[str], facility_alias: str, month_text: str, lines: List[str]) -> None:
    if not webhook_url or not lines:
        return
    force_text = (os.getenv("DISCORD_FORCE_TEXT", "0").strip() == "1")
    max_lines_env = os.getenv("DISCORD_MAX_LINES", "").strip()
    max_lines = None
    try:
        if max_lines_env:
            max_lines = max(1, int(max_lines_env))
    except Exception:
        max_lines = None
    if max_lines is not None and len(lines) > max_lines:
        lines = lines[:max_lines] + [f"... ほか {len(lines) - max_lines} 件"]

    title = f"{facility_alias}"
    description = "\n".join(lines)
    color_hex = _FACILITY_ALIAS_COLOR_HEX.get(facility_alias, _DEFAULT_COLOR_HEX)
    color_int = _hex_to_int(color_hex)
    client = DiscordWebhookClient.from_env()
    client.webhook_url = webhook_url
    if force_text:
        content = f"**{title}**\n{description}"
        client.send_text(content)
        return
    client.send_embed(title=title, description=description, color=color_int, footer_text="Facility monitor")

# ====== スナップショット世代ローテーション ======
def rotate_snapshot_files(outdir: Path, max_png: int = 50, max_html: int = 50) -> None:
    try:
        png_ts = sorted([p for p in outdir.glob("calendar_*.png") if p.is_file()], key=lambda p: p.stat().st_mtime)
        if len(png_ts) > max_png:
            for p in png_ts[: len(png_ts) - max_png]:
                try: p.unlink()
                except Exception: pass
        html_ts = sorted([p for p in outdir.glob("calendar_*.html") if p.is_file()], key=lambda p: p.stat().st_mtime)
        if len(html_ts) > max_html:
            for p in html_ts[: len(html_ts) - max_html]:
                try: p.unlink()
                except Exception: pass
    except Exception as e:
        print(f"[WARN] rotate_snapshot_files failed: {e}", flush=True)

# ====== 週表示への遷移支援（全施設：月表示→日クリック→週表示） ======
def ensure_month_view(page) -> bool:
    """
    週表示（prwca1000等）に居る場合に「一ヶ月検索結果」（prwmn1000）へ切替。
    画面内に月表示の目印となる table.m_akitablelist が現れるまで誘導する。
    """
    try:
        if page.locator("table.m_akitablelist").count() > 0:
            return True
        candidates = [
            "a:has-text('一ヶ月検索結果')",
            "a:has-text('一ヶ月')",
            "img[alt='一ヶ月検索結果']",
            "a[href*='rsvWInstSrchMonthVacantAction']",
            "a[href*='moveCalender']",
        ]
        for sel in candidates:
            try:
                el = page.locator(sel).first
                if el.count() > 0:
                    el.scroll_into_view_if_needed()
                    el.click(timeout=2000)
                    break
            except Exception:
                pass
        page.wait_for_timeout(300)
        return page.locator("table.m_akitablelist").count() > 0
    except Exception:
        return False

def open_day_detail(page, calendar_root, y: int, m: int, d: int, facility: Dict[str, Any]) -> bool:
    """月カレンダー上の対象日セルをクリックして詳細（週表示）へ。
    - <td> 直クリックではなく、セル内の <a> または <img> を優先してクリック
    - クリック後、URLが変わる or 週表示テーブルが出る まで確認し、失敗なら False
    """
    try:
        before = page.url
        pat = re.compile(rf"\b{d}\s*日\b")
        cells = calendar_root.locator(":scope tbody td, :scope [role='gridcell']")
        target = None
        for i in range(cells.count()):
            el = cells.nth(i)
            txt = (el.inner_text() or "")
            lab = (el.get_attribute("aria-label") or "") + " " + (el.get_attribute("title") or "")
            if pat.search(txt) or pat.search(lab):
                target = el
                break
        if not target:
            _log(f"open_day_detail: target day '{d}日' cell not found")
            return False

        # 優先クリック順: <a> → <img> → <td>
        clicked = False
        try:
            alink = target.locator("a").first
            if alink.count() > 0:
                alink.scroll_into_view_if_needed()
                alink.click(timeout=2000)
                clicked = True
        except Exception:
            pass
        if not clicked:
            try:
                img = target.locator("a img, img").first
                if img.count() > 0:
                    img.scroll_into_view_if_needed()
                    img.click(timeout=2000)
                    clicked = True
            except Exception:
                pass
        if not clicked:
            target.scroll_into_view_if_needed()
            target.click(timeout=2000)

        # 遷移検証：URL変化 or 週表示テーブル出現
        dv = (load_config().get("detail_view") or {})
        hint = (dv.get("common") or {}).get("week_table_selector", "table.akitablelist")
        page.wait_for_timeout(300)  # 小休止
        after = page.url
        week_ok = (page.locator(hint).count() > 0)
        _log(f"open_day_detail: clicked; url before={before} after={after} week_ok={week_ok}")
        if (after != before) or week_ok:
            return True
        return False
    except Exception as e:
        _log(f"open_day_detail exception: {e}")
        return False

def open_day_detail_from_month(page, y: int, m: int, d: int, facility: Dict[str, Any]) -> bool:
    """月表示テーブルから selectDay(..., y, m, d) を直接クリックして週表示へ"""
    try:
        dv = (load_config().get("detail_view") or {})
        fac_cfg = (dv.get(facility.get("name", "")) or {})
        monthly = (fac_cfg.get("monthly") or {})
        cal_sel = monthly.get("calendar_selector", "table.m_akitablelist")
        link_contains = monthly.get("day_link_contains", "selectDay(")
        table = page.locator(cal_sel).first
        if table.count() == 0:
            return False
        links = table.locator("a").all()
        target = None
        for a in links:
            href = a.get_attribute("href") or ""
            if (link_contains in href) and (f",{y}, {m}, {d}" in href or f",{y},{m},{d}" in href):
                target = a
                break
        if not target:
            return False
        before = page.url
        target.scroll_into_view_if_needed()
        target.click(timeout=2000)
        hint = (dv.get("common") or {}).get("week_table_selector", "table.akitablelist")
        wait_next_step_ready(page, css_hint=hint)
        after = page.url
        week_ok = (page.locator(hint).count() > 0)
        _log(f"open_day_detail_from_month: url before={before} after={after} week_ok={week_ok}")
        return week_ok or (after != before)
    except Exception as e:
        _log(f"open_day_detail_from_month exception: {e}")
        return False

def _status_from_img(img_el, common_cfg) -> Optional[str]:
    """セル内<img> の alt/src から ○/×/未判定 を返す"""
    try:
        alt = (img_el.get_attribute("alt") or "").strip()
        src = (img_el.get_attribute("src") or "").strip().lower()
    except Exception:
        alt, src = "", ""
    avail_alts = [s.lower() for s in (common_cfg.get("legend_available_alts") or [])]
    full_alts  = [s.lower() for s in (common_cfg.get("legend_full_alts") or [])]
    avail_srcs = [s.lower() for s in (common_cfg.get("legend_available_src_contains") or [])]
    full_srcs  = [s.lower() for s in (common_cfg.get("legend_full_src_contains") or [])]
    nalt = alt.lower()
    if any(a in nalt for a in avail_alts): return "○"
    if any(a in nalt for a in full_alts):  return "×"
    if any(k in src for k in avail_srcs):  return "○"
    if any(k in src for k in full_srcs):   return "×"
    return "未判定"

def parse_day_timebands(page, facility_name: str, y: int, m: int, d: int) -> List[Dict[str, str]]:
    """
    週表示テーブル（table.akitablelist）から対象日列の「時間帯ステータス」一覧を取得
    返り値: [{"label": 行ラベル, "range": "HH:MM-HH:MM", "status": "○|×|未判定"}, ...]
    """
    cfg = load_config()
    dv  = (cfg.get("detail_view") or {})
    common = (dv.get("common") or {})
    fac_cfg = (dv.get(facility_name) or {})
    tb_map  = (fac_cfg.get("timeband_map") or {})

    tbl_sel = common.get("week_table_selector", "table.akitablelist")
    table = page.locator(tbl_sel).first
    if table.count() == 0:
        return []

    # 列インデックス（「m月d日」を含むヘッダ）
    headers = page.locator(common.get("date_header_selector"))
    col = -1
    target_txt = f"{m}月{d}日"
    for i in range(headers.count()):
        txt = (headers.nth(i).inner_text() or "").replace("\n", "")
        if target_txt in txt:
            col = i
            break
    if col < 0:
        return []

    rows_labels = page.locator(common.get("row_label_selector"))
    out: List[Dict[str, str]] = []
    for i in range(rows_labels.count()):
        label_raw = (rows_labels.nth(i).inner_text() or "").strip()
        label_key = label_raw.replace("　", "").replace(" ", "")
        ranges = tb_map.get(label_raw) or tb_map.get(label_key) or []
        row = table.locator("tbody > tr").nth(i + 2)  # 1行目=年, 2行目=日付, 3行目以降=時間帯
        td  = row.locator("td.akitablelist").nth(col)
        img = td.locator("img").first
        st  = _status_from_img(img, common)
        out.extend({"label": label_raw, "range": r, "status": st} for r in ranges)
    return out

# ====== 時間帯スナップショットと通知 ======
def _day_dir(outdir_month: Path, day_int: int) -> Path:
    d = outdir_month / f"{day_int:02d}"
    d.mkdir(parents=True, exist_ok=True)
    return d

def load_day_slots(outdir_month: Path, day_int: int):
    p = _day_dir(outdir_month, day_int) / "time_slots.json"
    if not p.exists(): return None
    try: return json.loads(p.read_text("utf-8"))
    except Exception: return None

def save_day_slots(outdir_month: Path, day_int: int, slots: List[Dict[str, str]]):
    p = _day_dir(outdir_month, day_int) / "time_slots.json"
    safe_write_text(p, json.dumps({"slots": slots, "saved_at": jst_now().strftime("%Y-%m-%d %H:%M:%S")}, ensure_ascii=False, indent=2))

def _fmt_range(rng: str) -> str:
    try:
        s, e = rng.split("-")
        sh, _ = s.split(":"); eh, _ = e.split(":")
        return f"{int(sh)}時～{int(eh)}時"
    except Exception:
        return rng

def build_time_slot_improvement_lines(fac_alias: str, y: int, m: int, day_int: int, prev_slots, cur_slots) -> List[str]:
    prev_map = {(s["range"]): s.get("status", "未判定") for s in (prev_slots or [])}
    cur_map  = {(s["range"]): s.get("status", "未判定") for s in (cur_slots or [])}
    lines: List[str] = []
    wd = _weekday_jp(datetime.date(y, m, day_int))
    for rng, cur_st in cur_map.items():
        prev_st = prev_map.get(rng)
        if prev_st is None:
            if cur_st in ("○", "△"):
                lines.append(f"{y}年{m}月{day_int}日（{wd}）{_fmt_range(rng)}")
        else:
            if (prev_st, cur_st) in {("×", "△"), ("△", "○"), ("×", "○")}:
                lines.append(f"{y}年{m}月{day_int}日（{wd}）{_fmt_range(rng)}")
    return lines

# ====== メイン処理 ======
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

    # retention 設定（全体）
    cfg_ret = (config.get("retention") or {})
    max_png_default = int(cfg_ret.get("max_files_per_month_png", 50))
    max_html_default = int(cfg_ret.get("max_files_per_month_html", 50))

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
                short = FACILITY_TITLE_ALIAS.get(facility.get('name',''), facility.get('name','')) or facility.get('name','')
                outdir = facility_month_dir(short or 'unknown_facility', month_text)
                print(f"[INFO] outdir={outdir}", flush=True)

                # --- 当月集計 ---
                summary, details = summarize_vacancies(page, cal_root, config)
                prev_payload = load_last_payload(outdir)
                prev_summary = (prev_payload or {}).get("summary")
                prev_details = (prev_payload or {}).get("details") or []
                changed = summaries_changed(prev_summary, summary)
                latest_html, latest_png, ts_html, ts_png = save_calendar_assets(cal_root, outdir, save_ts=changed)

                fac_ret = facility.get("retention") or {}
                max_png = int(fac_ret.get("max_files_per_month_png", max_png_default))
                max_html = int(fac_ret.get("max_files_per_month_html", max_html_default))
                rotate_snapshot_files(outdir, max_png=max_png, max_html=max_html)

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

                # --- 差分通知（日） ---
                lines = build_aggregate_lines(month_text, prev_details, details)
                if lines:
                    send_aggregate_lines(DISCORD_WEBHOOK_URL, short, month_text, lines)

                # --- 時間帯通知（当月：詳細ログ付き / 全施設：月表示→週表示） ---
                ym = _parse_month_text(month_text)
                if ym:
                    y, mo = ym
                    improved_days: List[int] = []
                    prev_map = {}
                    for pd in (prev_details or []):
                        di = _day_str_to_int(pd.get("day",""))
                        if di is not None:
                            prev_map[di] = pd.get("status","未判定")
                    for d in (details or []):
                        di = _day_str_to_int(d.get("day",""))
                        if di is None:
                            continue
                        prev_st = prev_map.get(di)
                        cur_st  = d.get("status","未判定")
                        if (prev_st, cur_st) in IMPROVE_TRANSITIONS:
                            improved_days.append(di)

                    _log(f"improved_days={improved_days}")

                    dv = (load_config().get("detail_view") or {})
                    fac_cfg = (dv.get(facility.get("name", "")) or {})
                    has_monthly = True  # 全施設が月表示から遷移する前提

                    for di in improved_days:
                        step_dir = outdir / f"_{y}{mo:02d}{di:02d}"
                        step_dir.mkdir(parents=True, exist_ok=True)
                        _log(f"=== day {y}/{mo}/{di} ===")

                        opened = False
                        before_url = page.url

                        # 1) 必ず月表示へ切り替え → selectDay(...) or 日セル内リンククリック
                        _log("ensure month-view ...")
                        ok_mv = ensure_month_view(page)
                        _log(f"ensure month-view => {ok_mv}")
                        if not ok_mv:
                            try:
                                _log("navigate_to_facility() to re-enter flow")
                                navigate_to_facility(page, facility)
                                ok_mv = ensure_month_view(page)
                                _log(f"retry ensure month-view => {ok_mv}")
                            except Exception as e:
                                _log(f"month-view retry failed: {e}")
                                ok_mv = False

                        if ok_mv:
                            _log("try selectDay(...) first")
                            opened = open_day_detail_from_month(page, y, mo, di, facility)
                            _log(f"open_day_detail_from_month => {opened}")

                        # 2) フォールバック：月グリッドの該当日セル内リンクをクリック
                        if not opened and ok_mv:
                            try:
                                cal_tbl = page.locator("table.m_akitablelist").first
                                opened = open_day_detail(page, cal_tbl, y, mo, di, facility)
                                _log(f"open_day_detail (fallback) => {opened}")
                            except Exception as e:
                                _log(f"open_day_detail fallback exception: {e}")
                                opened = False

                        after_url = page.url
                        _log(f"url before={before_url}")
                        _log(f"url after ={after_url}")

                        # 3) 週表示テーブルの出現確認
                        week_ok = (page.locator("table.akitablelist").count() > 0)
                        _log(f"week table present? => {week_ok}")
                        if not opened or not week_ok:
                            _log("OPEN or WEEK-VIEW FAILED; capturing evidences ...")
                            _screenshot(page, step_dir / "failed_open.png")
                            _dump_week_html(page, step_dir / "failed_open.html")
                            continue

                        # 4) 対象日ヘッダのサンプル
                        try:
                            headers = page.locator((dv.get("common") or {}).get("date_header_selector"))
                            hdr_cnt = headers.count()
                            hdr_snippet = []
                            for i in range(min(hdr_cnt, 8)):
                                t = (headers.nth(i).inner_text() or "").strip().replace("\n","")
                                hdr_snippet.append(t)
                            _log(f"date headers sample={hdr_snippet}")
                        except Exception as e:
                            _log(f"header read failed: {e}")

                        # 5) 時間帯抽出
                        _log("parse_day_timebands ...")
                        slots = parse_day_timebands(page, facility.get("name",""), y, mo, di)
                        _log(f"slots parsed: {len(slots)} rows")
                        for i, s in enumerate(slots[:20], 1):
                            _log(f"  [{i:02d}] label='{s['label']}' range={s['range']} status={s['status']}")

                        # 6) 週表示のHTMLとスクショ保存
                        _dump_week_html(page, step_dir / "weekview.html")
                        try:
                            week_el = page.locator("table.akitablelist").first
                            week_el.screenshot(path=str(step_dir / "weekview.png"))
                            _log("weekview screenshot saved")
                        except Exception as e:
                            _log(f"weekview screenshot failed: {e}")
                            _screenshot(page, step_dir / "weekview_full.png")

                        # 7) 差分判定 & 通知
                        prev_slots_payload = load_day_slots(outdir, di)
                        prev_slots = (prev_slots_payload or {}).get("slots") if prev_slots_payload else []
                        save_day_slots(outdir, di, slots)

                        cnt = {"○":0, "△":0, "×":0, "未判定":0}
                        for s in slots: cnt[s["status"]] = cnt.get(s["status"],0) + 1
                        _log(f"slot stats: ○={cnt['○']} △={cnt['△']} ×={cnt['×']} 未判定={cnt['未判定']}")

                        lines_ts = build_time_slot_improvement_lines(short, y, mo, di, prev_slots, slots)
                        _log(f"notif lines: {len(lines_ts)}")
                        if lines_ts:
                            send_aggregate_lines(DISCORD_WEBHOOK_URL, short, month_text, lines_ts)

                        # 8) 次の改善日に備え、施設トップ→当月へ戻す（安定化）
                        try:
                            navigate_to_facility(page, facility)
                            month_text = get_current_year_month_text(page) or month_text
                            cal_root = locate_calendar_root(page, month_text or "予約カレンダー", facility)
                        except Exception as e:
                            _log(f"return to month failed: {e}")

                # --- 月移動ループ ---
                shifts = facility.get("month_shifts", [0,1])
                shifts = sorted(set(int(s) for s in shifts if isinstance(s,(int,float))))
                if 0 not in shifts: shifts.insert(0,0)
                max_shift = max(shifts); prev_month_text = month_text

                for step in range(1, max_shift + 1):
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
                        prev_payload2 = load_last_payload(outdir2)
                        prev_summary2 = (prev_payload2 or {}).get("summary")
                        prev_details2 = (prev_payload2 or {}).get("details") or []
                        changed2 = summaries_changed(prev_summary2, summary2)
                        latest_html2, latest_png2, ts_html2, ts_png2 = save_calendar_assets(cal_root2, outdir2, save_ts=changed2)

                        rotate_snapshot_files(outdir2, max_png=max_png, max_html=max_html)

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

                        lines2 = build_aggregate_lines(month_text2, prev_details2, details2)
                        if lines2:
                            send_aggregate_lines(DISCORD_WEBHOOK_URL, short, month_text2, lines2)

                    cal_root = cal_root2
                    prev_month_text = month_text2

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
        if not within:
            print("[INFO] outside monitoring window. exit.", flush=True)
            sys.exit(0)
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
    print("[INFO] monitor.py finished.")
