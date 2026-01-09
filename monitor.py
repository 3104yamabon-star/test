
# -*- coding: utf-8 -*-
"""
さいたま市 施設予約システムの空き状況監視（計測＋任意ダイアログ高速版）
改良点:
- 任意ダイアログ（同意/OK/確認/閉じる）の超軽量処理
- 既定タイムアウト短縮（page.set_default_timeout(5000)）
- 月遷移の成功判定を「月テキスト変化」のみで高速化
- ①アダプティブ待機（初期200ms＋セル数検知、上限GRACE_MS既定1000ms）
- ② summarize_vacancies を HTML一括パース（Playwright DOMアクセス最小化）
- ★ 差分検知（セル単位）＋ Discord通知（集約タイプ）
    - Embed成功時は施設別カラー、失敗時はテキストへ自動フォールバック
    - content 2000字分割、429リトライ、詳細レスポンス本文ログ

通知仕様（改善判定）:
改善遷移 = ×→△, △→○, ×→○, 未判定→△, 未判定→○
通知タイトル = 施設略語 + 月（例: 南浦和 2026年2月）
通知本文 = YYYY年M月D日 (曜・祝) : 前回 → 今回（ゼロパディングなし）
※ ステータス表示は「絵文字のみ」（太字なし）
"""

import os
import sys
import json
import re
import datetime
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List
from playwright.sync_api import sync_playwright

# ====== 環境 ======
try:
    import pytz
except Exception:
    pytz = None
# 祝日判定（任意導入）
try:
    import jpholiday  # pip install jpholiday が必要
except Exception:
    jpholiday = None

BASE_URL = os.getenv("BASE_URL")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
MONITOR_FORCE = os.getenv("MONITOR_FORCE", "0").strip() == "1"
MONITOR_START_HOUR = int(os.getenv("MONITOR_START_HOUR", "5"))
MONITOR_END_HOUR = int(os.getenv("MONITOR_END_HOUR", "23"))
TIMING_VERBOSE = os.getenv("TIMING_VERBOSE", "0").strip() == "1"
# 安定化猶予上限（ミリ秒）：既定 1000ms（環境変数 GRACE_MS で変更可）
GRACE_MS_DEFAULT = 1000
try:
    GRACE_MS = max(0, int(os.getenv("GRACE_MS", str(GRACE_MS_DEFAULT))))
except Exception:
    GRACE_MS = GRACE_MS_DEFAULT
# 祝日表示のON/OFF（祝日名は出さない仕様）
INCLUDE_HOLIDAY_FLAG = os.getenv("DISCORD_INCLUDE_HOLIDAY", "1").strip() == "1"

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

# ====== 画面安定化用ヘルパー（① アダプティブ待機・上限厳守） ======
def grace_pause(page, label: str = "grace wait"):
    """
    アダプティブ安定化待機（上限厳守版）:
    - 初期 200ms
    - 以降 200ms 間隔で calendar のセル数 >=28 を検知したら即抜け
    - 上限は GRACE_MS（既定 1000ms）を厳密に超えない
    """
    ms_cap = GRACE_MS if isinstance(GRACE_MS, int) else GRACE_MS_DEFAULT
    if ms_cap <= 0:
        return
    with time_section(f"{label} (adaptive, <= {ms_cap}ms)"):
        step = 200
        spent = 0
        page.wait_for_timeout(step); spent += step
        try:
            while spent < ms_cap:
                cells = page.locator("[role='gridcell'], table.reservation-calendar tbody td, .fc-daygrid-day, .calendar-day")
                if cells.count() >= 28:
                    break
                remaining = ms_cap - spent
                wait_ms = step if remaining >= step else remaining
                if wait_ms <= 0:
                    break
                page.wait_for_timeout(wait_ms)
                spent += wait_ms
        except Exception:
            pass

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

OPTIONAL_DIALOG_LABELS = ["同意する", "OK", "確認", "閉じる"]

def click_optional_dialogs_fast(page) -> None:
    for label in OPTIONAL_DIALOG_LABELS:
        with time_section(f"optional-dialog: '{label}'"):
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
                        clicked = True
                except Exception:
                    pass

def navigate_to_facility(page, facility: Dict[str, Any]) -> None:
    if not BASE_URL:
        raise RuntimeError("BASE_URL が未設定です。Secrets の BASE_URL に https://saitama.rsv.ws-scs.jp/web/ を設定してください。")
    with time_section("goto BASE_URL"):
        page.goto(BASE_URL, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_load_state("domcontentloaded", timeout=30000)
        page.set_default_timeout(5000)
        click_optional_dialogs_fast(page)
    for label in facility.get("click_sequence", []):
        with time_section(f"click_sequence: '{label}'"):
            ok = try_click_text(page, label, timeout_ms=5000)
            if not ok:
                raise RuntimeError(f"クリック対象が見つかりません：『{label}』（施設: {facility.get('name','')})")
            grace_pause(page, label="after availability view shown")

def get_current_year_month_text(page, calendar_root=None) -> Optional[str]:
    pat = re.compile(r"(\d{4})\s*年\s*(\d{1,2})\s*月")
    targets: List[str] = []
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

    grace_pause(page, label="after month transition")
    return True

# ====== 集計 / 保存（② HTML一括パース） ======
from datetime import datetime as _dt

def _st_from_text_and_src(raw: str, patterns: Dict[str, List[str]]) -> Optional[str]:
    if raw is None:
        return None
    txt = raw.strip()
    n = txt.replace("　", " ").lower()
    for ch in ["○", "〇", "△", "×"]:
        if ch in txt:
            return {"〇": "○"}.get(ch, ch)
    for kw in patterns["circle"]:
        if kw.lower() in n:
            return "○"
    for kw in patterns["triangle"]:
        if kw.lower() in n:
            return "△"
    for kw in patterns["cross"]:
        if kw.lower() in n:
            return "×"
    return None

def _status_from_class(cls: str, css_class_patterns: Dict[str, List[str]]) -> Optional[str]:
    if not cls:
        return None
    c = cls.lower()
    for kw in css_class_patterns["circle"]:
        if kw in c:
            return "○"
    for kw in css_class_patterns["triangle"]:
        if kw in c:
            return "△"
    for kw in css_class_patterns["cross"]:
        if kw in c:
            return "×"
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
    m = re.search(r"([1-9]|1\d|2\d|3[01])\s*日", text)
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
            m = _re.search(r"^([1-9]|1\d|2\d|3[01])\s*日", head, flags=_re.MULTILINE)
            if not m:
                try:
                    aria = el.get_attribute("aria-label") or ""
                    title = el.get_attribute("title") or ""
                    m = _re.search(r"([1-9]|1\d|2\d|3[01])\s*日", aria + " " + title)
                except Exception:
                    pass
            if not m:
                try:
                    imgs = el.locator("img"); jcnt = imgs.count()
                    for j in range(jcnt):
                        alt = imgs.nth(j).get_attribute("alt") or ""
                        tit = imgs.nth(j).get_attribute("title") or ""
                        mm = _re.search(r"([1-9]|1\d|2\d|3[01])\s*日", alt + " " + tit)
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
            details.append({"day": day, "status": st, "text": txt})

        return summary, details

def facility_month_dir(short: str, month_text: str) -> Path:
    safe_fac = re.sub(r"[\\/:*?\"<>|]+","_", short)
    safe_month = re.sub(r"[\\/:*?\"<>|]+","_", month_text or "unknown_month")
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

# ====== 差分（セル単位の改善）＋ 祝日表示ユーティリティ ======
IMPROVE_TRANSITIONS = {
    ("×", "△"), ("△", "○"), ("×", "○"), ("未判定", "△"), ("未判定", "○")
}

def _parse_month_text(month_text: str) -> Optional[Tuple[int, int]]:
    m = re.match(r"(\d{4})年(\d{1,2})月", month_text or "")
    if not m: return None
    return int(m.group(1)), int(m.group(2))

def _day_str_to_int(day_str: str) -> Optional[int]:
    m = re.search(r"([1-9]|1\d|2\d|3[01])\s*日", day_str or "")
    return int(m.group(1)) if m else None

def _weekday_jp(dt: datetime.date) -> str:
    names = ["月","火","水","木","金","土","日"]
    return names[dt.weekday()]

def _is_japanese_holiday(dt: datetime.date) -> bool:
    if not INCLUDE_HOLIDAY_FLAG:
        return False
    if jpholiday is None:
        return False
    try:
        return jpholiday.is_holiday(dt)
    except Exception:
        return False

# --- 装飾用（絵文字のみ） ---
_STATUS_EMOJI = {
    "×": "✖️",
    "△": "🔼",
    "○": "⭕️",
    "未判定": "❓",
}
def _decorate_status(st: str) -> str:
    """ステータスを絵文字のみで整形（例：🔼）。"""
    st = st or "未判定"
    return _STATUS_EMOJI.get(st, "❓")

def build_aggregate_lines(month_text: str, prev_details: List[Dict[str,str]], cur_details: List[Dict[str,str]]) -> List[str]:
    """
    改善セルを抽出し、'YYYY年M月D日 (曜[・祝]) : <prev> → <cur>' の行を返す（昇順）。
    - <prev>/<cur> は 絵文字のみ（太字なし）
    """
    ym = _parse_month_text(month_text)
    if not ym: return []
    y, mo = ym

    # 前回/今回の day→status マップ
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
            # 前回が無いセルは通知対象外（初回/新規など）
            continue
        if (prev_st, cur_st) in IMPROVE_TRANSITIONS:
            dt = datetime.date(y, mo, di)
            wd = _weekday_jp(dt)
            wd_part = f"{wd}・祝" if _is_japanese_holiday(dt) else wd
            prev_fmt = _decorate_status(prev_st)
            cur_fmt  = _decorate_status(cur_st)
            line = f"{y}年{mo}月{di}日 ({wd_part}) : {prev_fmt} → {cur_fmt}"
            lines.append(line)
    return lines

# ======================================================================
# Discord通知（Embed→失敗時テキストフォールバック／長文分割／429リトライ／詳細本文ログ）
# ======================================================================

DISCORD_CONTENT_LIMIT = 2000          # content の最大文字数
DISCORD_EMBED_DESC_LIMIT = 4096       # embed.description 上限

def _split_content(s: str, limit: int = DISCORD_CONTENT_LIMIT) -> List[str]:
    """Discordのcontent上限に合わせて安全に分割。改行優先・語切れ緩和。"""
    out: List[str] = []
    cur = (s or "").strip()
    while len(cur) > limit:
        cut = cur.rfind("\n", 0, limit)
        if cut < 0:
            cut = cur.rfind(" ", 0, limit)
        if cut < 0:
            cut = limit
        out.append(cur[:cut].rstrip())
        cur = cur[cut:].lstrip()
    if cur:
        out.append(cur)
    return out

def _truncate_embed_description(desc: str) -> str:
    if desc is None:
        return ""
    if len(desc) <= DISCORD_EMBED_DESC_LIMIT:
        return desc
    return desc[:DISCORD_EMBED_DESC_LIMIT - 3] + "..."

class DiscordWebhookClient:
    def __init__(self, webhook_url: str,
                 thread_id: Optional[str] = None,
                 wait: bool = True,
                 user_agent: Optional[str] = None,
                 timeout_sec: int = 10):
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

        # URL クエリの組み立て
        url = self.webhook_url
        params = []
        if self.wait:
            params.append("wait=true")
        if self.thread_id:
            params.append(f"thread_id={self.thread_id}")
        if params:
            url = f"{url}?{'&'.join(params)}"

        req = urllib.request.Request(url=url, data=data,
                                     headers={"Content-Type": "application/json",
                                              "User-Agent": self.user_agent})
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

    def send_embed(self, title: str, description: str,
                   color: int = 0x00B894,
                   footer_text: str = "Facility monitor") -> bool:
        embed = {
            "title": title,
            "description": _truncate_embed_description(description or ""),
            "color": color,
            "timestamp": jst_now().isoformat(),
            "footer": {"text": footer_text},
        }
        payload = {"embeds": [embed]}
        status, body, headers = self._post(payload)
        if status in (200, 204):
            print(f"[INFO] Discord notified (embed): title='{title}' len={len(description or '')} body={body}", flush=True)
            return True

        # 403 等 → テキストフォールバック
        print(f"[WARN] Embed failed: HTTP {status}; body={body}. Falling back to plain text.", flush=True)
        text = f"**{title}**\n{description or ''}"
        return self.send_text(text)

    def send_text(self, content: str) -> bool:
        pages = _split_content(content or "", limit=DISCORD_CONTENT_LIMIT)
        ok_all = True
        for i, page in enumerate(pages, 1):
            payload = {"content": page}
            status, body, headers = self._post(payload)
            if status in (200, 204):
                print(f"[INFO] Discord notified (text p{i}/{len(pages)}): {len(page)} chars body={body}", flush=True)
            else:
                ok_all = False
                print(f"[ERROR] Discord text failed (p{i}/{len(pages)}): HTTP {status} body={body}", flush=True)
        return ok_all

# --- 施設ごとの色分け（16進指定） ---
# 南浦和→青、岩槻→緑、鈴谷→黄色、岸町→赤
_FACILITY_ALIAS_COLOR_HEX = {
    "南浦和": "0x3498DB",  # Blue
    "岩槻":   "0x2ECC71",  # Green
    "鈴谷":   "0xF1C40F",  # Yellow
    "岸町":   "0xE74C3C",  # Red
}
_DEFAULT_COLOR_HEX = "0x00B894"      # 既定（緑系）

def _hex_to_int(hex_str: str) -> int:
    """'0xRRGGBB' → 10進整数へ安全に変換"""
    try:
        return int(hex_str, 16)
    except Exception:
        return int(_DEFAULT_COLOR_HEX, 16)

def send_aggregate_lines(webhook_url: Optional[str],
                         facility_alias: str,
                         month_text: str,
                         lines: List[str]) -> None:
    """
    施設×月の改善セル一覧をDiscordへ通知。
    - Embed優先、失敗時はテキストへ自動フォールバック
    - 空の lines は投稿しない
    環境変数:
      - DISCORD_FORCE_TEXT: "1" なら常にテキスト
      - DISCORD_THREAD_ID: Forum/スレッド投稿先が必要な場合の thread_id
      - DISCORD_WAIT: "1" でレスポンス本文取得（デフォルトON）
      - DISCORD_MAX_LINES: 行数上限（長文抑制）
      - DISCORD_USER_AGENT: 任意UA
    """
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

    title = f"{facility_alias} {month_text}"
    description = "\n".join(lines)

    # 施設ごと色決め（alias ベース）
    color_hex = _FACILITY_ALIAS_COLOR_HEX.get(facility_alias, _DEFAULT_COLOR_HEX)
    color_int = _hex_to_int(color_hex)

    client = DiscordWebhookClient.from_env()
    client.webhook_url = webhook_url  # 明示引数を優先

    if force_text:
        content = f"**{title}**\n{description}"
        client.send_text(content)
        return

    client.send_embed(title=title, description=description, color=color_int, footer_text="Facility monitor")

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
                short = FACILITY_TITLE_ALIAS.get(facility.get('name',''), facility.get('name','')) or facility.get('name','')
                outdir = facility_month_dir(short or 'unknown_facility', month_text)
                print(f"[INFO] outdir={outdir}", flush=True)

                # ---- 集計（当月） ----
                summary, details = summarize_vacancies(page, cal_root, config)

                # 前回payload（summary/details）を取得（通知用）
                prev_payload = load_last_payload(outdir)
                prev_summary = (prev_payload or {}).get("summary")
                prev_details = (prev_payload or {}).get("details") or []

                changed = summaries_changed(prev_summary, summary)
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

                # ★ 集約通知（当月）
                lines = build_aggregate_lines(month_text, prev_details, details)
                if lines:
                    send_aggregate_lines(DISCORD_WEBHOOK_URL, short, month_text, lines)

                # ---- 月遷移ループ ----
                shifts = facility.get("month_shifts", [0,1])
                shifts = sorted(set(int(s) for s in shifts if isinstance(s,(int,float))))
                if 0 not in shifts: shifts.insert(0,0)
                max_shift = max(shifts); prev_month_text = month_text

                for step in range(1, max_shifts_plus_one(max_shift)):
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

                        # ★ 集約通知（遷移先の月）
                        lines2 = build_aggregate_lines(month_text2, prev_details2, details2)
                        if lines2:
                            send_aggregate_lines(DISCORD_WEBHOOK_URL, short, month_text2, lines2)

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

def max_shifts_plus_one(max_shift: int) -> int:
    """for range(1, max_shift+1) の補助（読みやすさ向上のため）。"""
    return max_shift + 1

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

