
# -*- coding: utf-8 -*-
"""
さいたま市 施設予約システムの空き状況監視
 - 監視範囲の限定（カレンダー枠）
 - 日付の無いセルをスキップ
 - 次の月遷移の安定化（施設固有セレクタ／outerHTML変化待ち）
 - 集計が変化した時のみタイムスタンプ付き履歴を保存
 - パフォーマンス改善（不要リソースのロード抑制）
 - 監視時間帯のガードを CLI/環境変数で制御可能（--force / MONITOR_FORCE）

【環境変数（GitHub Secrets 想定）】
- BASE_URL: 例 "https://saitama.rsv.ws-scs.jp/web/"
- DISCORD_WEBHOOK_URL: （任意）

【監視時間帯の制御（任意）】
- MONITOR_FORCE=1        → 時間帯ガード無効化（常に実行）
- MONITOR_START_HOUR=5   → 開始時刻（JST、デフォルト 5）
- MONITOR_END_HOUR=23    → 終了時刻（JST、デフォルト 23）

【config.json 例】
{
  "facilities": [
    {
      "name": "南浦和コミュニティセンター",
      "click_sequence": ["施設の空き状況", "利用目的から", "屋内スポーツ", "バドミントン", "南浦和コミュニティセンター"],
      "month_shifts": [0, 1, 2, 3],
      "calendar_selector": "table.reservation-calendar",
      "next_month_selector": "a[href*='moveCalender']"
    }
  ],
  "next_month_label": "次の月",
  "calendar_root_hint": "空き状況",
  "status_patterns": { ... },
  "css_class_patterns": { ... },
  "debug": { "dump_calendar_html": true, ... }
}
"""

import os
import sys
import json
import re
import time
import datetime
from pathlib import Path

from PIL import Image, ImageDraw  # 互換のため残置
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# JST時間帯チェック
try:
    import pytz
except Exception:
    pytz = None

# === 環境変数（GitHub Secrets） ===
BASE_URL = os.getenv("BASE_URL")  # 例: "https://saitama.rsv.ws-scs.jp/web/"
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

# === 監視時間帯の制御 ===
MONITOR_FORCE = os.getenv("MONITOR_FORCE", "0").strip() == "1"
MONITOR_START_HOUR = int(os.getenv("MONITOR_START_HOUR", "5"))
MONITOR_END_HOUR = int(os.getenv("MONITOR_END_HOUR", "23"))

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

STATUS_RANK = {"×": 0, "△": 1, "○": 2, "〇": 2}

# ------------------------------
# 基本ユーティリティ
# ------------------------------
def ensure_dirs():
    SNAP_DIR.mkdir(parents=True, exist_ok=True)

def load_config():
    """純粋JSONとして config.json をロードし、最低限のキーを検証"""
    try:
        text = CONFIG_PATH.read_text("utf-8")
        cfg = json.loads(text)
    except json.JSONDecodeError as e:
        print(f"[ERROR] config.json の読み込みに失敗: {e}", flush=True)
        raise
    for key in ["facilities", "status_patterns", "css_class_patterns"]:
        if key not in cfg:
            raise RuntimeError(f"config.json の '{key}' が不足しています")
    return cfg

def jst_now():
    if pytz is None:
        return datetime.datetime.now()
    jst = pytz.timezone("Asia/Tokyo")
    return datetime.datetime.now(jst)

def is_within_monitoring_window(start_hour=5, end_hour=23):
    """JSTで 05:00〜23:59 を監視対象にする"""
    try:
        now = jst_now()
        return start_hour <= now.hour <= end_hour, now
    except Exception:
        return True, None  # 失敗時は実行

# ------------------------------
# Playwright 操作（遷移）
# ------------------------------
def try_click_text(page, label, timeout_ms=15000, quiet=True):
    locators = [
        page.get_by_role("link", name=label, exact=True),
        page.get_by_role("button", name=label, exact=True),
        page.get_by_text(label, exact=True),
        page.locator(f"text={label}"),
    ]
    for locator in locators:
        try:
            locator.wait_for(timeout=timeout_ms)
            locator.scroll_into_view_if_needed()
            locator.click(timeout=timeout_ms)
            return True
        except Exception as e:
            if not quiet:
                print(f"[WARN] try_click_text: 例外 {e}（label='{label}'）", flush=True)
            continue
    return False

def navigate_to_facility(page, facility):
    if not BASE_URL:
        raise RuntimeError("BASE_URL が未設定です。Secrets の BASE_URL に https://saitama.rsv.ws-scs.jp/web/ を入れてください。")

    page.goto(BASE_URL, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_load_state("domcontentloaded", timeout=30000)

    for opt in ["同意する", "OK", "確認", "閉じる"]:
        try_click_text(page, opt, timeout_ms=2000)

    for label in facility.get("click_sequence", []):
        ok = try_click_text(page, label)
        if not ok:
            raise RuntimeError(f"クリック対象が見つかりません：『{label}』（施設: {facility.get('name','')}）")

def get_current_year_month_text(page, calendar_root=None):
    pattern = re.compile(r"(\d{4})\s*年\s*(\d{1,2})\s*月")
    targets = []
    if calendar_root is not None:
        try:
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

def locate_calendar_root(page, hint: str, facility: dict = None):
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

def dump_calendar_html(calendar_root, out_path: Path):
    try:
        html = calendar_root.evaluate("el => el.outerHTML")
        Path(out_path).write_text(html, "utf-8")
    except Exception as e:
        print(f"[WARN] calendar HTML dump 失敗: {e}", flush=True)

def take_calendar_screenshot(calendar_root, out_path: Path):
    calendar_root.scroll_into_view_if_needed()
    calendar_root.screenshot(path=str(out_path))

# ------------------------------
# 月遷移（安定化）
# ------------------------------
def _compute_next_month_text(prev_month_text: str) -> str:
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

def click_next_month(page,
                     label_primary="次の月",
                     calendar_root=None,
                     prev_month_text=None,
                     wait_timeout_ms=20000,
                     facility=None):
    sel_cfg = (facility or {}).get("next_month_selector")
    clicked = False

    def _try_click(selector: str):
        nonlocal clicked
        try:
            el = page.locator(selector).first
            el.scroll_into_view_if_needed()
            el.click(timeout=2000)
            clicked = True
        except Exception:
            pass

    if sel_cfg:
        _try_click(sel_cfg)

    scopes = []
    if calendar_root is not None:
        scopes.append(calendar_root)
    scopes.append(page)

    selectors = [
        "a:has-text('次の月')",
        "a:has-text('次')",
        "a[href*='moveCalender']",
    ]

    for scope in scopes:
        if clicked:
            break
        for sel in selectors[:2]:
            try:
                el = scope.locator(sel).first
                el.scroll_into_view_if_needed()
                el.click(timeout=2000)
                clicked = True
                break
            except Exception:
                pass
        if clicked:
            break
        try:
            el = scope.locator("a[href*='moveCalender']").first
            el.scroll_into_view_if_needed()
            el.click(timeout=2000)
            clicked = True
        except Exception:
            pass
        if clicked:
            break
        try:
            el = scope.locator("a[href*='moveCalender']").first
            href = el.get_attribute("href") or ""
