#!/usr/bin/env python3
"""Fetch Pro-Football-Reference player stats and write requested CSV schema."""

from __future__ import annotations

import argparse
import csv
import html
import json
import os
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup, Comment

COMMENT_RE = re.compile(r"<!--|-->")
TABLE_RE = re.compile(
    r"<table[^>]*\bid=(?:\"([^\"]+)\"|'([^']+)')[^>]*>(.*?)</table>",
    re.DOTALL | re.IGNORECASE,
)
CAPTION_RE = re.compile(r"<caption[^>]*>(.*?)</caption>", re.DOTALL | re.IGNORECASE)
ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.DOTALL | re.IGNORECASE)
CELL_STAT_RE = re.compile(
    r'<(t[hd])[^>]*data-stat="([^"]+)"[^>]*>(.*?)</t[hd]>',
    re.DOTALL | re.IGNORECASE,
)
YEAR_RE = re.compile(r"(19\d{2}|20\d{2})")
TEAM_CODE_MAP = {
    "RAI": "LVR",
    "OAK": "LVR",
    "RAM": "LAR",
    "STL": "LAR",
    "SDG": "LAC",
    "PHO": "ARI",
}

DEFAULT_URL = "https://www.pro-football-reference.com/players/A/AvilSt00.htm"
DEFAULT_CSV = "out.csv"
ESPN_PROFILES_CSV = Path(__file__).parent / "output" / "espn_nfl_player_profiles.csv"

_ESPN_PROFILE_CACHE: Optional[List[Dict[str, str]]] = None

OUTPUT_COLUMNS = [
    "first_name",
    "last_name",
    "full_name",
    "dob",
    "draft_round",
    "draft_overall",
    "player_id",
    "image_link",
    "season",
    "college",
    "team",
    "awards",
    "pos",
    "ap_1",
    "ap_2",
    "ap",
    "pb",
    "opoy_top5",
    "opoy_votes",
    "opoy",
    "oroy_top5",
    "oroy_votes",
    "oroy",
    "cpoy_top5",
    "cpoy_votes",
    "cpoy",
    "mvp_top5",
    "mvp_votes",
    "mvp",
    "fan_pts",
    "fan_pts_ppr",
    "pass_yds",
    "pass_td",
    "pass_int",
    "pass_lng",
    "qbr",
    "pass_rating",
    "cmp_pct",
    "rush_yds",
    "rush_lng",
    "rush_ypa",
    "rush_ypg",
    "rush_td",
    "rec",
    "rec_yds",
    "rec_td",
    "rec_lng",
    "rec_ypg",
    "touches",
    "scrim_yds",
    "tot_td",
    "fmb",
    "playoff_rush_yds",
    "playoff_rush_td",
    "playoff_rush_lng",
    "playoff_rec_yds",
    "playoff_rec_td",
    "playoff_rec_lng",
    "playoff_scrim_yds",
    "playoff_fmb",
    "playoff_pass_int",
    "playoff_pass_yds",
    "playoff_pass_td",
    "playoff_pass_rating",
    "playoff_pass_lng",
    "sb_champ",
    "sb_mvp",
    "sb_champ_any",
    "sb_mvp_any",
    "mvp_top5_any",
    "mvp_any",
    "oroy_any",
    "opoy_any",
    "opoy_top5_any",
    "ap_1_any",
    "ap_2_any",
    "ap_any",
    "pb_any",
    "cpoy_top5_any",
    "cpoy_any",
    "career_rush_yds",
    "career_rec",
    "career_rec_yds",
    "career_rush_td",
    "career_rec_td",
    "career_pass_yds",
    "career_pass_td",
]


def _clean_text(value: str) -> str:
    value = re.sub(r"<[^>]+>", "", value)
    value = html.unescape(value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _strip_comments(text: str) -> str:
    return COMMENT_RE.sub("", text)


def _build_driver():
    from selenium import webdriver

    options = webdriver.ChromeOptions()
    if os.getenv("HEADLESS", "0") == "1":
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    profile_dir = Path(__file__).parent / ".selenium_profile"
    profile_dir.mkdir(parents=True, exist_ok=True)
    options.add_argument(f"--user-data-dir={profile_dir}")
    driver = webdriver.Chrome(options=options)
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"},
    )
    return driver


def _fetch_html_via_browser(url: str) -> str:
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    driver = _build_driver()
    try:
        for attempt in range(1, 4):
            driver.get(url)
            WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            page_text = (driver.page_source or "").lower()
            title = (driver.title or "").lower()

            if "security verification" in page_text or "just a moment" in title:
                print(
                    f"Cloudflare challenge detected (attempt {attempt}/3). "
                    "Complete verification in the browser, then press Enter."
                )
                input()
                time.sleep(2)
                continue

            if "<table" in page_text and "/players/" in driver.current_url:
                return driver.page_source

            if attempt < 3:
                print("Page loaded without tables yet; retrying...")
                time.sleep(2)

        raise RuntimeError(
            "Unable to load player tables after browser verification. "
            "Cloudflare may still be blocking this session."
        )
    finally:
        driver.quit()


def _fetch_html(url: str) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive",
    }

    try:
        from curl_cffi import requests as curl_requests  # type: ignore
    except Exception:
        curl_requests = None  # type: ignore

    if curl_requests is not None:
        session = curl_requests.Session(impersonate="chrome110")
        session.headers.update(headers)
        resp = session.get(url, timeout=30, headers={"Referer": "https://www.pro-football-reference.com/"})
        if resp.status_code != 403:
            resp.raise_for_status()
            return resp.text

    try:
        import cloudscraper  # type: ignore
    except Exception:
        cloudscraper = None  # type: ignore

    if cloudscraper is not None:
        scraper = cloudscraper.create_scraper()
        resp = scraper.get(url, timeout=30, headers=headers)
    else:
        session = requests.Session()
        session.headers.update(headers)
        session.get("https://www.pro-football-reference.com/", timeout=30)
        resp = session.get(url, timeout=30, headers={"Referer": "https://www.pro-football-reference.com/"})

    if resp.status_code == 403:
        return _fetch_html_via_browser(url)
    resp.raise_for_status()
    return resp.text


def _parse_table(table_html: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for row_html in ROW_RE.findall(table_html):
        if 'class="thead"' in row_html or "over_header" in row_html:
            continue

        row: Dict[str, str] = {}
        if 'class="left trophy"' in row_html:
            row["_sb_trophy"] = "1"

        for _, stat, cell in CELL_STAT_RE.findall(row_html):
            value = _clean_text(cell)
            if stat in row and value:
                row[stat] = f"{row[stat]} {value}".strip()
            else:
                row[stat] = value

        if row:
            rows.append(row)
    return rows


def _parse_bs4_table(table) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for tr in table.select("tr"):
        classes = tr.get("class") or []
        if "thead" in classes or "over_header" in classes:
            continue

        row: Dict[str, str] = {}
        if tr.select_one("th.left.trophy, td.left.trophy"):
            row["_sb_trophy"] = "1"

        for cell in tr.select("th[data-stat], td[data-stat]"):
            stat = cell.get("data-stat", "").strip()
            if not stat:
                continue
            value = _clean_text(cell.get_text(" ", strip=True))
            if stat in row and value:
                row[stat] = f"{row[stat]} {value}".strip()
            else:
                row[stat] = value

        if row:
            rows.append(row)
    return rows


def _extract_colleges(html_text: str) -> List[str]:
    colleges: List[str] = []

    soup = BeautifulSoup(html_text, "html.parser")
    meta_div = soup.select_one("#meta")

    if meta_div:
        for p in meta_div.select("p"):
            label = p.select_one("strong")
            if not label:
                continue
            if "college" not in label.get_text(" ", strip=True).lower():
                continue
            links = p.select("a")
            if links:
                for a in links:
                    c = _clean_text(a.get_text(" ", strip=True))
                    if c.lower() in {"college stats", "stats"}:
                        continue
                    if c and c not in colleges:
                        colleges.append(c)
            else:
                text = _clean_text(p.get_text(" ", strip=True))
                text = re.sub(r"^College:\s*", "", text, flags=re.IGNORECASE)
                if text and text not in colleges:
                    colleges.append(text)

    if not colleges:
        for m in re.finditer(r"College:\s*</strong>\s*(?:<a[^>]*>)?([^<]+)", html_text, re.I):
            c = _clean_text(m.group(1))
            if c and c not in colleges:
                colleges.append(c)
    return colleges


def _normalize_name(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip()).lower()


def _normalize_scalar(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (value or "").strip().lower())


def _ordinal_to_int(value: str) -> str:
    m = re.search(r"\b(\d+)(?:st|nd|rd|th)?\b", value or "", re.IGNORECASE)
    if not m:
        return ""
    return m.group(1)


def _extract_draft_info(html_text: str) -> Tuple[str, str]:
    soup = BeautifulSoup(html_text, "html.parser")
    meta_div = soup.select_one("#meta")
    if not meta_div:
        return "", ""

    draft_text = ""
    for p in meta_div.select("p"):
        label = p.select_one("strong")
        if not label:
            continue
        if "draft" not in label.get_text(" ", strip=True).lower():
            continue
        draft_text = p.get_text(" ", strip=True)
        break

    if not draft_text:
        return "", ""

    draft_round = ""
    draft_overall = ""

    m_round = re.search(r"\b(\d+)(?:st|nd|rd|th)?\s+round\b", draft_text, re.IGNORECASE)
    if m_round:
        draft_round = m_round.group(1)

    m_overall = re.search(r"\((\d+)(?:st|nd|rd|th)?\s+overall\)", draft_text, re.IGNORECASE)
    if m_overall:
        draft_overall = m_overall.group(1)
    else:
        m_overall = re.search(r"\b(\d+)(?:st|nd|rd|th)?\s+overall\b", draft_text, re.IGNORECASE)
        if m_overall:
            draft_overall = m_overall.group(1)

    return draft_round, draft_overall


def _estimate_first_season(html_text: str) -> str:
    seasons = [int(m.group(1)) for m in re.finditer(r'data-stat="year_id">\s*(19\d{2}|20\d{2})\s*<', html_text)]
    if not seasons:
        return ""
    return str(min(seasons))


def _load_espn_profiles(path: Path = ESPN_PROFILES_CSV) -> List[Dict[str, str]]:
    global _ESPN_PROFILE_CACHE
    if _ESPN_PROFILE_CACHE is not None:
        return _ESPN_PROFILE_CACHE

    if not path.exists():
        _ESPN_PROFILE_CACHE = []
        return _ESPN_PROFILE_CACHE

    records: List[Dict[str, str]] = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(
                {
                    "full_name": (row.get("full_name") or "").strip(),
                    "first_name": (row.get("first_name") or "").strip(),
                    "last_name": (row.get("last_name") or "").strip(),
                    "dob": (row.get("dob") or "").strip(),
                    "college": (row.get("college") or "").strip(),
                    "draft_round": _ordinal_to_int(row.get("draft_round") or ""),
                    "draft_overall": _ordinal_to_int(row.get("draft_overall") or ""),
                    "player_image": (row.get("player_image") or "").strip(),
                }
            )
    _ESPN_PROFILE_CACHE = records
    return _ESPN_PROFILE_CACHE


def _single_match(rows: List[Dict[str, str]]) -> Optional[Dict[str, str]]:
    if len(rows) == 1:
        return rows[0]
    return None


def _match_espn_image(
    profiles: List[Dict[str, str]],
    full_name: str,
    first_name: str,
    last_name: str,
    dob: str,
    colleges: List[str],
    draft_round: str,
    draft_overall: str,
) -> str:
    if not profiles:
        return ""

    full_name_matches = [p for p in profiles if _normalize_name(p["full_name"]) == _normalize_name(full_name)]
    exact = _single_match(full_name_matches)
    if exact:
        return exact.get("player_image", "")

    if dob:
        last_dob = [
            p
            for p in profiles
            if _normalize_name(p["last_name"]) == _normalize_name(last_name)
            and (p.get("dob") or "") == dob
        ]
        exact = _single_match(last_dob)
        if exact:
            return exact.get("player_image", "")

        first_dob = [
            p
            for p in profiles
            if _normalize_name(p["first_name"]) == _normalize_name(first_name)
            and (p.get("dob") or "") == dob
        ]
        exact = _single_match(first_dob)
        if exact:
            return exact.get("player_image", "")

    if dob and draft_round and draft_overall:
        draft_dob = [
            p
            for p in profiles
            if (p.get("dob") or "") == dob
            and (p.get("draft_round") or "") == draft_round
            and (p.get("draft_overall") or "") == draft_overall
        ]
        exact = _single_match(draft_dob)
        if exact:
            return exact.get("player_image", "")

    if dob and colleges:
        college_keys = {_normalize_scalar(c) for c in colleges if c}
        college_dob = [
            p
            for p in profiles
            if (p.get("dob") or "") == dob
            and _normalize_scalar(p.get("college") or "") in college_keys
        ]
        exact = _single_match(college_dob)
        if exact:
            return exact.get("player_image", "")

    return ""


def _extract_player_meta(html_text: str, url: str) -> Dict[str, str]:
    meta: Dict[str, str] = {}

    soup = BeautifulSoup(html_text, "html.parser")
    meta_div = soup.select_one("#meta")

    full_name = ""
    if meta_div:
        h1 = meta_div.select_one("h1")
        if h1:
            full_name = _clean_text(h1.get_text(" ", strip=True))
    if not full_name:
        name_match = re.search(r'<h1[^>]*itemprop="name"[^>]*>(.*?)</h1>', html_text, re.I | re.S)
        full_name = _clean_text(name_match.group(1)) if name_match else ""
    if not full_name:
        h1 = soup.find("h1")
        if h1:
            full_name = _clean_text(h1.get_text(" ", strip=True))
    first_name = ""
    last_name = ""
    if full_name:
        parts = full_name.split(" ", 1)
        first_name = parts[0]
        last_name = parts[1] if len(parts) > 1 else ""

    dob = ""
    dob_match = re.search(r'data-birth="(\d{4}-\d{2}-\d{2})"', html_text, re.I)
    if dob_match:
        raw_dob = dob_match.group(1)
        y, m, d = raw_dob.split("-")
        dob = f"{int(m):02d}/{int(d):02d}/{y}"

    colleges = _extract_colleges(html_text)
    draft_round, draft_overall = _extract_draft_info(html_text)
    profiles = _load_espn_profiles()
    image_link = _match_espn_image(
        profiles=profiles,
        full_name=full_name,
        first_name=first_name,
        last_name=last_name,
        dob=dob,
        colleges=colleges,
        draft_round=draft_round,
        draft_overall=draft_overall,
    )
    if not image_link:
        first_season = _estimate_first_season(html_text)
        print(f"(no matching image for {full_name or 'player'}, {first_season})")

    meta["first_name"] = first_name
    meta["last_name"] = last_name
    meta["full_name"] = full_name
    meta["player_id"] = url.rstrip("/").split("/")[-1].replace(".htm", "")
    meta["image_link"] = image_link
    meta["dob"] = dob
    meta["draft_round"] = draft_round
    meta["draft_overall"] = draft_overall
    meta["college_json"] = json.dumps(colleges)
    return meta


def _year_from_row(row: Dict[str, str]) -> str:
    for key in ("year_id", "year", "season", "year_ID"):
        v = row.get(key, "").strip()
        m = YEAR_RE.search(v)
        if m:
            return m.group(1)
    return ""


def _team_from_row(row: Dict[str, str]) -> str:
    team = (
        row.get("team")
        or row.get("team_name_abbr")
        or row.get("tm")
        or row.get("team_id")
        or row.get("team_ID")
        or ""
    ).strip().upper()
    return TEAM_CODE_MAP.get(team, team)


def _awards_from_row(row: Dict[str, str]) -> str:
    pieces: List[str] = []
    for key, value in row.items():
        if "award" in key.lower() and value.strip():
            pieces.append(value.strip())
    return " ".join(pieces).strip()


def _merge_awards(a: str, b: str) -> str:
    tokens: List[str] = []
    seen = set()
    for src in (a, b):
        for token in re.split(r"[\s,]+", src.strip()):
            if not token:
                continue
            t = token.upper()
            if t in seen:
                continue
            seen.add(t)
            tokens.append(token)
    return " ".join(tokens)


def _token_present(awards: str, token: str) -> bool:
    return bool(re.search(rf"\b{re.escape(token)}\b", awards, re.IGNORECASE))


def _award_rank_present(awards: str, prefix: str, ranks: List[int]) -> bool:
    return bool(re.search(rf"\b{re.escape(prefix)}-({'|'.join(str(r) for r in ranks)})\b", awards, re.IGNORECASE))


def _award_votes_present(awards: str, prefix: str) -> bool:
    return bool(re.search(rf"\b{re.escape(prefix)}-\d+\b", awards, re.IGNORECASE))

def _sb_mvp_present(awards: str) -> bool:
    return bool(
        re.search(r"\bSB\s*MVP(?:-\d+)?\b", awards, re.IGNORECASE)
        or re.search(r"\bSUPER\s+BOWL\s+MVP(?:-\d+)?\b", awards, re.IGNORECASE)
    )


def _normalize_table_id(table_id: str) -> str:
    return table_id.strip().lower()


def get_player_tables(url: str) -> Tuple[Dict[str, str], Dict[str, List[Dict[str, str]]]]:
    html_text = _fetch_html(url)
    meta = _extract_player_meta(html_text, url)

    tables: Dict[str, List[Dict[str, str]]] = {}
    soup = BeautifulSoup(html_text, "html.parser")

    for table in soup.select("table[id]"):
        table_id = _normalize_table_id(table.get("id", ""))
        if not table_id:
            continue
        rows = _parse_bs4_table(table)
        if rows:
            tables[table_id] = rows

    for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
        text = str(comment)
        if "<table" not in text:
            continue
        comment_soup = BeautifulSoup(text, "html.parser")
        for table in comment_soup.select("table[id]"):
            table_id = _normalize_table_id(table.get("id", ""))
            if not table_id or table_id in tables:
                continue
            rows = _parse_bs4_table(table)
            if rows:
                tables[table_id] = rows

    return meta, tables


def _table_rows(tables: Dict[str, List[Dict[str, str]]], ids: Tuple[str, ...]) -> List[Dict[str, str]]:
    for table_id in ids:
        if table_id in tables:
            return tables[table_id]
    return []


def _table_rows_like(
    tables: Dict[str, List[Dict[str, str]]], include_tokens: Tuple[str, ...], exclude_tokens: Tuple[str, ...] = ()
) -> List[Dict[str, str]]:
    for table_id, rows in tables.items():
        tid = table_id.lower()
        if not all(token in tid for token in include_tokens):
            continue
        if any(token in tid for token in exclude_tokens):
            continue
        return rows
    return []


def _find_row_by_season_team(rows: List[Dict[str, str]], season: str, team: str) -> Dict[str, str]:
    for row in rows:
        if _year_from_row(row) == season and _team_from_row(row) == team:
            return row
    return {}


def _find_row_by_season(rows: List[Dict[str, str]], season: str) -> Dict[str, str]:
    for row in rows:
        if _year_from_row(row) == season:
            return row
    return {}


def _base_keys(pass_rows: List[Dict[str, str]], rr_rows: List[Dict[str, str]]) -> List[Tuple[str, str]]:
    keys = []
    seen = set()
    for row in pass_rows + rr_rows:
        season = _year_from_row(row)
        team = _team_from_row(row)
        if not season:
            continue
        if not team:
            team = "TM"
        key = (season, team)
        if key in seen:
            continue
        seen.add(key)
        keys.append(key)
    keys.sort(key=lambda x: (x[0], x[1]))
    return keys


def _tm_awards_by_season(pass_rows: List[Dict[str, str]], rr_rows: List[Dict[str, str]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for row in pass_rows + rr_rows:
        season = _year_from_row(row)
        team = _team_from_row(row).upper()
        if not season or "TM" not in team:
            continue
        awards = _awards_from_row(row)
        if not awards:
            continue
        out[season] = _merge_awards(out.get(season, ""), awards)
    return out


def _truth(v: bool) -> str:
    return "true" if v else "false"


def _to_int(value: str) -> int:
    if not value:
        return 0
    cleaned = re.sub(r"[^0-9\-]", "", str(value))
    if not cleaned or cleaned == "-":
        return 0
    try:
        return int(cleaned)
    except ValueError:
        return 0


def _format_fan_pts(value: float, decimals: int = 2) -> str:
    text = f"{value:.{decimals}f}".rstrip("0").rstrip(".")
    return text if text else "0"


def _calc_fan_pts_common(
    pass_yds: str,
    pass_td: str,
    pass_int: str,
    rec: str,
    rush_yds: str,
    rec_yds: str,
    rush_td: str,
    rec_td: str,
    fmb: str,
    ppr: bool,
    decimals: int,
) -> str:
    points = (
        (_to_int(rec) if ppr else 0)
        + (_to_int(pass_yds) / 25.0)
        + (_to_int(pass_td) * 4)
        - (_to_int(pass_int) * 2)
        + ((_to_int(rush_yds) + _to_int(rec_yds)) / 10.0)
        + ((_to_int(rush_td) + _to_int(rec_td)) * 6)
        - _to_int(fmb)
    )
    return _format_fan_pts(points, decimals=decimals)


def _calc_fan_pts(
    pass_yds: str,
    pass_td: str,
    pass_int: str,
    rush_yds: str,
    rec_yds: str,
    rush_td: str,
    rec_td: str,
    fmb: str,
) -> str:
    return _calc_fan_pts_common(
        pass_yds=pass_yds,
        pass_td=pass_td,
        pass_int=pass_int,
        rec="0",
        rush_yds=rush_yds,
        rec_yds=rec_yds,
        rush_td=rush_td,
        rec_td=rec_td,
        fmb=fmb,
        ppr=False,
        decimals=1,
    )


def _calc_fan_pts_ppr(
    rec: str,
    pass_yds: str,
    pass_td: str,
    pass_int: str,
    rush_yds: str,
    rec_yds: str,
    rush_td: str,
    rec_td: str,
    fmb: str,
) -> str:
    return _calc_fan_pts_common(
        pass_yds=pass_yds,
        pass_td=pass_td,
        pass_int=pass_int,
        rec=rec,
        rush_yds=rush_yds,
        rec_yds=rec_yds,
        rush_td=rush_td,
        rec_td=rec_td,
        fmb=fmb,
        ppr=True,
        decimals=2,
    )


def _compose_rows(meta: Dict[str, str], tables: Dict[str, List[Dict[str, str]]]) -> List[Dict[str, str]]:
    pass_rows = _table_rows(tables, ("passing",))
    if not pass_rows:
        pass_rows = _table_rows_like(tables, include_tokens=("passing",), exclude_tokens=("playoff", "post"))

    rr_rows = _table_rows(tables, ("rushing_and_receiving", "receiving_and_rushing"))
    if not rr_rows:
        rr_rows = _table_rows_like(tables, include_tokens=("rushing", "receiving"), exclude_tokens=("playoff", "post"))

    pass_post_rows = _table_rows(tables, ("passing_playoffs", "passing_post"))
    if not pass_post_rows:
        pass_post_rows = _table_rows_like(tables, include_tokens=("passing", "playoff"))
        if not pass_post_rows:
            pass_post_rows = _table_rows_like(tables, include_tokens=("passing", "post"))

    rr_post_rows = _table_rows(tables, ("rushing_and_receiving_playoffs", "receiving_and_rushing_playoffs", "rushing_and_receiving_post"))
    if not rr_post_rows:
        rr_post_rows = _table_rows_like(tables, include_tokens=("rushing", "receiving", "playoff"))
        if not rr_post_rows:
            rr_post_rows = _table_rows_like(tables, include_tokens=("rushing", "receiving", "post"))

    keys = _base_keys(pass_rows, rr_rows)
    tm_awards = _tm_awards_by_season(pass_rows, rr_rows)
    out: List[Dict[str, str]] = []

    for season, team in keys:
        if not YEAR_RE.search(season):
            continue
        p = _find_row_by_season_team(pass_rows, season, team)
        r = _find_row_by_season_team(rr_rows, season, team)

        pp = _find_row_by_season_team(pass_post_rows, season, team) or _find_row_by_season(pass_post_rows, season)
        rp = _find_row_by_season_team(rr_post_rows, season, team) or _find_row_by_season(rr_post_rows, season)
        awards = _merge_awards(_awards_from_row(p), _awards_from_row(r))
        awards = _merge_awards(awards, _awards_from_row(pp))
        awards = _merge_awards(awards, _awards_from_row(rp))
        awards = _merge_awards(awards, tm_awards.get(season, ""))

        sb_champ = (p.get("_sb_trophy") == "1") or (r.get("_sb_trophy") == "1") or (pp.get("_sb_trophy") == "1") or (rp.get("_sb_trophy") == "1")
        sb_mvp = _sb_mvp_present(awards)

        row = {
            "first_name": meta.get("first_name", ""),
            "last_name": meta.get("last_name", ""),
            "full_name": meta.get("full_name", ""),
            "dob": meta.get("dob", ""),
            "draft_round": meta.get("draft_round", ""),
            "draft_overall": meta.get("draft_overall", ""),
            "player_id": meta.get("player_id", ""),
            "image_link": meta.get("image_link", ""),
            "season": season,
            "college": meta.get("college_json", "[]"),
            "team": team,
            "awards": awards,
            "pos": p.get("pos", r.get("pos", "")),
            "ap_1": _truth(_token_present(awards, "AP-1")),
            "ap_2": _truth(_token_present(awards, "AP-2")),
            "ap": "0",
            "pb": _truth(_token_present(awards, "PB")),
            "opoy_top5": _truth(_award_rank_present(awards, "OPoY", [1, 2, 3, 4, 5])),
            "opoy_votes": _truth(_award_votes_present(awards, "OPoY")),
            "opoy": _truth(_token_present(awards, "OPoY-1")),
            "oroy_top5": _truth(_award_rank_present(awards, "ORoY", [1, 2, 3, 4, 5])),
            "oroy_votes": _truth(_award_votes_present(awards, "ORoY")),
            "oroy": _truth(_token_present(awards, "ORoY-1")),
            "cpoy_top5": _truth(_award_rank_present(awards, "CPoY", [1, 2, 3, 4, 5])),
            "cpoy_votes": _truth(_award_votes_present(awards, "CPoY")),
            "cpoy": _truth(_token_present(awards, "CPoY-1")),
            "mvp_top5": _truth(_award_rank_present(awards, "MVP", [1, 2, 3, 4, 5])),
            "mvp_votes": _truth(_award_votes_present(awards, "MVP")),
            "mvp": _truth(_token_present(awards, "MVP-1")),
            "pass_yds": p.get("pass_yds", ""),
            "pass_td": p.get("pass_td", ""),
            "pass_int": p.get("pass_int", ""),
            "pass_lng": p.get("pass_long", p.get("pass_lng", "")),
            "qbr": p.get("qbr", ""),
            "pass_rating": p.get("pass_rating", ""),
            "cmp_pct": p.get("pass_cmp_pct", ""),
            "rush_yds": r.get("rush_yds", ""),
            "rush_lng": r.get("rush_long", ""),
            "rush_ypa": r.get("rush_yds_per_att", ""),
            "rush_ypg": r.get("rush_yds_per_g", ""),
            "rush_td": r.get("rush_td", ""),
            "rec": r.get("rec", ""),
            "rec_yds": r.get("rec_yds", ""),
            "rec_td": r.get("rec_td", ""),
            "rec_lng": r.get("rec_long", ""),
            "rec_ypg": r.get("rec_yds_per_g", ""),
            "touches": r.get("touches", ""),
            "scrim_yds": r.get("yds_from_scrimmage", ""),
            "tot_td": r.get("rush_receive_td", ""),
            "fmb": r.get("fumbles", ""),
            "playoff_rush_yds": rp.get("rush_yds", ""),
            "playoff_rush_td": rp.get("rush_td", ""),
            "playoff_rush_lng": rp.get("rush_long", ""),
            "playoff_rec_yds": rp.get("rec_yds", ""),
            "playoff_rec_td": rp.get("rec_td", ""),
            "playoff_rec_lng": rp.get("rec_long", ""),
            "playoff_scrim_yds": rp.get("yds_from_scrimmage", ""),
            "playoff_fmb": rp.get("fumbles", ""),
            "playoff_pass_int": pp.get("pass_int", ""),
            "playoff_pass_yds": pp.get("pass_yds", ""),
            "playoff_pass_td": pp.get("pass_td", ""),
            "playoff_pass_rating": pp.get("pass_rating", ""),
            "playoff_pass_lng": pp.get("pass_long", pp.get("pass_lng", "")),
            "sb_champ": _truth(sb_champ),
            "sb_mvp": _truth(sb_mvp),
            "sb_champ_any": "false",
            "sb_mvp_any": "false",
            "mvp_top5_any": "false",
            "mvp_any": "false",
            "oroy_any": "false",
            "opoy_any": "false",
            "opoy_top5_any": "false",
            "ap_1_any": "false",
            "ap_2_any": "false",
            "ap_any": "false",
            "pb_any": "false",
            "cpoy_top5_any": "false",
            "cpoy_any": "false",
            "career_rush_yds": "0",
            "career_rec": "0",
            "career_rec_yds": "0",
            "career_rush_td": "0",
            "career_rec_td": "0",
            "career_pass_yds": "0",
            "career_pass_td": "0",
        }
        row["fan_pts"] = _calc_fan_pts(
            pass_yds=row["pass_yds"],
            pass_td=row["pass_td"],
            pass_int=row["pass_int"],
            rush_yds=row["rush_yds"],
            rec_yds=row["rec_yds"],
            rush_td=row["rush_td"],
            rec_td=row["rec_td"],
            fmb=row["fmb"],
        )
        row["fan_pts_ppr"] = _calc_fan_pts_ppr(
            rec=row["rec"],
            pass_yds=row["pass_yds"],
            pass_td=row["pass_td"],
            pass_int=row["pass_int"],
            rush_yds=row["rush_yds"],
            rec_yds=row["rec_yds"],
            rush_td=row["rush_td"],
            rec_td=row["rec_td"],
            fmb=row["fmb"],
        )
        row["ap"] = _truth(row["ap_1"] == "true" or row["ap_2"] == "true")
        if YEAR_RE.search(row["season"]):
            out.append(row)

    if out:
        any_flags = {
            "sb_champ_any": any(r["sb_champ"] == "true" for r in out),
            "sb_mvp_any": any(r["sb_mvp"] == "true" for r in out),
            "mvp_top5_any": any(r["mvp_top5"] == "true" for r in out),
            "mvp_any": any(r["mvp"] == "true" for r in out),
            "oroy_any": any(r["oroy"] == "true" for r in out),
            "opoy_any": any(r["opoy"] == "true" for r in out),
            "opoy_top5_any": any(r["opoy_top5"] == "true" for r in out),
            "ap_1_any": any(r["ap_1"] == "true" for r in out),
            "ap_2_any": any(r["ap_2"] == "true" for r in out),
            "ap_any": any(r["ap"] == "true" for r in out),
            "pb_any": any(r["pb"] == "true" for r in out),
            "cpoy_top5_any": any(r["cpoy_top5"] == "true" for r in out),
            "cpoy_any": any(r["cpoy"] == "true" for r in out),
        }

        career = {
            "career_rush_yds": 0,
            "career_rec": 0,
            "career_rec_yds": 0,
            "career_rush_td": 0,
            "career_rec_td": 0,
            "career_pass_yds": 0,
            "career_pass_td": 0,
        }
        for r in out:
            if "TM" in r.get("team", "").upper():
                continue
            career["career_rush_yds"] += _to_int(r.get("rush_yds", ""))
            career["career_rec"] += _to_int(r.get("rec", ""))
            career["career_rec_yds"] += _to_int(r.get("rec_yds", ""))
            career["career_rush_td"] += _to_int(r.get("rush_td", ""))
            career["career_rec_td"] += _to_int(r.get("rec_td", ""))
            career["career_pass_yds"] += _to_int(r.get("pass_yds", ""))
            career["career_pass_td"] += _to_int(r.get("pass_td", ""))

        for r in out:
            for key, value in any_flags.items():
                r[key] = _truth(value)
            for key, value in career.items():
                r[key] = str(value)

    return out


def _write_csv(rows: List[Dict[str, str]], csv_path: str) -> None:
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({col: row.get(col, "") for col in OUTPUT_COLUMNS})


def run(url: str, csv_path: str = DEFAULT_CSV, write_csv: bool = True) -> List[Dict[str, str]]:
    meta, tables = get_player_tables(url)
    rows = _compose_rows(meta, tables)
    if write_csv:
        _write_csv(rows, csv_path)
    if not rows:
        print(f"Detected table ids: {', '.join(sorted(tables.keys())) or 'none'}")
        print("Warning: no qualifying season rows found for this player/page.")
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch Pro-Football-Reference player stats into CSV")
    parser.add_argument("--url", default=DEFAULT_URL, help="Player URL")
    parser.add_argument("--csv", default=DEFAULT_CSV, help="Output CSV path")
    args = parser.parse_args()

    rows = run(args.url, csv_path=args.csv, write_csv=True)
    print(f"Wrote {len(rows)} rows to {args.csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
