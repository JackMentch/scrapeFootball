#!/usr/bin/env python3
"""Scrape ESPN NFL player links (2008-2025) and player profile fields."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import re
import time
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.espn.com"
PLAYER_CANONICAL = "https://www.espn.com/nfl/player/_/id/{player_id}"

TEAM_CODES = [
    "ari",
    "atl",
    "bal",
    "buf",
    "car",
    "chi",
    "cin",
    "cle",
    "dal",
    "den",
    "det",
    "gb",
    "hou",
    "ind",
    "jax",
    "kc",
    "lv",
    "lac",
    "lar",
    "mia",
    "min",
    "ne",
    "no",
    "nyg",
    "nyj",
    "phi",
    "pit",
    "sea",
    "sf",
    "tb",
    "ten",
    "wsh",
    # Historical aliases for earlier seasons.
    "oak",
    "sd",
    "stl",
]

URL_PATTERNS = [
    "https://www.espn.com/nfl/team/stats/_/name/{team}/season/{year}/seasontype/2",
    "https://www.espn.com/nfl/team/stats/_/type/defense/name/{team}/season/{year}/seasontype/2",
    "https://www.espn.com/nfl/team/stats/_/type/special/name/{team}/season/{year}/seasontype/2",
]

PLAYER_LINK_RE = re.compile(
    r"(?:https?://www\.espn\.com)?/nfl/player/_/id/(\d+)(?:/[A-Za-z0-9\-\._~%]*)?",
    re.IGNORECASE,
)
YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2})\b")
DOB_RE = re.compile(
    r"\b(?:DOB|Born|Birthdate)\s*:?\s*"
    r"([A-Za-z]{3,9}\.? \d{1,2}, \d{4}|\d{1,2}/\d{1,2}/\d{2,4})",
    re.IGNORECASE,
)


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    return session


def get_html(session: requests.Session, url: str, retries: int = 3) -> Optional[str]:
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code == 200:
                return resp.text
            if resp.status_code in (403, 404):
                return None
        except requests.RequestException:
            pass
        if attempt < retries:
            time.sleep(1.5 * attempt)
    return None


def extract_player_links_from_html(html_text: str) -> Set[str]:
    links: Set[str] = set()
    for match in PLAYER_LINK_RE.finditer(html_text):
        player_id = match.group(1)
        links.add(PLAYER_CANONICAL.format(player_id=player_id))
    return links


def collect_unique_player_links(
    session: requests.Session, start_year: int, end_year: int, delay_seconds: float
) -> List[str]:
    links: Set[str] = set()
    total_years = (end_year - start_year) + 1
    total_teams = len(TEAM_CODES)
    total_urls_per_year = total_teams * len(URL_PATTERNS)
    attempted_urls = 0
    fetched_urls = 0
    t0 = time.time()

    print(
        "Starting link collection "
        f"for seasons {start_year}-{end_year} "
        f"({total_years} years, {total_urls_per_year} pages/year)"
    )
    for year in range(start_year, end_year + 1):
        year_start = time.time()
        before_year = len(links)
        print(f"[links] Season {year}: starting")
        for team_idx, team in enumerate(TEAM_CODES, start=1):
            before_team = len(links)
            for pattern in URL_PATTERNS:
                url = pattern.format(team=team, year=year)
                attempted_urls += 1
                html_text = get_html(session, url)
                if not html_text:
                    continue
                fetched_urls += 1
                links.update(extract_player_links_from_html(html_text))
                time.sleep(delay_seconds)
            added_team = len(links) - before_team
            print(
                f"[links] Season {year} | team {team_idx}/{total_teams} ({team}) "
                f"-> +{added_team} unique (total {len(links)})"
            )
        added_year = len(links) - before_year
        print(
            f"[links] Season {year}: complete, +{added_year} unique "
            f"in {round(time.time() - year_start, 1)}s"
        )
    ordered = sorted(links)
    print(
        f"Collected {len(ordered)} unique player links "
        f"(attempted pages={attempted_urls}, fetched pages={fetched_urls}) "
        f"in {round(time.time() - t0, 1)}s"
    )
    return ordered


def iter_dicts(obj: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(obj, dict):
        yield obj
        for value in obj.values():
            yield from iter_dicts(value)
    elif isinstance(obj, list):
        for item in obj:
            yield from iter_dicts(item)


def parse_next_data(soup: BeautifulSoup, html_text: str = "") -> Optional[Dict[str, Any]]:
    script = soup.find("script", id="__NEXT_DATA__")
    if script:
        # Some pages include whitespace/newlines that make .string None.
        payload = script.string or script.get_text(strip=False)
        if payload:
            try:
                return json.loads(payload)
            except json.JSONDecodeError:
                pass

    if html_text:
        # Fallback for cases where BeautifulSoup misses the script payload.
        m = re.search(
            r"<script[^>]*id=[\"']__NEXT_DATA__[\"'][^>]*>(.*?)</script>",
            html_text,
            flags=re.DOTALL | re.IGNORECASE,
        )
        if m:
            payload = m.group(1).strip()
            if payload:
                try:
                    return json.loads(payload)
                except json.JSONDecodeError:
                    return None
    return None


def to_year(value: Any) -> Optional[int]:
    if value is None:
        return None
    s = str(value)
    m = YEAR_RE.search(s)
    if not m:
        return None
    year = int(m.group(1))
    if 1950 <= year <= 2035:
        return year
    return None


def normalize_dob(value: str) -> str:
    s = (value or "").strip()
    if not s:
        return ""

    # ISO-ish formats: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS...
    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if m:
        y, mo, d = m.groups()
        return f"{int(mo):02d}/{int(d):02d}/{int(y):04d}"

    # Slash formats.
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{2,4})$", s)
    if m:
        mo, d, y = m.groups()
        year = int(y)
        if len(y) == 2:
            year += 1900 if year >= 50 else 2000
        return f"{int(mo):02d}/{int(d):02d}/{year:04d}"

    # Month name formats, e.g. March 13, 1976 or Mar 13, 1976.
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            parsed = dt.datetime.strptime(s.replace(".", ""), fmt)
            return parsed.strftime("%m/%d/%Y")
        except ValueError:
            continue

    return s


def best_athlete_dict(next_data: Dict[str, Any], player_id: str) -> Optional[Dict[str, Any]]:
    best: Optional[Tuple[int, Dict[str, Any]]] = None
    for d in iter_dicts(next_data):
        score = 0
        if str(d.get("id", "")) == player_id:
            score += 5
        if d.get("displayName") or d.get("fullName"):
            score += 2
        if d.get("firstName") or d.get("lastName"):
            score += 2
        if d.get("college") or d.get("dateOfBirth"):
            score += 1
        if score == 0:
            continue
        if best is None or score > best[0]:
            best = (score, d)
    return best[1] if best else None


def extract_college(athlete: Dict[str, Any], soup: BeautifulSoup) -> str:
    college = athlete.get("college")
    if isinstance(college, dict):
        return str(college.get("displayName") or college.get("name") or "").strip()
    if isinstance(college, str):
        return college.strip()

    for script in soup.find_all("script", type="application/ld+json"):
        raw = script.string or script.get_text(strip=False)
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError:
            continue
        for d in iter_dicts(obj):
            alumni = d.get("alumniOf")
            if isinstance(alumni, dict):
                name = str(alumni.get("name") or "").strip()
                if name:
                    return name
            if isinstance(alumni, str) and alumni.strip():
                return alumni.strip()

    text = soup.get_text(" ", strip=True)
    m = re.search(
        r"\bCollege\s*:?\s*([A-Za-z0-9\.\- '&]+?)(?=\b(?:Draft|Status|Position|Experience|DOB|Born|Birthdate)\b|$)",
        text,
        re.IGNORECASE,
    )
    return m.group(1).strip() if m else ""


def extract_dob(athlete: Dict[str, Any], soup: BeautifulSoup) -> str:
    dob = str(athlete.get("dateOfBirth") or athlete.get("birthDate") or "").strip()
    if dob:
        return normalize_dob(dob)

    for script in soup.find_all("script", type="application/ld+json"):
        raw = script.string or script.get_text(strip=False)
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError:
            continue
        for d in iter_dicts(obj):
            birth = str(d.get("birthDate") or "").strip()
            if birth:
                return normalize_dob(birth)

    text = soup.get_text(" ", strip=True)
    m = DOB_RE.search(text)
    return normalize_dob(m.group(1).strip()) if m else ""


def extract_season_bounds(
    athlete: Dict[str, Any], next_data: Optional[Dict[str, Any]], soup: BeautifulSoup, dob: str
) -> Tuple[str, str]:
    direct_candidates = [
        athlete.get("debutYear"),
        athlete.get("firstYear"),
        athlete.get("startYear"),
        athlete.get("rookieYear"),
        athlete.get("lastYear"),
        athlete.get("endYear"),
    ]
    years = [to_year(v) for v in direct_candidates if to_year(v) is not None]

    if next_data:
        for d in iter_dicts(next_data):
            for key, value in d.items():
                lower = str(key).lower()
                if ("season" in lower or "year" in lower) and "birth" not in lower:
                    y = to_year(value)
                    if y is not None:
                        years.append(y)

    text = soup.get_text(" ", strip=True)
    text_years = [int(y) for y in YEAR_RE.findall(text)]
    dob_year = to_year(dob)
    current_year = time.gmtime().tm_year
    filtered_text_years = [
        y
        for y in text_years
        if 1960 <= y <= (current_year + 1) and (dob_year is None or y >= (dob_year + 18))
    ]
    years.extend(filtered_text_years)

    draft_match = re.search(r"\bDraft(?:\s+Info)?\b.*?\b(19\d{2}|20\d{2})\b", text, re.IGNORECASE)
    draft_year = int(draft_match.group(1)) if draft_match else None

    if not years:
        return "", ""

    first = min(years)
    last = max(years)
    if draft_year is not None and (first < 1960 or draft_year > first):
        first = draft_year
    return str(first), str(last)


def extract_names(athlete: Dict[str, Any], soup: BeautifulSoup) -> Tuple[str, str, str]:
    first = str(athlete.get("firstName") or "").strip()
    last = str(athlete.get("lastName") or "").strip()
    full = str(athlete.get("displayName") or athlete.get("fullName") or "").strip()

    if not full:
        h1 = soup.find("h1")
        if h1:
            full = h1.get_text(" ", strip=True)
    if not first or not last:
        parts = full.split()
        if parts:
            if not first:
                first = parts[0]
            if not last and len(parts) > 1:
                last = " ".join(parts[1:])

    return first, last, full


def extract_draft_info(
    athlete: Dict[str, Any], next_data: Optional[Dict[str, Any]], soup: BeautifulSoup
) -> Tuple[str, str]:
    round_val = ""
    overall_val = ""

    def as_int_str(v: Any) -> str:
        if v is None:
            return ""
        s = str(v).strip()
        return s if s.isdigit() else ""

    def parse_from_text(text: str) -> Tuple[str, str]:
        draft_round = ""
        draft_overall = ""
        m_round = re.search(r"\bRound\s*(\d+)\b", text, re.IGNORECASE)
        if m_round:
            draft_round = m_round.group(1)
        m_overall = re.search(r"\b(?:No\.?|#)\s*(\d+)\s*overall\b", text, re.IGNORECASE)
        if not m_overall:
            m_overall = re.search(r"\((\d+)\s*overall\)", text, re.IGNORECASE)
        if not m_overall:
            m_overall = re.search(r"\b(?:Pick|Pk)\s*:?\s*(\d+)\b", text, re.IGNORECASE)
        if m_overall:
            draft_overall = m_overall.group(1)
        return draft_round, draft_overall

    draft = athlete.get("draft")
    if isinstance(draft, dict):
        round_val = as_int_str(
            draft.get("round") or draft.get("roundNumber") or draft.get("draftRound")
        )
        overall_val = as_int_str(
            draft.get("overall")
            or draft.get("overallPick")
            or draft.get("pick")
            or draft.get("draftPick")
        )
        text_bits = " ".join(
            str(draft.get(k) or "")
            for k in ("displayText", "description", "text", "summary")
            if draft.get(k)
        )
        if text_bits and (not round_val or not overall_val):
            t_round, t_overall = parse_from_text(text_bits)
            if not round_val:
                round_val = t_round
            if not overall_val:
                overall_val = t_overall

    if not round_val:
        round_val = as_int_str(
            athlete.get("draftRound")
            or athlete.get("round")
            or athlete.get("draft_round")
            or athlete.get("roundNumber")
        )
    if not overall_val:
        overall_val = as_int_str(
            athlete.get("draftOverall")
            or athlete.get("draftPick")
            or athlete.get("pick")
            or athlete.get("overallPick")
        )

    if next_data and (not round_val or not overall_val):
        for d in iter_dicts(next_data):
            joined = " ".join(str(d.get(k) or "") for k in d.keys())
            if "draft" not in joined.lower():
                continue
            if not round_val:
                round_val = as_int_str(d.get("round") or d.get("draftRound") or d.get("roundNumber"))
            if not overall_val:
                overall_val = as_int_str(
                    d.get("overall") or d.get("overallPick") or d.get("pick") or d.get("draftPick")
                )
            if round_val and overall_val:
                break

    if not round_val or not overall_val:
        text = soup.get_text(" ", strip=True)
        m_draft = re.search(
            r"\bDraft(?:\s+Info)?\b[:\s]*(.{0,160})",
            text,
            flags=re.IGNORECASE,
        )
        if m_draft:
            t_round, t_overall = parse_from_text(m_draft.group(1))
            if not round_val:
                round_val = t_round
            if not overall_val:
                overall_val = t_overall

    return round_val, overall_val


def extract_player_profile(session: requests.Session, player_url: str) -> Optional[Dict[str, str]]:
    html_text = get_html(session, player_url)
    if not html_text:
        return None

    soup = BeautifulSoup(html_text, "html.parser")
    player_id_match = re.search(r"/id/(\d+)", player_url)
    player_id = player_id_match.group(1) if player_id_match else ""

    next_data = parse_next_data(soup, html_text=html_text)
    athlete: Dict[str, Any] = {}
    if next_data:
        athlete = best_athlete_dict(next_data, player_id) or {}

    first_name, last_name, full_name = extract_names(athlete, soup)
    dob = extract_dob(athlete, soup)
    college = extract_college(athlete, soup)
    first_season, last_season = extract_season_bounds(athlete, next_data, soup, dob)
    draft_round, draft_overall = extract_draft_info(athlete, next_data, soup)
    player_image = (
        f"https://a.espncdn.com/combiner/i?img=/i/headshots/nfl/players/full/{player_id}.png&w=350&h=254"
        if player_id
        else ""
    )

    return {
        "player_url": player_url,
        "first_name": first_name,
        "last_name": last_name,
        "full_name": full_name,
        "dob": dob,
        "college": college,
        "first_season": first_season,
        "last_season": last_season,
        "draft_round": draft_round,
        "draft_overall": draft_overall,
        "player_image": player_image,
    }


def write_links_csv(links: List[str], path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["player_url"])
        for link in links:
            writer.writerow([link])


def write_profiles_csv(rows: List[Dict[str, str]], path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fieldnames = [
        "player_url",
        "first_name",
        "last_name",
        "full_name",
        "dob",
        "college",
        "first_season",
        "last_season",
        "draft_round",
        "draft_overall",
        "player_image",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def run(
    start_year: int = 2008,
    end_year: int = 2025,
    links_csv: str = "output/espn_nfl_player_links_2008_2025.csv",
    profiles_csv: str = "output/espn_nfl_player_profiles_2008_2025.csv",
    delay_seconds: float = 0.4,
) -> None:
    run_start = time.time()
    session = build_session()
    print("Session initialized.")
    links = collect_unique_player_links(session, start_year, end_year, delay_seconds)
    write_links_csv(links, links_csv)
    print(f"Wrote links CSV: {links_csv}")

    rows: List[Dict[str, str]] = []
    missing_core = 0
    failed_profiles = 0
    profile_start = time.time()
    print(f"Starting profile scrape for {len(links)} players...")
    for idx, link in enumerate(links, start=1):
        if idx <= 10 or idx % 100 == 0:
            print(f"[profiles] {idx}/{len(links)} -> {link}")
        data = extract_player_profile(session, link)
        if data:
            rows.append(data)
            if not all(
                data.get(k)
                for k in (
                    "dob",
                    "college",
                    "first_season",
                    "last_season",
                    "draft_round",
                    "draft_overall",
                    "player_image",
                )
            ):
                missing_core += 1
                if idx <= 10 or idx % 100 == 0:
                    print(
                        f"[profiles] warning: partial profile for {link} "
                        f"(core-missing count={missing_core})"
                    )
        else:
            failed_profiles += 1
            print(
                f"[profiles] warning: failed to fetch/parse {link} "
                f"(failed count={failed_profiles})"
            )

        if idx % 100 == 0 or idx == len(links):
            elapsed = max(time.time() - profile_start, 0.001)
            rate = idx / elapsed
            eta_seconds = int((len(links) - idx) / rate) if rate > 0 else 0
            print(
                f"[profiles] progress {idx}/{len(links)} | "
                f"rows={len(rows)} failed={failed_profiles} partial={missing_core} "
                f"rate={rate:.2f}/s eta~{eta_seconds}s"
            )
        time.sleep(delay_seconds)

    write_profiles_csv(rows, profiles_csv)
    print(f"Wrote {len(rows)} player profiles to {profiles_csv}")
    print(
        "Run summary: "
        f"links={len(links)} profiles={len(rows)} failed={failed_profiles} partial={missing_core} "
        f"total_time={round(time.time() - run_start, 1)}s"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Scrape ESPN NFL player links and profile fields")
    parser.add_argument("--start-year", type=int, default=2008)
    parser.add_argument("--end-year", type=int, default=2025)
    parser.add_argument("--links-csv", default="output/espn_nfl_player_links_2008_2025.csv")
    parser.add_argument("--profiles-csv", default="output/espn_nfl_player_profiles_2008_2025.csv")
    parser.add_argument("--delay-seconds", type=float, default=0.4)
    args = parser.parse_args()

    run(
        start_year=args.start_year,
        end_year=args.end_year,
        links_csv=args.links_csv,
        profiles_csv=args.profiles_csv,
        delay_seconds=args.delay_seconds,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
