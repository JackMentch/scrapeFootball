#!/usr/bin/env python3
"""Fetch Pro-Football-Reference player data and write JSON + table CSV files."""

from __future__ import annotations

import argparse
import csv
import html
import json
import os
import re
import time
from pathlib import Path
from typing import Dict, List, Tuple

import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

COMMENT_RE = re.compile(r"<!--|-->")
TABLE_RE = re.compile(
    r"<table[^>]*\\bid=(?:\"([^\"]+)\"|'([^']+)')[^>]*>(.*?)</table>",
    re.DOTALL | re.IGNORECASE,
)
CAPTION_RE = re.compile(r"<caption[^>]*>(.*?)</caption>", re.DOTALL | re.IGNORECASE)
ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.DOTALL | re.IGNORECASE)
CELL_STAT_RE = re.compile(
    r'<(t[hd])[^>]*data-stat="([^"]+)"[^>]*>(.*?)</t[hd]>',
    re.DOTALL | re.IGNORECASE,
)

DEFAULT_URL = "https://www.pro-football-reference.com/players/A/AvilSt00.htm"
DEFAULT_OUTPUT_DIR = Path("output/player_data")


def _clean_text(value: str) -> str:
    value = re.sub(r"<[^>]+>", "", value)
    value = html.unescape(value)
    value = re.sub(r"\\s+", " ", value).strip()
    return value


def _strip_comments(text: str) -> str:
    return COMMENT_RE.sub("", text)


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
        resp = session.get(
            url,
            timeout=30,
            headers={"Referer": "https://www.pro-football-reference.com/"},
        )
        if resp.status_code != 403:
            resp.raise_for_status()
            return resp.text
        print("curl_cffi chrome110 received 403; trying other backends...")

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
        resp = session.get(
            url,
            timeout=30,
            headers={"Referer": "https://www.pro-football-reference.com/"},
        )

    if resp.status_code == 403:
        print("HTTP 403 detected. Falling back to Selenium browser fetch...")
        return _fetch_html_via_browser(url)

    resp.raise_for_status()
    return resp.text


def _build_driver() -> webdriver.Chrome:
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
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
    driver = webdriver.Chrome(options=options)
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"},
    )
    return driver


def _fetch_html_via_browser(url: str) -> str:
    driver = _build_driver()
    try:
        driver.get(url)
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        page_text = (driver.page_source or "").lower()
        title = (driver.title or "").lower()
        if "security verification" in page_text or "just a moment" in title:
            print(
                "Cloudflare challenge detected in browser. Complete verification, "
                "wait for player page content, then press Enter."
            )
            input()
            time.sleep(2)
        return driver.page_source
    finally:
        driver.quit()


def _parse_table(table_html: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for row_html in ROW_RE.findall(table_html):
        if 'class="thead"' in row_html or "over_header" in row_html:
            continue

        row: Dict[str, str] = {}
        for _, stat, cell in CELL_STAT_RE.findall(row_html):
            row[stat] = _clean_text(cell)

        if not row:
            continue
        rows.append(row)

    return rows


def _extract_player_meta(html_text: str, url: str) -> Dict[str, str]:
    meta: Dict[str, str] = {}

    name_match = re.search(r'<h1[^>]*itemprop="name"[^>]*>(.*?)</h1>', html_text, re.I | re.S)
    if name_match:
        meta["name"] = _clean_text(name_match.group(1))

    dob_match = re.search(r'data-birth="(\\d{4}-\\d{2}-\\d{2})"', html_text, re.I)
    if dob_match:
        meta["birth_date"] = dob_match.group(1)

    position_match = re.search(r"Position:\\s*</strong>\\s*([^<]+)", html_text, re.I)
    if position_match:
        meta["position"] = _clean_text(position_match.group(1))

    throws_match = re.search(r"Throws:\\s*</strong>\\s*([^<]+)", html_text, re.I)
    if throws_match:
        meta["throws"] = _clean_text(throws_match.group(1))

    shoots_match = re.search(r"Shoots:\\s*</strong>\\s*([^<]+)", html_text, re.I)
    if shoots_match:
        meta["shoots"] = _clean_text(shoots_match.group(1))

    height_weight_match = re.search(r"(\\d-\\d|\\d{2}-\\d)\\s*,\\s*(\\d+lb)", html_text, re.I)
    if height_weight_match:
        meta["height"] = height_weight_match.group(1)
        meta["weight"] = height_weight_match.group(2)

    image_match = re.search(r'<img[^>]*src="([^"]+)"[^>]*>', html_text, re.I)
    if image_match:
        meta["image_url"] = image_match.group(1)

    meta["player_url"] = url
    meta["player_id"] = url.rstrip("/").split("/")[-1].replace(".htm", "")
    return meta


def get_player_tables(url: str) -> Tuple[Dict[str, str], List[Tuple[str, str, List[Dict[str, str]]]]]:
    html_text = _strip_comments(_fetch_html(url))
    meta = _extract_player_meta(html_text, url)

    tables: List[Tuple[str, str, List[Dict[str, str]]]] = []
    for table_id_a, table_id_b, table_html in TABLE_RE.findall(html_text):
        table_id = table_id_a or table_id_b
        caption_match = CAPTION_RE.search(table_html)
        caption = _clean_text(caption_match.group(1)) if caption_match else table_id
        rows = _parse_table(table_html)
        if rows:
            tables.append((table_id, caption, rows))

    return meta, tables


def _write_table_csv(rows: List[Dict[str, str]], csv_path: Path) -> None:
    if not rows:
        return

    fieldnames: List[str] = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def run(url: str, output_dir: Path = DEFAULT_OUTPUT_DIR, write_csv: bool = True) -> Dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)

    meta, tables = get_player_tables(url)
    player_id = str(meta.get("player_id", "player"))

    data: Dict[str, object] = {
        "meta": meta,
        "table_count": len(tables),
        "tables": {
            table_id: {
                "caption": caption,
                "row_count": len(rows),
                "rows": rows,
            }
            for table_id, caption, rows in tables
        },
    }

    json_path = output_dir / f"{player_id}.json"
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    if write_csv:
        tables_dir = output_dir / f"{player_id}_tables"
        tables_dir.mkdir(parents=True, exist_ok=True)
        for table_id, _, rows in tables:
            _write_table_csv(rows, tables_dir / f"{table_id}.csv")

    print(f"Wrote JSON: {json_path}")
    if write_csv:
        print(f"Wrote table CSVs to: {output_dir / f'{player_id}_tables'}")
    print(f"Extracted {len(tables)} tables")

    return data


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch Pro-Football-Reference player data")
    parser.add_argument("--url", default=DEFAULT_URL, help="Player URL")
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory for JSON/table outputs",
    )
    parser.add_argument(
        "--no-csv",
        action="store_true",
        help="Do not write per-table CSV files",
    )
    args = parser.parse_args()

    run(args.url, output_dir=Path(args.output_dir), write_csv=not args.no_csv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
