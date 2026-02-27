import argparse
import csv
import html
import os
import re
import string
import time
from typing import Dict, List, Optional

from bs4 import BeautifulSoup, Comment
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

BASE_URL = "https://www.pro-football-reference.com"
ALLOWED_POSITIONS = {"RB", "FB", "HB", "WR", "TE", "QB"}

ANCHOR_RE = re.compile(
    r'<a[^>]+href=["\'](/players/([A-Z])/[A-Za-z0-9]+\.htm)["\'][^>]*>.*?</a>',
    re.IGNORECASE | re.DOTALL,
)
YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2})\b")
POS_RE = re.compile(r"\b(RB|FB|HB|WR|TE|QB)\b", re.IGNORECASE)


def build_driver():
    options = webdriver.ChromeOptions()
    if os.getenv("HEADLESS", "0") == "1":
        options.add_argument("--headless=new")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return driver


def clean_text(value: str) -> str:
    value = re.sub(r"<[^>]+>", " ", value)
    value = html.unescape(value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def detect_challenge(page_source: str, title: str) -> bool:
    t = (title or "").lower()
    body = (page_source or "").lower()
    return "security verification" in body or "just a moment" in t


def prompt_challenge_once(driver, state: Dict[str, bool], reason: str):
    if state.get("challenge_prompted"):
        return
    print(f"Cloudflare challenge detected ({reason}). Solve it in browser, then press Enter.")
    input()
    time.sleep(2)
    state["challenge_prompted"] = True


def extract_candidates_from_html(html_text: str, letter: Optional[str] = None):
    def parse_end_year(text: str) -> Optional[int]:
        years = [int(y) for y in YEAR_RE.findall(text)]
        if not years:
            return None
        return max(years)

    def upsert_candidate(
        by_link: Dict[str, Dict[str, object]],
        path: str,
        row_letter: str,
        context_text: str,
    ) -> None:
        if letter and row_letter != letter.upper():
            return
        link = f"{BASE_URL}{path}"
        end_year = parse_end_year(context_text)
        pos_match = POS_RE.search(context_text)
        position = pos_match.group(1).upper() if pos_match else ""

        existing = by_link.get(link)
        if existing is None:
            by_link[link] = {
                "link": link,
                "end_year": end_year,
                "position": position,
                "context": context_text,
            }
            return
        if existing.get("end_year") is None and end_year is not None:
            existing["end_year"] = end_year
        if not existing.get("position") and position:
            existing["position"] = position

    by_link: Dict[str, Dict[str, object]] = {}

    soup = BeautifulSoup(html_text, "html.parser")

    def parse_player_rows(row_soup):
        for p_tag in row_soup.select("p"):
            a_tag = p_tag.find("a", href=True)
            if not a_tag:
                continue
            path = a_tag["href"]
            m = re.match(r"^/players/([A-Z])/([A-Za-z0-9]+)\.htm$", path, re.IGNORECASE)
            if not m:
                continue
            row_letter = m.group(1).upper()
            context_text = clean_text(p_tag.get_text(" ", strip=True)).upper()
            upsert_candidate(by_link, path, row_letter, context_text)

    players_div = soup.find("div", id="div_players")
    if players_div is not None:
        parse_player_rows(players_div)

    for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
        text = str(comment)
        if "/players/" not in text:
            continue
        comment_soup = BeautifulSoup(text, "html.parser")
        comment_div = comment_soup.find("div", id="div_players")
        if comment_div is not None:
            parse_player_rows(comment_div)
        else:
            parse_player_rows(comment_soup)

    # Last-resort fallback for unusual markup.
    if not by_link:
        for m in ANCHOR_RE.finditer(html_text):
            path = m.group(1)
            row_letter = m.group(2).upper()
            start = max(0, m.start() - 260)
            end = min(len(html_text), m.end() + 260)
            context_text = clean_text(html_text[start:end]).upper()
            upsert_candidate(by_link, path, row_letter, context_text)

    return list(by_link.values())


def filter_candidates(candidates):
    filtered = []
    for item in candidates:
        end_year = item.get("end_year")
        if end_year is None or int(end_year) < 1980:
            continue

        position = str(item.get("position") or "").upper()
        if position in ALLOWED_POSITIONS:
            filtered.append(item["link"])

    return filtered


def scrape_letter(driver, letter, state):
    url = f"{BASE_URL}/players/{letter}/"
    driver.get(url)
    time.sleep(4)

    if detect_challenge(driver.page_source, driver.title):
        prompt_challenge_once(driver, state, reason=f"letter {letter}")
        driver.get(url)
        time.sleep(2)

    candidates = extract_candidates_from_html(driver.page_source, letter=letter)
    links = filter_candidates(candidates)

    if not links:
        print(
            f"No qualifying links for letter {letter}. "
            f"Title: {driver.title.strip() or 'N/A'} Candidates: {len(candidates)}"
        )

    return links


def get_all_player_links(delay_seconds=10):
    all_links = []
    seen = set()
    state = {"challenge_prompted": False}
    driver = build_driver()

    try:
        for idx, letter in enumerate(string.ascii_uppercase):
            print(f"Scraping letter {letter}...")
            links = scrape_letter(driver, letter, state)
            new_count = 0
            for link in links:
                if link in seen:
                    continue
                seen.add(link)
                all_links.append(link)
                new_count += 1
            print(f"Finished letter {letter}: {new_count} links")
            if idx < len(string.ascii_uppercase) - 1:
                time.sleep(delay_seconds)
    finally:
        driver.quit()

    return all_links


def run(output_path="output/player_links.csv"):
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    links = get_all_player_links(delay_seconds=10)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for link in links:
            writer.writerow([link])

    print(f"Successfully wrote {len(links)} player links to {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape PFR player links with position/year filters")
    parser.add_argument("--output", default="output/player_links.csv", help="CSV output path")
    args = parser.parse_args()

    run(output_path=args.output)
