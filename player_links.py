import argparse
import csv
import html
import os
import re
import string
import time
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup, Comment
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

BASE_URL = "https://www.pro-football-reference.com"
ALLOWED_POSITIONS = {"RB", "FB", "HB", "WR", "TE", "QB"}
DEFAULT_CROSS_ENTRIES_CSV = "output/cross_entries.csv"

ANCHOR_RE = re.compile(r'<a[^>]+href=["\']([^"\']+\.htm)["\'][^>]*>.*?</a>', re.IGNORECASE | re.DOTALL)
YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2})\b")
POS_RE = re.compile(r"\b(RB|FB|HB|WR|TE|QB)\b", re.IGNORECASE)
DEFAULT_OFFLINE_DIR = "player_links"
DEFAULT_PROFILE_DIR = str(Path(__file__).parent / ".selenium_profile_links")


def build_driver(profile_dir: str = DEFAULT_PROFILE_DIR):
    options = webdriver.ChromeOptions()
    if os.getenv("HEADLESS", "0") == "1":
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    profile_path = Path(profile_dir)
    profile_path.mkdir(parents=True, exist_ok=True)
    options.add_argument(f"--user-data-dir={profile_path}")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return driver


def save_letter_html(offline_dir: str, letter: str, html_text: str) -> Path:
    output_dir = Path(offline_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{letter}.html"
    output_path.write_text(html_text, encoding="utf-8")
    return output_path


def clean_text(value: str) -> str:
    value = re.sub(r"<[^>]+>", " ", value)
    value = html.unescape(value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def detect_challenge(page_source: str, title: str) -> bool:
    t = (title or "").lower()
    body = (page_source or "").lower()
    return "security verification" in body or "just a moment" in t


def wait_for_challenge_clear(
    driver,
    timeout_seconds: float = 120,
    stable_seconds: float = 2,
) -> bool:
    def challenge_cleared(_driver) -> bool:
        page_source = _driver.page_source or ""
        title = _driver.title or ""
        if detect_challenge(page_source, title):
            return False
        if _driver.find_elements(By.ID, "div_players"):
            return True
        if "/players/" in page_source:
            return True
        return False

    try:
        WebDriverWait(driver, timeout_seconds, poll_frequency=1).until(challenge_cleared)
        time.sleep(stable_seconds)
        return True
    except Exception:
        return False


def prompt_challenge_once(driver, state: Dict[str, bool], reason: str):
    if state.get("challenge_prompted"):
        return
    print(
        f"Cloudflare challenge detected ({reason}). "
        "Solve it in the browser; the script will continue automatically when the page clears."
    )
    if not wait_for_challenge_clear(driver):
        print("Timed out waiting for challenge to clear. Press Enter after the player index page is visible.")
        input()
        time.sleep(2)
    state["challenge_prompted"] = True


def normalize_player_path(href: str) -> Optional[tuple[str, str]]:
    raw = (href or "").strip()
    if not raw:
        return None

    path = urlparse(raw).path if "://" in raw else raw
    m = re.match(r"^/players/([A-Z])/([A-Za-z0-9.]+)\.htm$", path, re.IGNORECASE)
    if not m:
        return None
    return path, m.group(1).upper()


def extract_candidates_from_html(
    html_text: str,
    letter: Optional[str] = None,
    cross_entries: Optional[List[Dict[str, str]]] = None,
):
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
        if letter and row_letter != letter.upper() and cross_entries is not None:
            cross_entries.append(
                {
                    "page_letter": letter.upper(),
                    "player_letter": row_letter,
                    "link": f"{BASE_URL}{path}",
                    "context": context_text,
                }
            )
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
            normalized = normalize_player_path(a_tag["href"])
            if not normalized:
                continue
            path, row_letter = normalized
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
            normalized = normalize_player_path(m.group(1))
            if not normalized:
                continue
            path, row_letter = normalized
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


def scrape_letter(
    driver,
    letter,
    state,
    cross_entries: Optional[List[Dict[str, str]]] = None,
    offline_dir: Optional[str] = None,
):
    url = f"{BASE_URL}/players/{letter}/"
    driver.get(url)
    time.sleep(4)

    if detect_challenge(driver.page_source, driver.title):
        prompt_challenge_once(driver, state, reason=f"letter {letter}")
        driver.get(url)
        time.sleep(2)

    page_source = driver.page_source
    if offline_dir:
        saved_path = save_letter_html(offline_dir, letter, page_source)
        print(f"Saved letter {letter} HTML to {saved_path}")

    candidates = extract_candidates_from_html(page_source, letter=letter, cross_entries=cross_entries)
    links = filter_candidates(candidates)

    if not links:
        print(
            f"No qualifying links for letter {letter}. "
            f"Title: {driver.title.strip() or 'N/A'} Candidates: {len(candidates)}"
        )

    return links


def get_all_player_links(
    delay_seconds: float = 10,
    cross_entries: Optional[List[Dict[str, str]]] = None,
    profile_dir: str = DEFAULT_PROFILE_DIR,
    save_pages_dir: Optional[str] = None,
):
    all_links = []
    seen = set()
    state = {"challenge_prompted": False}
    driver = build_driver(profile_dir=profile_dir)

    try:
        for idx, letter in enumerate(string.ascii_uppercase):
            print(f"Scraping letter {letter}...")
            links = scrape_letter(
                driver,
                letter,
                state,
                cross_entries=cross_entries,
                offline_dir=save_pages_dir,
            )
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


def resolve_offline_letter_path(offline_dir: str, letter: str) -> Path:
    base = Path(offline_dir)
    if not base.exists():
        raise FileNotFoundError(f"Offline directory not found: {offline_dir}")

    direct = base / f"{letter}.html"
    if direct.exists():
        return direct

    pattern = f"*Starting with {letter} *Pro-Football-Reference.com.html"
    matches = sorted(path for path in base.glob(pattern) if path.is_file())
    if matches:
        return matches[0]

    raise FileNotFoundError(f"Missing saved HTML page for letter {letter} in {offline_dir}")


def get_all_player_links_offline(
    offline_dir: str = DEFAULT_OFFLINE_DIR,
    cross_entries: Optional[List[Dict[str, str]]] = None,
) -> List[str]:
    all_links: List[str] = []
    seen = set()

    for letter in string.ascii_uppercase:
        html_path = resolve_offline_letter_path(offline_dir, letter)
        print(f"Parsing offline letter {letter} from {html_path}")
        html_text = html_path.read_text(encoding="utf-8")
        candidates = extract_candidates_from_html(html_text, letter=letter, cross_entries=cross_entries)
        links = filter_candidates(candidates)
        new_count = 0
        for link in links:
            if link in seen:
                continue
            seen.add(link)
            all_links.append(link)
            new_count += 1
        print(f"Finished offline letter {letter}: {new_count} links")

    return all_links


def write_cross_entries(csv_path: str, cross_entries: List[Dict[str, str]]) -> None:
    seen = set()
    rows: List[Dict[str, str]] = []
    for row in cross_entries:
        key = (row["page_letter"], row["player_letter"], row["link"])
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)

    output_dir = os.path.dirname(csv_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["page_letter", "player_letter", "link", "context"],
        )
        writer.writeheader()
        writer.writerows(rows)


def run(
    output_path="output/player_links.csv",
    offline: bool = False,
    offline_dir: str = DEFAULT_OFFLINE_DIR,
    cross_entries_path: str = DEFAULT_CROSS_ENTRIES_CSV,
    delay_seconds: float = 10,
    profile_dir: str = DEFAULT_PROFILE_DIR,
    save_pages_dir: Optional[str] = None,
):
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    cross_entries: List[Dict[str, str]] = []
    if offline:
        links = get_all_player_links_offline(offline_dir=offline_dir, cross_entries=cross_entries)
    else:
        links = get_all_player_links(
            delay_seconds=delay_seconds,
            cross_entries=cross_entries,
            profile_dir=profile_dir,
            save_pages_dir=save_pages_dir,
        )

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for link in links:
            writer.writerow([link])

    write_cross_entries(cross_entries_path, cross_entries)
    print(f"Successfully wrote {len(links)} player links to {output_path}")
    print(f"Logged {len({(r['page_letter'], r['player_letter'], r['link']) for r in cross_entries})} cross entries to {cross_entries_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape PFR player links with position/year filters")
    parser.add_argument("--output", default="output/player_links.csv", help="CSV output path")
    parser.add_argument("--offline", action="store_true", help="Read saved letter pages from disk instead of scraping")
    parser.add_argument("--offline-dir", default=DEFAULT_OFFLINE_DIR, help="Directory containing saved letter HTML pages")
    parser.add_argument("--cross-entries-output", default=DEFAULT_CROSS_ENTRIES_CSV, help="CSV path for cross-letter entries")
    parser.add_argument("--delay-seconds", type=float, default=10, help="Delay between letter pages when scraping live")
    parser.add_argument("--profile-dir", default=DEFAULT_PROFILE_DIR, help="Chrome user-data directory to persist session state")
    parser.add_argument("--save-pages-dir", help="Directory to save fetched letter HTML pages for later offline runs")
    args = parser.parse_args()

    run(
        output_path=args.output,
        offline=args.offline,
        offline_dir=args.offline_dir,
        cross_entries_path=args.cross_entries_output,
        delay_seconds=args.delay_seconds,
        profile_dir=args.profile_dir,
        save_pages_dir=args.save_pages_dir,
    )
