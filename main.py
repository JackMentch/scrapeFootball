#!/usr/bin/env python3
"""Iterate player links and generate one CSV per player via get_bref_stats."""

from __future__ import annotations

import argparse
import csv
import os
import time
from typing import List
from urllib.parse import urlparse

from export_player_stats_csv import ensure_nfl_schema, upload_player_stats_nfl_rows
from get_bref_stats import run as run_bref_stats


DEFAULT_LINKS_CSV = "output/player_links.csv"
DEFAULT_OUT_DIR = "out"


def player_id_from_url(url: str) -> str:
    path = urlparse(url).path.rstrip("/")
    tail = path.split("/")[-1] if path else ""
    if tail.endswith(".htm"):
        return tail[:-4]
    return tail or "unknown_player"


def read_player_links(path: str) -> List[str]:
    links: List[str] = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            link = row[0].strip()
            if not link:
                continue
            links.append(link)
    return links


def run(
    links_csv: str = DEFAULT_LINKS_CSV,
    out_dir: str = DEFAULT_OUT_DIR,
    delay_seconds: float = 0.0,
    upload: bool = True,
    max_players: int = 0,
) -> None:
    os.makedirs(out_dir, exist_ok=True)
    links = read_player_links(links_csv)
    if max_players > 0:
        links = links[:max_players]
    print(f"Loaded {len(links)} player links from {links_csv}")
    if upload:
        print("Ensuring player_stats_nfl schema...")
        ensure_nfl_schema(
            os.environ.get(
                "DATABASE_URL",
                "postgresql://postgres:VSRhJUfegIZXrwEHDaDKOLgDJSFsMysl@switchback.proxy.rlwy.net:46735/railway",
            )
        )
        print("Schema ready.")

    total_inserted = 0
    total_updated = 0
    total_unchanged = 0
    total_errors = 0

    for idx, link in enumerate(links, start=1):
        pid = player_id_from_url(link)
        out_csv = os.path.join(out_dir, f"{pid}.csv")
        try:
            print(f"[{idx}/{len(links)}] {pid}: scraping...")
            t0 = time.time()
            rows = run_bref_stats(link, csv_path=out_csv, write_csv=True)
            scrape_seconds = round(time.time() - t0, 2)
            if upload:
                print(f"[{idx}/{len(links)}] {pid}: uploading {len(rows)} rows...")
                t1 = time.time()
                summary = upload_player_stats_nfl_rows(rows)
                total_inserted += summary["inserted"]
                total_updated += summary["updated"]
                total_unchanged += summary["unchanged"]
                upload_seconds = round(time.time() - t1, 2)
                print(
                    f"[{idx}/{len(links)}] {pid}: wrote {len(rows)} rows -> {out_csv} | "
                    f"db inserted={summary['inserted']} updated={summary['updated']} unchanged={summary['unchanged']} "
                    f"(scrape={scrape_seconds}s upload={upload_seconds}s)"
                )
            else:
                print(
                    f"[{idx}/{len(links)}] {pid}: wrote {len(rows)} rows -> {out_csv} "
                    f"(scrape={scrape_seconds}s, upload skipped)"
                )
        except Exception as exc:
            total_errors += 1
            print(f"[{idx}/{len(links)}] {pid}: ERROR {exc}")
        if delay_seconds > 0:
            time.sleep(delay_seconds)

    if upload:
        print(
            "DB summary totals: "
            f"inserted={total_inserted} updated={total_updated} "
            f"unchanged={total_unchanged} errors={total_errors}"
        )
    else:
        print(f"Run complete (upload skipped). errors={total_errors}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate per-player CSV files from player_links.csv")
    parser.add_argument("--links-csv", default=DEFAULT_LINKS_CSV, help="Input CSV of player links")
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR, help="Output directory for per-player CSV files")
    parser.add_argument("--delay-seconds", type=float, default=0.0, help="Optional delay between players")
    parser.add_argument("--no-upload", action="store_true", help="Skip DB upload and only write per-player CSVs")
    parser.add_argument("--max-players", type=int, default=0, help="Process only first N players (0 = all)")
    args = parser.parse_args()

    run(
        links_csv=args.links_csv,
        out_dir=args.out_dir,
        delay_seconds=args.delay_seconds,
        upload=not args.no_upload,
        max_players=args.max_players,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
