#!/usr/bin/env python3
"""Iterate player links and generate one CSV per player via FlareSolverr-backed get_bref_stats."""

from __future__ import annotations

import argparse
import csv
import os
import signal
import subprocess
import time
from pathlib import Path
from typing import List
from urllib.parse import urlparse

import requests

from export_player_stats_csv import ensure_nfl_schema, upload_player_stats_nfl_rows
from get_bref_stats_flare import (
    DEFAULT_FLARESOLVERR_ENDPOINT,
    DEFAULT_MAX_TIMEOUT_MS,
    DEFAULT_RETRY_ATTEMPTS,
    DEFAULT_RETRY_DELAY_SECONDS,
    DEFAULT_WAIT_SECONDS,
    run as run_bref_stats,
)


DEFAULT_LINKS_CSV = "output/player_links.csv"
DEFAULT_OUT_DIR = "out"
FLARESOLVERR_ROOT = Path(__file__).resolve().parent.parent / "FlareSolverr"
FLARESOLVERR_RUNTIME_HOME = FLARESOLVERR_ROOT / ".runtime"
FLARESOLVERR_VENV_PYTHON = FLARESOLVERR_ROOT / ".venv" / "bin" / "python"
FLARESOLVERR_STARTUP_TIMEOUT_SECONDS = 45


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


def _flaresolverr_health_url(endpoint: str) -> str:
    base = endpoint.rsplit("/v1", 1)[0] if endpoint.endswith("/v1") else endpoint.rstrip("/")
    return f"{base}/"


def _is_flaresolverr_ready(endpoint: str) -> bool:
    try:
        response = requests.get(_flaresolverr_health_url(endpoint), timeout=2)
        response.raise_for_status()
        body = response.json()
        return body.get("msg") == "FlareSolverr is ready!"
    except Exception:
        return False


def _start_flaresolverr(endpoint: str) -> subprocess.Popen[str]:
    if not FLARESOLVERR_VENV_PYTHON.exists():
        raise RuntimeError(
            f"FlareSolverr virtualenv not found at {FLARESOLVERR_VENV_PYTHON}. "
            "Set up FlareSolverr before using main_flare.py."
        )

    FLARESOLVERR_RUNTIME_HOME.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["HOME"] = str(FLARESOLVERR_RUNTIME_HOME)
    env["HEADLESS"] = "false"

    process = subprocess.Popen(
        [str(FLARESOLVERR_VENV_PYTHON), "src/flaresolverr.py"],
        cwd=str(FLARESOLVERR_ROOT),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
        start_new_session=True,
    )

    deadline = time.time() + FLARESOLVERR_STARTUP_TIMEOUT_SECONDS
    while time.time() < deadline:
        if process.poll() is not None:
            raise RuntimeError("FlareSolverr exited before becoming ready.")
        if _is_flaresolverr_ready(endpoint):
            return process
        time.sleep(1)

    _stop_flaresolverr(process)
    raise RuntimeError("Timed out waiting for FlareSolverr to become ready.")


def _stop_flaresolverr(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.time() + 10
    while time.time() < deadline:
        if process.poll() is not None:
            return
        time.sleep(0.25)
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def run(
    links_csv: str = DEFAULT_LINKS_CSV,
    out_dir: str = DEFAULT_OUT_DIR,
    delay_seconds: float = 0.0,
    upload: bool = True,
    max_players: int = 0,
    *,
    flaresolverr_url: str = DEFAULT_FLARESOLVERR_ENDPOINT,
    flaresolverr_session: str = "",
    max_timeout_ms: int = DEFAULT_MAX_TIMEOUT_MS,
    wait_seconds: int = DEFAULT_WAIT_SECONDS,
    retry_attempts: int = DEFAULT_RETRY_ATTEMPTS,
    retry_delay_seconds: int = DEFAULT_RETRY_DELAY_SECONDS,
) -> None:
    flaresolverr_process: subprocess.Popen[str] | None = None
    started_flaresolverr = False

    if not _is_flaresolverr_ready(flaresolverr_url):
        print("Starting FlareSolverr...")
        flaresolverr_process = _start_flaresolverr(flaresolverr_url)
        started_flaresolverr = True
        print("FlareSolverr ready.")
    else:
        print("Using existing FlareSolverr server.")

    os.makedirs(out_dir, exist_ok=True)
    links = read_player_links(links_csv)
    if max_players > 0:
        links = links[:max_players]
    print(f"Loaded {len(links)} player links from {links_csv}")
    try:
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
                print(f"[{idx}/{len(links)}] {pid}: scraping via FlareSolverr...")
                t0 = time.time()
                rows = run_bref_stats(
                    link,
                    csv_path=out_csv,
                    write_csv=True,
                    endpoint=flaresolverr_url,
                    session_id=flaresolverr_session or None,
                    max_timeout_ms=max_timeout_ms,
                    wait_seconds=wait_seconds,
                    retry_attempts=retry_attempts,
                    retry_delay_seconds=retry_delay_seconds,
                )
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
    finally:
        if started_flaresolverr and flaresolverr_process is not None:
            print("Stopping FlareSolverr...")
            _stop_flaresolverr(flaresolverr_process)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate per-player CSV files from player_links.csv via FlareSolverr")
    parser.add_argument("--links-csv", default=DEFAULT_LINKS_CSV, help="Input CSV of player links")
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR, help="Output directory for per-player CSV files")
    parser.add_argument("--delay-seconds", type=float, default=0.0, help="Optional delay between players")
    parser.add_argument("--no-upload", action="store_true", help="Skip DB upload and only write per-player CSVs")
    parser.add_argument("--max-players", type=int, default=0, help="Process only first N players (0 = all)")
    parser.add_argument(
        "--flaresolverr-url",
        default=DEFAULT_FLARESOLVERR_ENDPOINT,
        help="FlareSolverr API endpoint",
    )
    parser.add_argument(
        "--flaresolverr-session",
        default="",
        help="Optional existing FlareSolverr session id",
    )
    parser.add_argument(
        "--max-timeout-ms",
        type=int,
        default=DEFAULT_MAX_TIMEOUT_MS,
        help="FlareSolverr maxTimeout value in milliseconds",
    )
    parser.add_argument(
        "--wait-seconds",
        type=int,
        default=DEFAULT_WAIT_SECONDS,
        help="FlareSolverr waitInSeconds value",
    )
    parser.add_argument(
        "--retry-attempts",
        type=int,
        default=DEFAULT_RETRY_ATTEMPTS,
        help="Retry count for FlareSolverr transport or 5xx failures",
    )
    parser.add_argument(
        "--retry-delay-seconds",
        type=int,
        default=DEFAULT_RETRY_DELAY_SECONDS,
        help="Delay between FlareSolverr retries",
    )
    args = parser.parse_args()

    run(
        links_csv=args.links_csv,
        out_dir=args.out_dir,
        delay_seconds=args.delay_seconds,
        upload=not args.no_upload,
        max_players=args.max_players,
        flaresolverr_url=args.flaresolverr_url,
        flaresolverr_session=args.flaresolverr_session,
        max_timeout_ms=args.max_timeout_ms,
        wait_seconds=args.wait_seconds,
        retry_attempts=args.retry_attempts,
        retry_delay_seconds=args.retry_delay_seconds,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
