#!/usr/bin/env python3
"""Fetch Pro-Football-Reference player stats via a local FlareSolverr server."""

from __future__ import annotations

import argparse
import time
from typing import Dict, List, Optional

import requests

import get_bref_stats as base

DEFAULT_FLARESOLVERR_ENDPOINT = "http://127.0.0.1:8191/v1"
DEFAULT_MAX_TIMEOUT_MS = 120000
DEFAULT_WAIT_SECONDS = 2
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_DELAY_SECONDS = 2


def _fetch_html_via_flaresolverr(
    url: str,
    *,
    endpoint: str = DEFAULT_FLARESOLVERR_ENDPOINT,
    session_id: Optional[str] = None,
    max_timeout_ms: int = DEFAULT_MAX_TIMEOUT_MS,
    wait_seconds: int = DEFAULT_WAIT_SECONDS,
    retry_attempts: int = DEFAULT_RETRY_ATTEMPTS,
    retry_delay_seconds: int = DEFAULT_RETRY_DELAY_SECONDS,
) -> str:
    payload = {
        "cmd": "request.get",
        "url": url,
        "maxTimeout": max_timeout_ms,
        "waitInSeconds": wait_seconds,
    }
    if session_id:
        payload["session"] = session_id

    timeout = (10, (max_timeout_ms / 1000.0) + 30)
    attempts = max(1, retry_attempts)
    last_error: Optional[Exception] = None

    for attempt in range(1, attempts + 1):
        try:
            response = requests.post(endpoint, json=payload, timeout=timeout)
            response.raise_for_status()

            body = response.json()
            if body.get("status") != "ok":
                message = body.get("message") or "unknown FlareSolverr error"
                raise RuntimeError(f"FlareSolverr request failed: {message}")

            solution = body.get("solution") or {}
            status_code = solution.get("status")
            html_text = solution.get("response") or ""
            if status_code and int(status_code) >= 400:
                raise RuntimeError(f"FlareSolverr returned HTTP {status_code} for {url}")
            if not html_text:
                raise RuntimeError(f"FlareSolverr returned an empty response for {url}")
            return html_text
        except (requests.RequestException, ValueError, RuntimeError) as exc:
            last_error = exc
            if attempt == attempts:
                break
            time.sleep(retry_delay_seconds)

    assert last_error is not None
    raise last_error


def configure_flaresolverr(
    *,
    endpoint: str = DEFAULT_FLARESOLVERR_ENDPOINT,
    session_id: Optional[str] = None,
    max_timeout_ms: int = DEFAULT_MAX_TIMEOUT_MS,
    wait_seconds: int = DEFAULT_WAIT_SECONDS,
    retry_attempts: int = DEFAULT_RETRY_ATTEMPTS,
    retry_delay_seconds: int = DEFAULT_RETRY_DELAY_SECONDS,
) -> None:
    def _fetch(url: str) -> str:
        return _fetch_html_via_flaresolverr(
            url,
            endpoint=endpoint,
            session_id=session_id,
            max_timeout_ms=max_timeout_ms,
            wait_seconds=wait_seconds,
            retry_attempts=retry_attempts,
            retry_delay_seconds=retry_delay_seconds,
        )

    base._fetch_html = _fetch


OUTPUT_COLUMNS = base.OUTPUT_COLUMNS
DEFAULT_URL = base.DEFAULT_URL
DEFAULT_CSV = base.DEFAULT_CSV


def run(
    url: str,
    csv_path: str = DEFAULT_CSV,
    write_csv: bool = True,
    *,
    endpoint: str = DEFAULT_FLARESOLVERR_ENDPOINT,
    session_id: Optional[str] = None,
    max_timeout_ms: int = DEFAULT_MAX_TIMEOUT_MS,
    wait_seconds: int = DEFAULT_WAIT_SECONDS,
    retry_attempts: int = DEFAULT_RETRY_ATTEMPTS,
    retry_delay_seconds: int = DEFAULT_RETRY_DELAY_SECONDS,
) -> List[Dict[str, str]]:
    configure_flaresolverr(
        endpoint=endpoint,
        session_id=session_id,
        max_timeout_ms=max_timeout_ms,
        wait_seconds=wait_seconds,
        retry_attempts=retry_attempts,
        retry_delay_seconds=retry_delay_seconds,
    )
    return base.run(url, csv_path=csv_path, write_csv=write_csv)


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch Pro-Football-Reference player stats into CSV via FlareSolverr")
    parser.add_argument("--url", default=DEFAULT_URL, help="Player URL")
    parser.add_argument("--csv", default=DEFAULT_CSV, help="Output CSV path")
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

    rows = run(
        args.url,
        csv_path=args.csv,
        write_csv=True,
        endpoint=args.flaresolverr_url,
        session_id=args.flaresolverr_session or None,
        max_timeout_ms=args.max_timeout_ms,
        wait_seconds=args.wait_seconds,
        retry_attempts=args.retry_attempts,
        retry_delay_seconds=args.retry_delay_seconds,
    )
    print(f"Wrote {len(rows)} rows to {args.csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
