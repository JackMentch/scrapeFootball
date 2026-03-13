#!/usr/bin/env python3
"""Upload NFL player stats rows to player_stats_nfl with schema sync and dedupe."""

from __future__ import annotations

import csv
import json
import os
import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set

from get_bref_stats import OUTPUT_COLUMNS

TABLE_NAME = "player_stats_nfl"

BOOL_COLS: Set[str] = {
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
}

INT_COLS: Set[str] = {
    "season",
    "draft_round",
    "draft_overall",
    "pass_yds",
    "pass_td",
    "pass_int",
    "pass_lng",
    "rush_yds",
    "rush_lng",
    "rush_td",
    "rec",
    "rec_yds",
    "rec_td",
    "rec_lng",
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
    "playoff_pass_lng",
    "career_rush_yds",
    "career_rec",
    "career_rec_yds",
    "career_rush_td",
    "career_rec_td",
    "career_pass_yds",
    "career_pass_td",
}

FLOAT_COLS: Set[str] = {
    "qbr",
    "pass_rating",
    "cmp_pct",
    "rush_ypa",
    "rush_ypg",
    "rec_ypg",
    "playoff_pass_rating",
    "fan_pts",
    "fan_pts_ppr",
}

JSON_COLS: Set[str] = {"college"}


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _parse_bool(value: str) -> Optional[bool]:
    lowered = value.strip().lower()
    if lowered == "":
        return None
    if lowered in {"true", "t", "1", "yes", "y"}:
        return True
    if lowered in {"false", "f", "0", "no", "n"}:
        return False
    return None


def _parse_int(value: str) -> Optional[int]:
    cleaned = re.sub(r"[^0-9\-]", "", value.strip())
    if not cleaned or cleaned == "-":
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def _parse_float(value: str) -> Optional[float]:
    cleaned = re.sub(r"[^0-9\.\-]", "", value.strip())
    if not cleaned or cleaned in {"-", ".", "-."}:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_json_array(value: str) -> Optional[str]:
    raw = value.strip()
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return json.dumps(parsed)
        return None
    except json.JSONDecodeError:
        return None


def _convert_row(row: Dict[str, str]) -> Dict[str, Any]:
    converted: Dict[str, Any] = {}
    for key in OUTPUT_COLUMNS:
        raw = (row.get(key) or "").strip()
        if raw == "":
            converted[key] = None
            continue
        if key in BOOL_COLS:
            converted[key] = _parse_bool(raw)
        elif key in INT_COLS:
            converted[key] = _parse_int(raw)
        elif key in FLOAT_COLS:
            converted[key] = _parse_float(raw)
        elif key in JSON_COLS:
            converted[key] = _parse_json_array(raw)
        else:
            converted[key] = raw
    return converted


def _exec(database_url: str, sql: str, params: Optional[Sequence[Any]] = None) -> None:
    params = params or ()
    try:
        import psycopg  # type: ignore
    except Exception:
        psycopg = None  # type: ignore
    if psycopg is not None:
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
            conn.commit()
        return
    try:
        import psycopg2  # type: ignore
    except Exception:
        psycopg2 = None  # type: ignore
    if psycopg2 is not None:
        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
            conn.commit()
        return
    raise RuntimeError("psycopg/psycopg2 not installed; install one or use psql.")


def _fetch_table_columns(database_url: str, table_name: str) -> Optional[Set[str]]:
    query = (
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = %s"
    )
    try:
        import psycopg  # type: ignore
    except Exception:
        psycopg = None  # type: ignore
    if psycopg is not None:
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (table_name,))
                return {row[0] for row in cur.fetchall()}
    try:
        import psycopg2  # type: ignore
    except Exception:
        psycopg2 = None  # type: ignore
    if psycopg2 is not None:
        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (table_name,))
                return {row[0] for row in cur.fetchall()}
    return None


def _fetch_existing_rows_for_player(
    database_url: str, player_id: str, columns: List[str]
) -> Dict[tuple[str, str, str], Dict[str, Any]]:
    if not columns:
        return {}
    select_cols = ", ".join(_quote_ident(c) for c in columns)
    query = (
        f"SELECT {select_cols} FROM {TABLE_NAME} "
        "WHERE player_id = %s"
    )

    def _row_to_keyed_dict(desc_cols: List[str], row: Sequence[Any]) -> tuple[tuple[str, str, str], Dict[str, Any]]:
        data = {desc_cols[i]: row[i] for i in range(len(desc_cols))}
        key = (
            str(data.get("player_id") or ""),
            str(data.get("team") or ""),
            str(data.get("season") or ""),
        )
        return key, data

    try:
        import psycopg  # type: ignore
    except Exception:
        psycopg = None  # type: ignore
    if psycopg is not None:
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (player_id,))
                rows = cur.fetchall()
                desc_cols = [d.name for d in (cur.description or [])]
        out: Dict[tuple[str, str, str], Dict[str, Any]] = {}
        for row in rows:
            key, data = _row_to_keyed_dict(desc_cols, row)
            out[key] = data
        return out

    try:
        import psycopg2  # type: ignore
    except Exception:
        psycopg2 = None  # type: ignore
    if psycopg2 is not None:
        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (player_id,))
                rows = cur.fetchall()
                desc_cols = [d[0] for d in (cur.description or [])]
        out: Dict[tuple[str, str, str], Dict[str, Any]] = {}
        for row in rows:
            key, data = _row_to_keyed_dict(desc_cols, row)
            out[key] = data
        return out

    return {}


def _exec_many_returning(
    database_url: str, insert_sql: str, values: Iterable[Sequence[Any]]
) -> Optional[List[Dict[str, Any]]]:
    try:
        import psycopg  # type: ignore
    except Exception:
        psycopg = None  # type: ignore
    if psycopg is not None:
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                changed: List[Dict[str, Any]] = []
                for row_values in values:
                    cur.execute(insert_sql, list(row_values))
                    ret = cur.fetchone()
                    if ret is not None:
                        changed.append(
                            {
                                "player_id": ret[0],
                                "season": ret[1],
                                "team": ret[2],
                                "updated": ret[3],
                                "inserted": ret[4],
                            }
                        )
            conn.commit()
        return changed

    try:
        import psycopg2  # type: ignore
    except Exception:
        psycopg2 = None  # type: ignore
    if psycopg2 is not None:
        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cur:
                changed: List[Dict[str, Any]] = []
                for row_values in values:
                    cur.execute(insert_sql, list(row_values))
                    ret = cur.fetchone()
                    if ret is not None:
                        changed.append(
                            {
                                "player_id": ret[0],
                                "season": ret[1],
                                "team": ret[2],
                                "updated": ret[3],
                                "inserted": ret[4],
                            }
                        )
            conn.commit()
        return changed

    return None


def _column_type(col: str) -> str:
    if col in BOOL_COLS:
        return "BOOLEAN"
    if col in INT_COLS:
        return "INTEGER"
    if col in FLOAT_COLS:
        return "DOUBLE PRECISION"
    if col in JSON_COLS:
        return "JSONB"
    return "TEXT"


def ensure_nfl_schema(database_url: str) -> None:
    create_sql = (
        f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} ("
        "player_id TEXT NOT NULL, "
        "team TEXT NOT NULL, "
        "season INTEGER NOT NULL"
        ")"
    )
    _exec(database_url, create_sql)

    existing = _fetch_table_columns(database_url, TABLE_NAME) or set()
    for col in OUTPUT_COLUMNS:
        if col in existing:
            continue
        alter_sql = f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS {_quote_ident(col)} {_column_type(col)}"
        _exec(database_url, alter_sql)

    idx_sql = (
        f"CREATE UNIQUE INDEX IF NOT EXISTS {TABLE_NAME}_player_team_season_idx "
        f"ON {TABLE_NAME} (player_id, team, season)"
    )
    _exec(database_url, idx_sql)


def _upload_rows(rows: List[Dict[str, str]], database_url: str) -> Dict[str, int]:
    if not rows:
        return {"inserted": 0, "updated": 0, "unchanged": 0}

    ensure_nfl_schema(database_url)
    table_columns = _fetch_table_columns(database_url, TABLE_NAME) or set()
    columns = [c for c in OUTPUT_COLUMNS if c in table_columns]
    conflict_cols = {"player_id", "team", "season"}
    if not conflict_cols.issubset(set(columns)):
        raise RuntimeError("Missing one or more required conflict columns in player_stats_nfl.")

    quoted_cols = [_quote_ident(c) for c in columns]
    placeholders = ", ".join(["%s::jsonb" if c in JSON_COLS else "%s" for c in columns])
    update_cols = [c for c in columns if c not in conflict_cols]
    if not update_cols:
        raise RuntimeError("No updatable columns available for upload.")

    update_clause = ", ".join(f"{_quote_ident(c)} = EXCLUDED.{_quote_ident(c)}" for c in update_cols)
    diff_clause = " OR ".join(
        f"{TABLE_NAME}.{_quote_ident(c)} IS DISTINCT FROM EXCLUDED.{_quote_ident(c)}" for c in update_cols
    )

    insert_sql = (
        f'INSERT INTO {TABLE_NAME} ({", ".join(quoted_cols)}) '
        f"VALUES ({placeholders}) "
        "ON CONFLICT (player_id, team, season) DO UPDATE "
        f"SET {update_clause} "
        f"WHERE {diff_clause} "
        "RETURNING player_id, season, team, (xmax <> 0) AS updated, (xmax = 0) AS inserted"
    )

    values: List[List[Any]] = []
    new_rows_by_key: Dict[tuple[str, str, str], Dict[str, Any]] = {}
    player_id_for_fetch = ""
    for row in rows:
        converted = _convert_row(row)
        key = (
            str(converted.get("player_id") or ""),
            str(converted.get("team") or ""),
            str(converted.get("season") or ""),
        )
        if not player_id_for_fetch:
            player_id_for_fetch = key[0]
        new_rows_by_key[key] = converted
        values.append([converted.get(col) for col in columns])

    existing_by_key = _fetch_existing_rows_for_player(database_url, player_id_for_fetch, columns)

    changed = _exec_many_returning(database_url, insert_sql, values)
    if changed is None:
        raise RuntimeError("psycopg/psycopg2 not installed; install one or use psql.")

    inserted = sum(1 for r in changed if r["inserted"])
    updated = sum(1 for r in changed if (not r["inserted"]) and r["updated"])
    unchanged = len(values) - len(changed)

    updated_descriptions: List[str] = []
    for row in changed:
        if not row["updated"] or row["inserted"]:
            continue
        key = (str(row["player_id"]), str(row["team"]), str(row["season"]))
        before = existing_by_key.get(key, {})
        after = new_rows_by_key.get(key, {})
        changed_cols: List[str] = []
        for col in columns:
            if col in {"player_id", "team", "season"}:
                continue
            b = before.get(col)
            a = after.get(col)
            if isinstance(b, (dict, list)):
                b = json.dumps(b, sort_keys=True)
            if isinstance(a, (dict, list)):
                a = json.dumps(a, sort_keys=True)
            if str(b) != str(a):
                changed_cols.append(col)
        desc = (
            f"{row['player_id']} season={row['season']} team={row['team']} "
            f"changed: {', '.join(changed_cols) if changed_cols else '(no column diff detected)'}"
        )
        updated_descriptions.append(desc)
        print("Updated row detail:", desc)

    return {
        "inserted": inserted,
        "updated": updated,
        "unchanged": unchanged,
        "updated_descriptions": updated_descriptions,
    }


def upload_player_stats_nfl_rows(
    rows: List[Dict[str, str]], database_url: Optional[str] = None
) -> Dict[str, int]:
    database_url = database_url or os.environ.get(
        "DATABASE_URL",
        "postgresql://postgres:VSRhJUfegIZXrwEHDaDKOLgDJSFsMysl@switchback.proxy.rlwy.net:46735/railway",
    )
    return _upload_rows(rows, database_url)


def upload_player_stats_nfl(csv_path: str, database_url: Optional[str] = None) -> Dict[str, int]:
    database_url = database_url or os.environ.get(
        "DATABASE_URL",
        "postgresql://postgres:VSRhJUfegIZXrwEHDaDKOLgDJSFsMysl@switchback.proxy.rlwy.net:46735/railway",
    )
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [row for row in reader]
    return _upload_rows(rows, database_url)
