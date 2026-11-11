"""
Astro CSV Signal Engine for Index Futures
Implements:
1. Flexible CSV Parsing & Ingestion:
   - Dynamic header detection across first 15 lines (skips metadata/preamble rows).
   - Column normalization (maps U/D Logic, Direction, Signal, etc. to 'direction').
   - Timestamp parsing & chronological sorting across multiple date/time formats.
   - Robust direction aliasing (Upside/Downside/Neutral).
2. 3-Row Cluster Signal Logic:
   - Window of [current, next, next+1] where current is latest row <= now.
   - All 3 upside -> BUY (Long Future).
   - All 3 downside -> SELL (Short Future).
   - Mixed/neutral -> None.
3. Entry Filters:
   - Active position filter (one trade at a time).
   - Index Trading hours filter (default 09:15 to 15:30 IST, configurable).
   - Fingerprint deduplication.
4. Audit Logging:
   - Logs every evaluation and decision to logs/YYYY-MM-DD/app.log.
"""

import io
import os
import re
import datetime
import logging
from typing import Dict, Any, Optional, List, Tuple, Union
import pandas as pd

logger = logging.getLogger("AstroSignalEngine")

IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

# Direction aliases
UPSIDE_ALIASES = {
    "upside", "buy", "up", "bullish", "1", "1.0", "+1", "long", "b", "call", "green", "positive", "uptrend"
}
DOWNSIDE_ALIASES = {
    "downside", "sell", "down", "bearish", "-1", "-1.0", "short", "s", "put", "red", "negative", "downtrend"
}


def normalize_direction(val: Any) -> str:
    """Normalizes direction values to 'upside', 'downside', or 'neutral'."""
    if val is None or pd.isna(val):
        return "neutral"
    s = str(val).strip().lower()
    # Remove surrounding quotes, brackets
    s = re.sub(r"[^\w\.\-\+]", "", s)
    if s in UPSIDE_ALIASES:
        return "upside"
    elif s in DOWNSIDE_ALIASES:
        return "downside"
    return "neutral"


def clean_time_string(val: Any) -> str:
    """
    Extracts a valid start time string from raw time entries.
    Handles ranges like '09:15 - 09:30', '9.15 to 9.30', '9.3', '09:15:00', '9:15 AM'.
    """
    if val is None or pd.isna(val):
        return ""
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "null"):
        return ""

    # Check if range separator exists (e.g., ' - ', ' to ', ' ~ ')
    parts = re.split(r"\s*(?:-|to|~|\/)\s*", s, flags=re.IGNORECASE)
    first_part = parts[0].strip() if parts else s

    # Check for standard colon time pattern HH:MM or HH:MM:SS with optional AM/PM
    match_colon = re.search(r"(\d{1,2}:\d{2}(?::\d{2})?(?:\s*(?:AM|PM|am|pm))?)", first_part)
    if match_colon:
        return match_colon.group(1).strip()

    # Check for dot-time format like 9.30, 09.15, 9.3 (where 9.3 == 09:30), 14.5 (14:50)
    match_dot = re.match(r"^(\d{1,2})\.(\d{1,2})(?:\s*(?:AM|PM|am|pm))?$", first_part, re.IGNORECASE)
    if match_dot:
        hour = match_dot.group(1).zfill(2)
        min_part = match_dot.group(2)
        minute = (min_part + "0") if len(min_part) == 1 else min_part.zfill(2)[:2]
        return f"{hour}:{minute}:00"

    return first_part


def detect_header_row(lines: List[str], max_scan_lines: int = 30) -> int:
    """
    Scans the first N lines of CSV text to detect the true header row.
    Looks for keywords: date, time, direction, udlogic, logic, signal, trend, view.
    """
    header_keywords = {
        "date", "time", "datetime", "timestamp", "timing", "direction",
        "udlogic", "logic", "signal", "trend", "ud", "view", "bias",
        "action", "call", "position", "side", "trade", "type", "updown", "prediction"
    }
    scan_limit = min(len(lines), max_scan_lines)

    for idx in range(scan_limit):
        line = lines[idx].strip()
        if not line:
            continue
        parts = re.split(r"[,;\t|]", line)
        tokens = [re.sub(r"[^a-zA-Z0-9]", "", p).lower() for p in parts if p.strip()]

        has_date_or_time = any(t in ("date", "time", "datetime", "timestamp", "timing", "fromtime", "starttime") for t in tokens)
        has_logic = any(t in ("direction", "udlogic", "logic", "signal", "trend", "ud", "view", "bias", "call", "action", "position", "side", "trade", "type", "updown") for t in tokens)

        if has_date_or_time and has_logic:
            return idx
        if any(t in header_keywords for t in tokens) and len(tokens) >= 2:
            return idx

    return 0


def parse_astro_csv(content: Union[str, bytes]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Parses an uploaded Astro CSV file into a clean, chronologically sorted DataFrame.
    Supports multi-encoding, automatic delimiter detection, time range extraction,
    and fallback column heuristics. Returns (df, metadata).
    """
    if isinstance(content, bytes):
        for enc in ("utf-8-sig", "utf-8", "latin-1", "cp1252", "iso-8859-1"):
            try:
                text = content.decode(enc)
                break
            except UnicodeDecodeError:
                continue
        else:
            text = content.decode("utf-8", errors="replace")
    else:
        text = str(content)

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        raise ValueError("Astro CSV content is empty.")

    header_idx = detect_header_row(lines, max_scan_lines=30)
    csv_text = "\n".join(lines[header_idx:])

    # Parse with pandas, attempting multiple separators with dtype=str to preserve formatting
    df = None
    parse_errors = []
    for sep in [None, ",", ";", "\t", "|"]:
        try:
            if sep is None:
                df_candidate = pd.read_csv(io.StringIO(csv_text), sep=None, engine="python", dtype=str)
            else:
                df_candidate = pd.read_csv(io.StringIO(csv_text), sep=sep, dtype=str)
            if not df_candidate.empty and len(df_candidate.columns) >= 2:
                df = df_candidate
                break
        except Exception as e:
            parse_errors.append(str(e))
            continue

    if df is None or df.empty:
        try:
            df = pd.read_csv(io.StringIO(csv_text), dtype=str)
        except Exception as e:
            raise ValueError(f"Failed to parse CSV structure: {e}")

    if df.empty:
        raise ValueError("Astro CSV contains no data rows.")

    # Drop all-NaN columns and rows
    df = df.dropna(how="all").dropna(axis=1, how="all")

    # Clean and match column names
    date_col = None
    time_col = None
    datetime_col = None
    dir_col = None

    date_keys = ("date", "dt", "tradedate", "sessiondate", "day", "entrydate", "startdate")
    time_keys = ("time", "tm", "tradetime", "timing", "fromtime", "starttime", "candletime", "bartime", "entrytime")
    dt_keys = ("datetime", "timestamp", "datetimestamp", "dateandtime")
    dir_keys = ("direction", "dir", "udlogic", "logic", "signal", "trend", "ud", "view", "bias", "call", "action", "position", "side", "trade", "type", "updown", "prediction", "marketview")

    for col in df.columns:
        clean = re.sub(r"[^a-zA-Z0-9]", "", str(col)).lower()
        if clean in dt_keys:
            datetime_col = col
        elif clean in date_keys:
            date_col = col
        elif clean in time_keys:
            time_col = col
        elif clean in dir_keys:
            dir_col = col

    # Fallback heuristic for direction column if not matched by name
    if dir_col is None:
        for col in df.columns:
            if col in (date_col, time_col, datetime_col):
                continue
            sample_vals = [normalize_direction(v) for v in df[col].dropna().head(10)]
            if any(v in ("upside", "downside") for v in sample_vals):
                dir_col = col
                break

    # Fallback to column index if still not found
    if dir_col is None:
        if len(df.columns) >= 3:
            dir_col = df.columns[2]
        elif len(df.columns) >= 2:
            dir_col = df.columns[1]
        else:
            raise ValueError(f"Could not identify direction/signal column. Available columns: {list(df.columns)}")

    df["raw_direction"] = df[dir_col].astype(str)
    df["direction"] = df["raw_direction"].apply(normalize_direction)

    # Heuristic for time column if not found
    if time_col is None and datetime_col is None:
        for col in df.columns:
            if col in (date_col, dir_col):
                continue
            sample_str = " ".join([str(v) for v in df[col].dropna().head(5)])
            if re.search(r"\d{1,2}[:.]\d{2}", sample_str):
                time_col = col
                break

    # Construct datetime series
    today_str = datetime.datetime.now(IST).strftime("%Y-%m-%d")

    if datetime_col is not None:
        raw_times = df[datetime_col].astype(str).str.strip()
        datetime_series = raw_times
    elif date_col is not None and time_col is not None and date_col != time_col:
        clean_dates = df[date_col].astype(str).str.strip()
        clean_times = df[time_col].apply(clean_time_string)
        datetime_series = clean_dates + " " + clean_times
    elif time_col is not None:
        clean_times = df[time_col].apply(clean_time_string)
        datetime_series = today_str + " " + clean_times
    elif date_col is not None:
        datetime_series = df[date_col].astype(str).str.strip()
    else:
        # Check first column as timestamp or time
        first_col = df.columns[0]
        sample_str = " ".join([str(v) for v in df[first_col].dropna().head(5)])
        if re.search(r"\d{1,2}[:.]\d{2}", sample_str):
            clean_times = df[first_col].apply(clean_time_string)
            datetime_series = today_str + " " + clean_times
        else:
            raise ValueError(f"No date or time column detected in Astro CSV. Available columns: {list(df.columns)}")

    sample = datetime_series.dropna().iloc[0] if len(datetime_series.dropna()) else ""
    is_year_first = bool(re.match(r"^\s*\d{4}", str(sample)))
    parsed_dates = pd.to_datetime(
        datetime_series,
        errors="coerce",
        format="mixed",
        dayfirst=(not is_year_first),
    )

    parsed_dt_list = []
    for dt in parsed_dates:
        if pd.isna(dt):
            parsed_dt_list.append(None)
        else:
            py_dt = dt.to_pydatetime()
            if py_dt.tzinfo is None:
                py_dt = py_dt.replace(tzinfo=IST)
            else:
                py_dt = py_dt.astimezone(IST)
            parsed_dt_list.append(py_dt)

    df["parsed_dt"] = parsed_dt_list
    df = df.dropna(subset=["parsed_dt"])
    if df.empty:
        raise ValueError("Could not parse valid timestamps from the Date/Time column in this CSV.")

    df = df.sort_values(by="parsed_dt").reset_index(drop=True)

    metadata = {
        "header_row_skipped": header_idx,
        "row_count": len(df),
        "start_time": df["parsed_dt"].iloc[0].isoformat() if len(df) else None,
        "end_time": df["parsed_dt"].iloc[-1].isoformat() if len(df) else None,
        "unique_directions": df["direction"].unique().tolist(),
        "detected_direction_col": str(dir_col),
        "detected_time_col": str(time_col or datetime_col or "auto"),
    }
    return df, metadata


class AstroSignalEngine:
    """
    Evaluates 3-row cluster window and entry filters for Index Futures entries.
    """

    def __init__(self, log_dir: str = "logs"):
        self.log_dir = log_dir

    def log_decision(
        self,
        event_type: str,
        current_dt: datetime.datetime,
        details: Dict[str, Any],
        commodity: str = "NIFTY",
    ) -> None:
        """Appends audit decision record to logs/YYYY-MM-DD/app.log."""
        try:
            today_str = current_dt.strftime("%Y-%m-%d")
            dir_path = os.path.join(self.log_dir, today_str)
            os.makedirs(dir_path, exist_ok=True)
            log_path = os.path.join(dir_path, "app.log")

            time_str = current_dt.strftime("%H:%M:%S")
            msg = f"[{time_str}] [ASTRO_SIGNAL] [{commodity}] [{event_type}] {details}\n"
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(msg)
        except Exception as e:
            logger.warning(f"Failed writing astro audit log: {e}")

    def evaluate_cluster(
        self,
        df: pd.DataFrame,
        current_dt: Optional[datetime.datetime] = None,
        last_trade_direction: Optional[str] = None,
        has_active_trade: bool = False,
        cutoff_time_ist: str = "15:30",
        market_open_ist: str = "09:15",
        commodity: str = "NIFTY",
    ) -> Dict[str, Any]:
        """
        Evaluates the 3-row cluster at current_dt against entry filters.
        Returns a decision payload dict with signal: 'BUY' or 'SELL'.
        """
        now = current_dt if current_dt is not None else datetime.datetime.now(IST)
        if now.tzinfo is None:
            now = now.replace(tzinfo=IST)

        if df is None or df.empty:
            res = {"status": "NO_SIGNAL", "reason": "empty_dataframe", "timestamp": now.isoformat()}
            self.log_decision("NO_SIGNAL", now, res, commodity)
            return res

        # If all rows in df are on a single date different from `now.date()`,
        # dynamically align timestamps to `now.date()` so intraday time-of-day matching works seamlessly in live sessions.
        now_date = now.date()
        df_dates = {dt.date() for dt in df["parsed_dt"] if dt is not None}
        if df_dates and now_date not in df_dates and len(df_dates) == 1:
            df = df.copy()
            df["parsed_dt"] = [
                dt.replace(year=now_date.year, month=now_date.month, day=now_date.day) if dt is not None else None
                for dt in df["parsed_dt"]
            ]

        past_rows = df[df["parsed_dt"] <= now]
        if past_rows.empty:
            preview_window = df.iloc[0 : min(3, len(df))]
            preview_summary = [
                {
                    "time": r["parsed_dt"].strftime("%H:%M:%S") if hasattr(r["parsed_dt"], "strftime") else str(r["parsed_dt"]),
                    "raw": str(r.get("raw_direction", r["direction"])),
                    "normalized": str(r["direction"]),
                }
                for _, r in preview_window.iterrows()
            ]
            first_time_str = df["parsed_dt"].iloc[0].strftime("%H:%M:%S") if len(df) else ""
            res = {
                "status": "NO_SIGNAL",
                "reason": f"Waiting for report start time ({first_time_str})",
                "timestamp": now.isoformat(),
                "window": preview_summary,
                "first_row_time": df["parsed_dt"].iloc[0].isoformat() if len(df) else None,
            }
            self.log_decision("NO_SIGNAL", now, res, commodity)
            return res

        actual_idx = past_rows.index[-1]
        window = df.iloc[actual_idx : actual_idx + 3]

        if len(window) < 3:
            fallback_window = df.tail(min(3, len(df)))
            fallback_summary = [
                {
                    "time": r["parsed_dt"].strftime("%H:%M:%S") if hasattr(r["parsed_dt"], "strftime") else str(r["parsed_dt"]),
                    "raw": str(r.get("raw_direction", r["direction"])),
                    "normalized": str(r["direction"]),
                }
                for _, r in fallback_window.iterrows()
            ]
            res = {
                "status": "NO_SIGNAL",
                "reason": "Report concluded (all rows completed)",
                "remaining_count": len(window),
                "timestamp": now.isoformat(),
                "window": fallback_summary,
            }
            self.log_decision("NO_SIGNAL", now, res, commodity)
            return res

        directions = window["direction"].tolist()
        window_summary = [
            {
                "time": r["parsed_dt"].strftime("%H:%M:%S") if hasattr(r["parsed_dt"], "strftime") else str(r["parsed_dt"]),
                "raw": str(r.get("raw_direction", r["direction"])),
                "normalized": str(d),
            }
            for d, (_, r) in zip(directions, window.iterrows())
        ]

        raw_signal = None
        trade_side = None

        if all(d == "upside" for d in directions):
            raw_signal = "BUY"
            trade_side = "BUY"
        elif all(d == "downside" for d in directions):
            raw_signal = "SELL"
            trade_side = "SELL"
        else:
            res = {
                "status": "NO_SIGNAL",
                "reason": "cluster_mixed_or_neutral",
                "directions": directions,
                "window": window_summary,
                "timestamp": now.isoformat(),
            }
            self.log_decision("EVALUATED_NO_SIGNAL", now, res, commodity)
            return res

        # Filter 1: Trading hours check (09:00 to cutoff_time_ist, e.g. 23:30)
        try:
            c_h, c_m = [int(x) for x in cutoff_time_ist.split(":")[:2]]
            cutoff_t = datetime.time(c_h, c_m)
        except Exception:
            cutoff_t = datetime.time(15, 30)

        try:
            o_h, o_m = [int(x) for x in market_open_ist.split(":")[:2]]
            open_t = datetime.time(o_h, o_m)
        except Exception:
            open_t = datetime.time(9, 15)

        now_t = now.time()
        if not (open_t <= now_t <= cutoff_t):
            res = {
                "status": "BLOCKED",
                "filter": "trading_hours",
                "raw_signal": raw_signal,
                "trade_side": trade_side,
                "current_time": now_t.strftime("%H:%M:%S"),
                "hours_range": f"{open_t.strftime('%H:%M')} - {cutoff_t.strftime('%H:%M')}",
                "window": window_summary,
            }
            self.log_decision("FILTER_BLOCKED", now, res, commodity)
            return res

        # Filter 2: Active open trade filter
        if has_active_trade:
            res = {
                "status": "BLOCKED",
                "filter": "active_trade_open",
                "raw_signal": raw_signal,
                "trade_side": trade_side,
                "window": window_summary,
                "timestamp": now.isoformat(),
            }
            self.log_decision("FILTER_BLOCKED", now, res, commodity)
            return res

        # Approved Signal!
        cluster_dt = window.iloc[0]["parsed_dt"]
        cluster_dt_iso = cluster_dt.isoformat() if hasattr(cluster_dt, "isoformat") else str(cluster_dt)
        fingerprint = f"{int(actual_idx)}_{cluster_dt_iso}_{trade_side}"

        approved = {
            "status": "APPROVED",
            "signal": raw_signal,
            "trade_side": trade_side,
            "directions": directions,
            "window": window_summary,
            "timestamp": now.isoformat(),
            "fingerprint": fingerprint,
            "cluster_time": cluster_dt_iso,
            "cluster_index": int(actual_idx),
            "commodity": commodity,
        }
        self.log_decision("SIGNAL_APPROVED", now, approved, commodity)
        return approved


# Global singleton instance
astro_signal_engine = AstroSignalEngine()
