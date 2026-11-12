import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def safe_cast_int(value, default=0):
    """
    Safely cast numeric metric values to integer with a fallback.
    Prevents runtime crashes caused by None, non-numeric strings, or invalid API formats.
    """
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        try:
            return int(float(value))
        except (ValueError, TypeError):
            logger.warning(f"Could not parse numeric metric value: {value!r}. Defaulting to {default}.")
            return default


def parse_duration(duration_str):
    """
    Parse ISO 8601 duration string (e.g., 'PT15M33S', 'P0D', 'P1DT2H') into timedelta.
    Defensively handles None, malformed strings, and zero-duration representations.
    """
    if not duration_str or not isinstance(duration_str, str):
        return timedelta(0)

    # Fast-path for zero duration or empty representation
    cleaned = duration_str.strip().lstrip("P")
    if not cleaned or cleaned == "0D":
        return timedelta(0)

    # Separate date (days) and time (hours, minutes, seconds) components
    days_part = ""
    time_part = ""

    if "T" in cleaned:
        parts = cleaned.split("T", 1)
        days_part = parts[0]
        time_part = parts[1]
    else:
        # Day-only duration like 'P1D' or '0D'
        days_part = cleaned

    values = {"D": 0, "H": 0, "M": 0, "S": 0}

    if "D" in days_part:
        val_str, _ = days_part.split("D", 1)
        try:
            values["D"] = int(val_str)
        except (ValueError, TypeError):
            logger.warning(
                f"Failed to parse day component '{val_str}' in duration '{duration_str}'. Defaulting to 0."
            )
            values["D"] = 0

    for component in ["H", "M", "S"]:
        if component in time_part:
            val_str, time_part = time_part.split(component, 1)
            try:
                values[component] = int(val_str)
            except (ValueError, TypeError):
                logger.warning(
                    f"Failed to parse component '{component}' ({val_str!r}) in duration '{duration_str}'. Defaulting to 0."
                )
                values[component] = 0

    return timedelta(
        days=values["D"],
        hours=values["H"],
        minutes=values["M"],
        seconds=values["S"],
    )


def transform_data(row):
    """
    Transform and enrich a row for core data warehouse consumption.
    Defensively parses duration, classifies video type, and safely type-casts integer metrics.
    """
    video_id = row.get("Video_ID") or row.get("video_id") or "UNKNOWN"

    try:
        raw_duration = row.get("Duration") or row.get("duration")
        duration_td = parse_duration(raw_duration)
        row["Duration"] = (datetime.min + duration_td).time()
        row["Video_Type"] = "Shorts" if duration_td.total_seconds() <= 60 else "Normal"
    except Exception as e:
        logger.warning(
            f"Failed to transform duration for Video_ID '{video_id}' (value: {row.get('Duration')!r}): {e}. "
            "Falling back to 00:00:00 and Video_Type 'Unknown'."
        )
        row["Duration"] = datetime.min.time()
        row["Video_Type"] = "Unknown"

    # Defensive integer casting for metrics to prevent runtime task crashes and handle NULLs
    for col in ["Video_Views", "Likes_Count", "Comments_Count", "view_count", "like_count", "comment_count"]:
        if col in row:
            row[col] = safe_cast_int(row[col], default=0)

    return row

