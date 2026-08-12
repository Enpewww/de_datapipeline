from datetime import datetime, timedelta, time
import logging

logger = logging.getLogger(__name__)

def parse_duration(duration_str):

    duration_str = duration_str.replace("P", "").replace("T", "")

    components = ["D", "H", "M", "S"]
    values = {"D": 0, "H": 0, "M": 0, "S": 0}

    for component in components:
        if component in duration_str:
            value, duration_str = duration_str.split(component)
            values[component] = int(value)

    total_duration = timedelta(
        days = values["D"],
        hours = values["H"],
        minutes = values["M"],
        seconds = values["S"]
    )

    return total_duration


def transform_data(row):

    duration_str = str(row["Duration"]).strip()

    logger.info(
        f"TRANSFORM BEFORE: "
        f"Video_Id={row['Video_Id']!r}, "
        f"Duration={row['Duration']!r}, "
        f"duration_str={duration_str!r}, "
        f"Type={type(row['Duration'])}"
    )

    if duration_str.isdigit() and len(duration_str) == 3:
        row["Duration"] = time(0, 0, 0)
        row["Video_Type"] = "Unavailable"

        logger.info(
            f"Transform the Unavailable 3-digit video duration: {row}"
        )

        return row

    if duration_str == "P0D":
        row["Duration"] = time(0, 0, 0)
        row["Video_Type"] = "Unavailable"

        logger.info(
            f"Transform the Unavailable P0D video duration: {row}"
        )

        return row

    duration_td = parse_duration(duration_str)

    row["Duration"] = (datetime.min + duration_td).time()

    if duration_td < timedelta(minutes=1):
        row["Video_Type"] = "Short"
    else:
        row["Video_Type"] = "Normal"

    logger.info(
        f"Transform the Normal/Short videos: {row}"
    )

    return row