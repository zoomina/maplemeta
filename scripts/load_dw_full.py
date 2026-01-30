import argparse
import os
import re
from typing import List, Set

from load_dw_daily import load_dw_for_date


DATE_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2})")


def _data_json_dir(explicit_dir: str = None) -> str:
    if explicit_dir:
        return explicit_dir
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_dir, "data_json")


def discover_dates(data_json_dir: str) -> List[str]:
    dates: Set[str] = set()
    for name in os.listdir(data_json_dir):
        if not name.endswith(".json"):
            continue
        match = DATE_PATTERN.search(name)
        if match:
            dates.add(match.group(1))
    return sorted(dates)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load DW tables for all dates in data_json.")
    parser.add_argument("--data-json-dir", required=False, help="Override data_json directory")
    args = parser.parse_args()

    data_json_dir = _data_json_dir(args.data_json_dir)
    dates = discover_dates(data_json_dir)

    for date in dates:
        load_dw_for_date(date, data_json_dir)


if __name__ == "__main__":
    main()
