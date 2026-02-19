import argparse
import os
import re
from typing import List, Set, Tuple

from load_dw_daily import load_dw_for_date


DATE_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2})")


def _data_json_dir(explicit_dir: str = None) -> str:
    if explicit_dir:
        return explicit_dir
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_dir, "data_json")


def discover_targets(data_json_dir: str, recursive: bool = True) -> List[Tuple[str, str]]:
    targets: Set[Tuple[str, str]] = set()

    if recursive:
        for dirpath, _, filenames in os.walk(data_json_dir):
            for name in filenames:
                if not name.endswith(".json"):
                    continue
                match = DATE_PATTERN.search(name)
                if match:
                    targets.add((dirpath, match.group(1)))
    else:
        for name in os.listdir(data_json_dir):
            if not name.endswith(".json"):
                continue
            match = DATE_PATTERN.search(name)
            if match:
                targets.add((data_json_dir, match.group(1)))

    return sorted(targets, key=lambda x: (x[0], x[1]))


def main() -> None:
    parser = argparse.ArgumentParser(description="Load DW tables for all dates in data_json.")
    parser.add_argument("--data-json-dir", required=False, help="Override data_json directory")
    parser.add_argument(
        "--non-recursive",
        action="store_true",
        help="Scan only top-level data_json directory (default: recursive).",
    )
    args = parser.parse_args()

    data_json_dir = _data_json_dir(args.data_json_dir)
    recursive = not args.non_recursive
    targets = discover_targets(data_json_dir, recursive=recursive)

    if not targets:
        raise FileNotFoundError(f"No JSON targets found under: {data_json_dir}")

    total = len(targets)
    for idx, (target_dir, date) in enumerate(targets, start=1):
        print(f"[{idx}/{total}] DW full load -> dir={target_dir}, date={date}")
        load_dw_for_date(date, target_dir)


if __name__ == "__main__":
    main()
