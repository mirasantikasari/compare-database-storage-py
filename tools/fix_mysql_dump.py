from __future__ import annotations

import argparse
import re
from pathlib import Path


TABLE_MARKER_RE = re.compile(r"^-- Table structure for (.+?)\s*$")
DROP_RE = re.compile(r"^DROP TABLE IF EXISTS `([^`]+)`;")
CREATE_RE = re.compile(r"^CREATE TABLE `([^`]+)`")
INSERT_RE = re.compile(r"^INSERT INTO `([^`]+)`")


def is_log_table(table: str) -> bool:
    name = table.strip("`").lower()
    return "log" in name


def normalize_line(line: str) -> str:
    line = line.replace("ROW_FORMAT = Compact", "ROW_FORMAT = Dynamic")
    line = line.replace("ROW_FORMAT=Compact", "ROW_FORMAT=Dynamic")
    line = line.replace("ROW_FORMAT = COMPACT", "ROW_FORMAT = DYNAMIC")
    line = line.replace("ROW_FORMAT=COMPACT", "ROW_FORMAT=DYNAMIC")
    line = line.replace("utf8mb4_0900_ai_ci", "utf8mb4_unicode_ci")
    line = line.replace("utf8mb4_0900_bin", "utf8mb4_bin")
    return line


def table_from_line(line: str) -> str | None:
    for regex in (DROP_RE, CREATE_RE, INSERT_RE):
        match = regex.match(line)
        if match:
            return match.group(1)
    return None


def fix_dump(src: Path, dst: Path) -> dict[str, int | list[str]]:
    dst.parent.mkdir(parents=True, exist_ok=True)

    skipped_tables: set[str] = set()
    skipped_lines = 0
    written_lines = 0
    skipping_table = False

    with src.open("r", encoding="utf-8", errors="replace", newline="") as reader, dst.open(
        "w", encoding="utf-8", newline=""
    ) as writer:
        writer.write("-- Fixed for MySQL import: log tables removed, utf8mb4_0900 normalized, ROW_FORMAT Dynamic.\n")
        writer.write("SET NAMES utf8mb4;\n")
        writer.write("SET FOREIGN_KEY_CHECKS = 0;\n")
        writer.write("SET SQL_MODE = 'NO_AUTO_VALUE_ON_ZERO';\n\n")

        for line in reader:
            marker = TABLE_MARKER_RE.match(line)
            if marker:
                table = marker.group(1)
                skipping_table = is_log_table(table)
                if skipping_table:
                    skipped_tables.add(table)
                    skipped_lines += 1
                    continue

            table = table_from_line(line)
            if table and is_log_table(table):
                skipping_table = True
                skipped_tables.add(table)
                skipped_lines += 1
                continue

            if skipping_table:
                skipped_lines += 1
                continue

            normalized = normalize_line(line)
            if normalized.startswith("SET NAMES ") or normalized.startswith("SET FOREIGN_KEY_CHECKS"):
                continue

            writer.write(normalized)
            written_lines += 1

    return {
        "skipped_lines": skipped_lines,
        "written_lines": written_lines,
        "skipped_tables": sorted(skipped_tables),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Create a MySQL-friendlier SQL dump.")
    parser.add_argument("src", type=Path)
    parser.add_argument("dst", type=Path)
    args = parser.parse_args()

    result = fix_dump(args.src, args.dst)
    print(f"wrote: {args.dst}")
    print(f"written_lines: {result['written_lines']}")
    print(f"skipped_lines: {result['skipped_lines']}")
    print("skipped_tables:")
    for table in result["skipped_tables"]:
        print(f"- {table}")


if __name__ == "__main__":
    main()
