#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Robust vertical concatenation of CSV files:
- Keep the header from the first CSV only
- From each CSV, take physical lines [start..end] inclusive (1-based indexing)
  e.g., default 2..256 (= 255 data lines per file)
- Handle encoding automatically: try utf-8-sig, utf-8, then cp932 (Shift-JIS)
- Handle column shifts:
    * If a row has more fields than header, squash the overflow back into the last column
    * If a row has fewer fields, right-pad with empty strings
- Write output with UTF-8 BOM for better Excel compatibility
"""
import argparse
import csv
from pathlib import Path
from typing import List, Optional

ENCODINGS_TRY = ("utf-8-sig", "utf-8", "cp932")

def read_all_lines(path: Path) -> Optional[list]:
    for enc in ENCODINGS_TRY:
        try:
            with open(path, "r", encoding=enc, newline="") as f:
                return f.read().splitlines()
        except Exception:
            continue
    return None

def parse_header(line: str) -> List[str]:
    return next(csv.reader([line]))

def parse_row(line: str) -> List[str]:
    return next(csv.reader([line]))

def main():
    ap = argparse.ArgumentParser(description="Concatenate CSV rows 2..256 per file (robust), keeping only one header.")
    ap.add_argument("input_dir", help="Folder containing the CSV files")
    ap.add_argument("-o", "--output", default="all_data.csv", help="Output CSV path")
    ap.add_argument("--pattern", default="*.csv", help="Glob pattern for input files (default: *.csv)")
    ap.add_argument("--with-source", action="store_true", help="Add a 'source_file' column at the beginning")
    ap.add_argument("--start", type=int, default=2, help="Start line (1-based, inclusive). Default: 2")
    ap.add_argument("--end", type=int, default=256, help="End line (1-based, inclusive). Default: 256")
    args = ap.parse_args()

    start_idx = max(1, args.start) - 1  # convert to 0-based
    end_idx = max(start_idx, args.end - 1)  # inclusive (0-based)

    root = Path(args.input_dir)
    files = sorted(root.glob(args.pattern))
    if not files:
        raise SystemExit(f"No CSV files matched: {root}/{args.pattern}")

    # Read header from first file
    first_lines = read_all_lines(files[0])
    if not first_lines:
        raise SystemExit(f"Failed to read first file: {files[0]} with encodings {ENCODINGS_TRY}")
    header = parse_header(first_lines[0])

    # Prepare output
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    full_header = (["source_file"] + header) if args.with_source else header

    wrote = 0
    kept_rows = 0
    skipped_files = 0

    with open(out_path, "w", encoding="utf-8-sig", newline="") as out_f:
        writer = csv.writer(out_f)
        writer.writerow(full_header)

        for path in files:
            lines = read_all_lines(path)
            if not lines:
                print(f"[WARN] Could not read {path} with any encoding; skipping.")
                skipped_files += 1
                continue

            # Select physical lines [start..end] inclusive in 1-based => [start_idx..end_idx] in 0-based
            data_lines = lines[start_idx : end_idx + 1]
            if not data_lines:
                print(f"[WARN] No data lines in range {args.start}..{args.end} for {path.name}")
                continue

            for line in data_lines:
                row = parse_row(line)

                if len(row) > len(header):
                    row = row[:len(header)-1] + [",".join(row[len(header)-1:])]
                elif len(row) < len(header):
                    row = row + [""] * (len(header) - len(row))

                if args.with_source:
                    writer.writerow([path.name] + row)
                else:
                    writer.writerow(row)

                kept_rows += 1
            wrote += 1

    print(f"Wrote {out_path}  files_processed={wrote}  rows_written={kept_rows}  skipped_files={skipped_files}")
    print(f"Header columns: {len(header)}; Encoding tried (in order): {ENCODINGS_TRY}")

if __name__ == "__main__":
    main()
