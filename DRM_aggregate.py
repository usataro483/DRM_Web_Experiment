
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DRM aggregator: concatenate CSVs (robust) and compute per-subject metrics
mirroring the example Excel (HitA/HitB/MissA/MissB/FA_*/CR_* and mean RT).

Usage examples:
  # 1) Read all CSVs in a folder, then aggregate
  python drm_aggregate.py "C:\path\to\csvs" --pattern "*.csv" --start 2 --end 256 -o combined.csv -a aggregated.xlsx

  # 2) Aggregate from an already-combined CSV
  python drm_aggregate.py combined.csv --aggregate-only -a aggregated.xlsx

Notes:
- Encoding is auto-tried (utf-8-sig, utf-8, cp932).
- Header is taken from the first file (for folder input).
- Flexible column detection:
    subj_id column: one of ["subj_id","subject","participant","id"]
    truth/condition column: one of ["truth","ground_truth","item_type","stim_type","cond","condition","label","correct_answer"]
    response column: one of ["resp","response","answer","key","resp_key","resp_text"]
    RT column (optional): one of ["rt","RT","reaction_time","response_time","latency"]
- Value mapping (case-insensitive):
    Target A: ["olda","targeta","a_target","aold","a-old","a"]
    Target B: ["oldb","targetb","b_target","bold","b-old","b"]
    Lure A:   ["lurea","a_lure","a-lure","lure-a","a*"]
    Lure B:   ["lureb","b_lure","b-lure","lure-b","b*"]
    New/C:    ["new","c","control","foil","unstudied","new(c)"]

  Response (old/studied): ["old","studied","learned","yes","1","学習した"]
  Response (new/unstudied): ["new","unstudied","no","0","学習していない"]

If your dataset uses different labels, use the CLI to override:
  --truth-col TRUTH --resp-col RESP
  --map-target-a "my_oldA" --map-lure-a "my_lureA" ... etc.

Output:
- Combined CSV (if not --aggregate-only)
- Aggregated Excel (.xlsx) with columns:
  subj_id, HitA, HitB, MissA, MissB, FA_C, FA_lureA, FA_lureB, CR_C, CR_lureA, CR_lureB, RT
  (Plus *_rate columns and any passthrough demographics if found: age, sex/gender)
"""
import argparse, csv, re, math
from pathlib import Path
from typing import List, Optional, Tuple, Dict
import pandas as pd

ENCODINGS_TRY = ("utf-8-sig", "utf-8", "cp932")

# ---- Robust CSV reading (line-slice + column-shift handling) -----------------
def read_all_lines(path: Path) -> Optional[list]:
    for enc in ENCODINGS_TRY:
        try:
            with open(path, "r", encoding=enc, newline="") as f:
                return f.read().splitlines()
        except Exception:
            continue
    return None

def parse_row(line: str) -> List[str]:
    return next(csv.reader([line]))

def concat_csvs(input_dir: Path, pattern: str, start: int, end: int, out_csv: Optional[Path]) -> pd.DataFrame:
    files = sorted(input_dir.glob(pattern))
    if not files:
        raise SystemExit(f"No CSV files matched: {input_dir}/{pattern}")

    first_lines = read_all_lines(files[0])
    if not first_lines:
        raise SystemExit(f"Failed to read first file: {files[0]} with encodings {ENCODINGS_TRY}")
    header = parse_row(first_lines[0])

    start_idx = max(1, start) - 1
    end_idx = max(start_idx, end - 1)

    rows = []
    for path in files:
        lines = read_all_lines(path)
        if not lines:
            print(f"[WARN] Could not read {path} with any encoding; skipping.")
            continue
        data_lines = lines[start_idx : end_idx + 1]
        for line in data_lines:
            row = parse_row(line)
            if len(row) > len(header):
                row = row[:len(header)-1] + [",".join(row[len(header)-1:])]
            elif len(row) < len(header):
                row = row + [""] * (len(header) - len(row))
            rows.append(row)

    df = pd.DataFrame(rows, columns=header)
    if out_csv is not None:
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    return df

# ---- Flexible column detection & normalization --------------------------------
CAND_SUBJ = ["subj_id","subject","participant","id"]
CAND_TRUTH = ["truth","ground_truth","item_type","stim_type","cond","condition","label","correct_answer"]
CAND_RESP = ["resp","response","answer","key","resp_key","resp_text"]
CAND_RT = ["rt","RT","reaction_time","response_time","latency"]
DEMOS = ["age","sex","gender"]

def pick_col(df: pd.DataFrame, candidates: List[str], override: Optional[str]) -> Optional[str]:
    if override and override in df.columns:
        return override
    low_cols = {c.lower(): c for c in df.columns}
    for name in candidates:
        if name.lower() in low_cols:
            return low_cols[name.lower()]
    return None

def norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "")).lower()

# Default value maps; can be expanded
DEFAULT_MAP = {
    "target_a": {"olda","targeta","atarget","aold","aold2","a"},  # allow loose
    "target_b": {"oldb","targetb","btarget","bold","bold2","b"},
    "lure_a":   {"lurea","alure","lure-a","a*","lure_a"},
    "lure_b":   {"lureb","blure","lure-b","b*","lure_b"},
    "new_c":    {"new","c","control","foil","unstudied","newc","new(c)"},
}
RESP_OLD = {"old","studied","learned","yes","1","学習した","studied1"}
RESP_NEW = {"new","unstudied","no","0","学習していない","new0"}

def categorize_row(val_truth: str) -> str:
    v = norm(val_truth)
    if v in DEFAULT_MAP["target_a"]:
        return "targetA"
    if v in DEFAULT_MAP["target_b"]:
        return "targetB"
    if v in DEFAULT_MAP["lure_a"]:
        return "lureA"
    if v in DEFAULT_MAP["lure_b"]:
        return "lureB"
    if v in DEFAULT_MAP["new_c"]:
        return "newC"
    # Fallback heuristics
    if "lure" in v and "a" in v: return "lureA"
    if "lure" in v and "b" in v: return "lureB"
    if "old" in v and "a" in v:  return "targetA"
    if "old" in v and "b" in v:  return "targetB"
    if "new" in v or "control" in v or v=="c": return "newC"
    return "unknown"

def resp_is_old(val_resp: str) -> Optional[bool]:
    v = norm(val_resp)
    if v in RESP_OLD: return True
    if v in RESP_NEW: return False
    # heuristics
    if v in {"1","y","t"}: return True
    if v in {"0","n","f"}: return False
    if "old" in v: return True
    if "new" in v: return False
    return None

# ---- Aggregation ---------------------------------------------------------------
def safe_div(n: float, d: float) -> float:
    return float(n) / float(d) if d else float("nan")

def aggregate(df: pd.DataFrame,
              subj_col: str, truth_col: str, resp_col: str,
              rt_col: Optional[str]) -> pd.DataFrame:
    # Normalize working columns
    work = df.copy()
    work["_cat"] = work[truth_col].astype(str).map(categorize_row)
    work["_is_old"] = work[resp_col].astype(str).map(resp_is_old)
    if rt_col and rt_col in work.columns:
        # Try numeric
        work["_rt"] = pd.to_numeric(work[rt_col], errors="coerce")
    else:
        work["_rt"] = float("nan")

    # Filter rows we can interpret
    work = work[work["_cat"] != "unknown"].copy()

    # Prepare counts per subject
    def agg_one(g: pd.DataFrame) -> pd.Series:
        # totals per category
        n_tA = (g["_cat"]=="targetA").sum()
        n_tB = (g["_cat"]=="targetB").sum()
        n_lA = (g["_cat"]=="lureA").sum()
        n_lB = (g["_cat"]=="lureB").sum()
        n_c  = (g["_cat"]=="newC").sum()

        # decisions (old=True/new=False), ignore None
        mask_old = g["_is_old"] == True
        mask_new = g["_is_old"] == False

        HitA = ((g["_cat"]=="targetA") & mask_old).sum()
        HitB = ((g["_cat"]=="targetB") & mask_old).sum()
        MissA = ((g["_cat"]=="targetA") & mask_new).sum()
        MissB = ((g["_cat"]=="targetB") & mask_new).sum()

        FA_lureA = ((g["_cat"]=="lureA") & mask_old).sum()
        FA_lureB = ((g["_cat"]=="lureB") & mask_old).sum()
        FA_C     = ((g["_cat"]=="newC")  & mask_old).sum()

        CR_lureA = ((g["_cat"]=="lureA") & mask_new).sum()
        CR_lureB = ((g["_cat"]=="lureB") & mask_new).sum()
        CR_C     = ((g["_cat"]=="newC")  & mask_new).sum()

        # rates
        HitA_rate = safe_div(HitA, n_tA)
        HitB_rate = safe_div(HitB, n_tB)
        FA_lureA_rate = safe_div(FA_lureA, n_lA)
        FA_lureB_rate = safe_div(FA_lureB, n_lB)
        FA_C_rate     = safe_div(FA_C, n_c)

        # mean RT where parseable
        RT = g.loc[g["_rt"].notna(), "_rt"].mean()

        out = {
            "HitA": HitA, "HitB": HitB, "MissA": MissA, "MissB": MissB,
            "FA_C": FA_C, "FA_lureA": FA_lureA, "FA_lureB": FA_lureB,
            "CR_C": CR_C, "CR_lureA": CR_lureA, "CR_lureB": CR_lureB,
            "HitA_rate": HitA_rate, "HitB_rate": HitB_rate,
            "FA_C_rate": FA_C_rate, "FA_lureA_rate": FA_lureA_rate, "FA_lureB_rate": FA_lureB_rate,
            "RT": RT
        }
        # pass through demographics if columns exist (take first non-null)
        for dcol in DEMOS:
            if dcol in g.columns:
                val = g[dcol].dropna().iloc[0] if g[dcol].notna().any() else None
                out[dcol] = val
        return pd.Series(out)

    ag = work.groupby(subj_col, dropna=False).apply(agg_one).reset_index().rename(columns={subj_col:"subj_id"})
    # order columns similar to Excel
    base_cols = ["subj_id","HitA","HitB","MissA","MissB","FA_C","FA_lureA","FA_lureB","CR_C","CR_lureA","CR_lureB","RT"]
    rate_cols = ["HitA_rate","HitB_rate","FA_C_rate","FA_lureA_rate","FA_lureB_rate"]
    demo_cols = [c for c in DEMOS if c in ag.columns]
    ordered = [c for c in base_cols if c in ag.columns] + [c for c in rate_cols if c in ag.columns] + demo_cols
    # Add any missing then the rest
    rest = [c for c in ag.columns if c not in ordered]
    ag = ag[ordered + rest]
    return ag

def main():
    ap = argparse.ArgumentParser(description="Concatenate DRM CSVs and aggregate per-subject metrics.")
    ap.add_argument("input", help="Folder of CSVs OR a single combined CSV file")
    ap.add_argument("--pattern", default="*.csv", help="Glob pattern for CSVs if input is a folder")
    ap.add_argument("--start", type=int, default=2, help="Start line (1-based, inclusive) when slicing each CSV")
    ap.add_argument("--end", type=int, default=256, help="End line (1-based, inclusive) when slicing each CSV")

    ap.add_argument("-o","--output", default="all_data.csv", help="Path to save combined CSV (optional)")
    ap.add_argument("-a","--aggregate-out", default="all_data.xlsx", help="Path to save aggregated Excel")
    ap.add_argument("--aggregate-only", action="store_true", help="Treat input as a single combined CSV and skip concatenation")

    # Optional overrides
    ap.add_argument("--subj-col", default=None, help="Override subject ID column name")
    ap.add_argument("--truth-col", default=None, help="Override ground-truth/condition column")
    ap.add_argument("--resp-col", default=None, help="Override response column")
    ap.add_argument("--rt-col", default=None, help="Override reaction time column")

    args = ap.parse_args()
    inp = Path(args.input)

    if args.aggregate_only:
        df = pd.read_csv(inp, dtype=str, encoding="utf-8-sig")
    else:
        if inp.is_file():
            # treat as already-combined CSV
            df = pd.read_csv(inp, dtype=str, encoding="utf-8-sig")
        else:
            df = concat_csvs(inp, args.pattern, args.start, args.end, Path(args.output) if args.output else None)

    # Best-effort type coercion (do not fail if headers differ)
    # Try both utf-8-sig and cp932 for safety
    if df.empty:
        raise SystemExit("No rows to aggregate.")
    # Try to locate key columns
    subj_col = pick_col(df, CAND_SUBJ, args.subj_col)
    truth_col = pick_col(df, CAND_TRUTH, args.truth_col)
    resp_col = pick_col(df, CAND_RESP, args.resp_col)
    rt_col = pick_col(df, CAND_RT, args.rt_col)

    missing = [name for name, col in [("subj_id",subj_col),("truth/cond",truth_col),("response",resp_col)] if col is None]
    if missing:
        raise SystemExit(f"Could not find required column(s): {', '.join(missing)}. "
                         f"Use --subj-col/--truth-col/--resp-col to specify.")

    ag = aggregate(df, subj_col, truth_col, resp_col, rt_col)
    out_path = Path(args.aggregate_out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl") as xw:
        ag.to_excel(xw, index=False, sheet_name="by_subject")
    print(f"Wrote aggregation to {out_path} with {len(ag)} subjects.")

if __name__ == "__main__":
    main()
