"""Merge engines_summary.csv and llm_testing_data.csv into merged_repo_data copy.csv.

This script reads an existing merged_repo_data copy.csv file and adds columns from:
- data/engines_summary.csv (engine/framework information)
- data/llm_testing_data.csv (testing practices data)

The merge is performed using the 'Project' column from the CSVs and 'Repo_Name' from the base file.
Output overwrites the input file by default.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Final

import pandas as pd


def read_csv_or_empty(path: Path) -> pd.DataFrame:
    """Read a CSV file if it exists; otherwise return an empty DataFrame."""
    if not path.exists():
        print(f"Warning: File not found: {path}")
        return pd.DataFrame()
    return pd.read_csv(path)


def rename_with_prefix(df: pd.DataFrame, prefix: str, exclude: set[str]) -> pd.DataFrame:
    """Prefix all column names except those in exclude.
    
    This avoids name collisions when merging.
    """
    if df.empty:
        return df
    rename_map: dict[str, str] = {
        c: f"{prefix}{c}" for c in df.columns if c not in exclude
    }
    return df.rename(columns=rename_map)


def merge_on_repo_name(
    base: pd.DataFrame,
    right: pd.DataFrame,
    right_key: str,
    suffixes: tuple[str, str] = ("_x", "_y"),
) -> pd.DataFrame:
    """Left-merge `right` onto `base` using `Repo_Name` == `right_key`.
    
    If the right DataFrame is empty or missing the key, the merge is skipped.
    """
    if right.empty or right_key not in right.columns or base.empty:
        return base
    return base.merge(right, how="left", left_on="Repo_Name", right_on=right_key, suffixes=suffixes)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge engines and LLM testing data into merged_repo_data.csv"
    )
    cwd: Final[Path] = Path.cwd()
    default_root: Final[Path] = cwd
    data_dir: Final[Path] = default_root / "data"

    parser.add_argument(
        "--base",
        type=Path,
        default=data_dir / "merged_repo_data.csv",
        help="Path to base merged_repo_data.csv",
    )
    parser.add_argument(
        "--engines",
        type=Path,
        default=data_dir / "engines_summary.csv",
        help="Path to engines_summary.csv",
    )
    parser.add_argument(
        "--llm-testing",
        type=Path,
        default=data_dir / "llm_testing_data.csv",
        help="Path to llm_testing_data.csv",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output CSV path (defaults to overwriting the base file)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Set output to base file if not specified
    if args.output is None:
        args.output = args.base

    # Read base file
    base_df = read_csv_or_empty(args.base)
    if base_df.empty:
        raise SystemExit(f"Base file not found or empty: {args.base}")

    # Ensure the join key exists in base
    if "Repo_Name" not in base_df.columns:
        raise SystemExit("Column 'Repo_Name' not found in base file")

    print(f"Loaded base file with {len(base_df)} rows")

    # Read engines and LLM testing data
    engines_df = read_csv_or_empty(args.engines)
    llm_testing_df = read_csv_or_empty(args.llm_testing)

    # Process engines data
    if not engines_df.empty and "Project" in engines_df.columns:
        print(f"Processing engines data: {len(engines_df)} rows")
        engines_df = rename_with_prefix(engines_df, prefix="engine_", exclude={"Project"})
    else:
        print("Warning: engines_summary.csv is empty or missing 'Project' column")

    # Process LLM testing data
    if not llm_testing_df.empty and "Project" in llm_testing_df.columns:
        print(f"Processing LLM testing data: {len(llm_testing_df)} rows")
        llm_testing_df = rename_with_prefix(llm_testing_df, prefix="llm_testing_", exclude={"Project"})
    else:
        print("Warning: llm_testing_data.csv is empty or missing 'Project' column")

    # Perform merges
    merged = base_df.copy()
    merged = merge_on_repo_name(merged, llm_testing_df, right_key="Project", suffixes=("", "_llm_testing"))
    merged = merge_on_repo_name(merged, engines_df, right_key="Project", suffixes=("", "_engine"))
    

    # Write output
    args.output.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(args.output, index=False)
    print(f"\nWrote merged data to: {args.output}")
    print(f"Total columns: {len(merged.columns)}")
    print(f"Total rows: {len(merged)}")


if __name__ == "__main__":
    main()
