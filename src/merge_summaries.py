"""Merge assets/tests/docs summaries, engines, and LLM testing data into repo_data and write a combined CSV.

Defaults assume the repository layout:
- data/repo_data_manual_analysis.csv
- data/assets_summary.csv
- data/tests_summary_manual_analysis.csv
- data/docs_summary.csv
- data/engines_summary.csv
- data/llm_testing_data.csv

Output is written to data/merged_repo_data.csv by default.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Final

import pandas as pd


def read_csv_or_empty(path: Path) -> pd.DataFrame:
    """Read a CSV file if it exists; otherwise return an empty DataFrame.

    Returning an empty DataFrame allows downstream left-joins to be no-ops
    without special-casing missing inputs.
    """
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def rename_with_prefix(df: pd.DataFrame, prefix: str, exclude: set[str]) -> pd.DataFrame:
    """Prefix all column names except those in exclude.

    This avoids name collisions (e.g., GitHub link columns) when merging.
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


def aggregate_docs_llm(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    
    df = df.copy()
    df['Projeto'] = df['Projeto'].str.strip()
    
    aggregated = df.groupby('Projeto').agg({
        'Nome arquivo': lambda x: '; '.join(x.unique()),
        'LLM Description': lambda x: '; '.join(x.dropna().unique()),
    }).reset_index()
    
    file_counts = df.groupby('Projeto')['Nome arquivo'].nunique().reset_index()
    file_counts.columns = ['Projeto', 'docs_llm_file_count']
    
    aggregated = aggregated.merge(file_counts, on='Projeto', how='left')
    
    aggregated = aggregated.rename(columns={
        'Projeto': 'Project',
        'Nome arquivo': 'docs_llm_files',
        'LLM Description': 'docs_llm_descriptions',
    })
    
    return aggregated


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge summaries into repo_data.csv")
    cwd: Final[Path] = Path.cwd()
    default_root: Final[Path] = cwd
    data_dir: Final[Path] = default_root / "data"

    parser.add_argument(
        "--repo-data",
        type=Path,
        default=data_dir / "repo_data.csv",
        help="Path to repo_data.csv",
    )
    parser.add_argument(
        "--engine",
        type=Path,
        default=data_dir / "engines_summary.csv",
        help="Path to engines_summary.csv",
    )
    parser.add_argument(
        "--assets",
        type=Path,
        default=data_dir / "assets_summary.csv",
        help="Path to assets_summary.csv",
    )
    parser.add_argument(
        "--tests",
        type=Path,
        default=data_dir / "tests_summary_manual_analysis.csv",
        help="Path to tests_summary_manual_analysis.csv",
    )
    parser.add_argument(
        "--docs",
        type=Path,
        default=data_dir / "docs_summary.csv",
        help="Path to docs_summary.csv",
    )
    parser.add_argument(
        "--docs-llm",
        type=Path,
        default=data_dir / "llm_testing_data.csv",
        help="Path to llm_testing_data.csv",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=data_dir / "merged_repo_data.csv",
        help="Output CSV path",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    repo_df = read_csv_or_empty(args.repo_data)
    if repo_df.empty:
        raise SystemExit(f"Base repo_data not found or empty: {args.repo_data}")

    # Ensure the join key exists in base
    if "Repo_Name" not in repo_df.columns:
        raise SystemExit("Column 'Repo_Name' not found in repo_data.csv")

    # Read summaries
    assets_df = read_csv_or_empty(args.assets)
    tests_df = read_csv_or_empty(args.tests)
    docs_df = read_csv_or_empty(args.docs)
    engines_df = read_csv_or_empty(args.engine)
    llm_testing_df = read_csv_or_empty(args.docs_llm)

    # Normalize and prefix columns (keep the common join key unprefixed)
    # Assets/tests/docs use 'Project' for repo slug
    if not assets_df.empty and "Project" in assets_df.columns:
        assets_df = rename_with_prefix(assets_df, prefix="assets_", exclude={"Project"})
    if not tests_df.empty and "Project" in tests_df.columns:
        tests_df = rename_with_prefix(tests_df, prefix="tests_", exclude={"Project"})
    if not docs_df.empty and "Project" in docs_df.columns:
        # Keep 'Project' for join; also avoid clashing with base's GitHub columns
        docs_df = rename_with_prefix(docs_df, prefix="docs_", exclude={"Project"})
    
    # Process engines data
    if not engines_df.empty and "Project" in engines_df.columns:
        engines_df = rename_with_prefix(engines_df, prefix="engine_", exclude={"Project"})
    
    # Process LLM testing data
    if not llm_testing_df.empty and "Project" in llm_testing_df.columns:
        llm_testing_df = rename_with_prefix(llm_testing_df, prefix="llm_testing_", exclude={"Project"})

    # Perform merges
    merged = repo_df.copy()
    merged = merge_on_repo_name(merged, assets_df, right_key="Project", suffixes=("", "_assets"))
    merged = merge_on_repo_name(merged, tests_df, right_key="Project", suffixes=("", "_tests"))
    merged = merge_on_repo_name(merged, docs_df, right_key="Project", suffixes=("", "_docs"))
    merged = merge_on_repo_name(merged, engines_df, right_key="Project", suffixes=("", "_engine"))
    merged = merge_on_repo_name(merged, llm_testing_df, right_key="Project", suffixes=("", "_llm_testing"))

    # Write output
    args.output.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(args.output, index=False)
    print(f"Wrote merged data to: {args.output}")


if __name__ == "__main__":
    main()


