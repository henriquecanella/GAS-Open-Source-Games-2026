import os
import subprocess
import pandas as pd

# --- Configuration ---
CSV_FILE = "/Users/shuvi/Documents/GitHub/cnpq_project/data/repo_data_manual_analysis.csv"   # Path to your CSV file
OUTPUT_DIR = "/Users/shuvi/Documents/GitHub/cnpq_project/all_games"    # Directory where repos will be cloned
COLUMN_NAME = "Github_links"  # Column name in CSV

def main():
    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Load CSV file
    df = pd.read_csv(CSV_FILE)

    if COLUMN_NAME not in df.columns:
        print(f"❌ Column '{COLUMN_NAME}' not found in CSV.")
        return

    # Iterate over each GitHub link with original row indices
    for i, (row_idx, raw_link) in enumerate(df[COLUMN_NAME].items(), start=1):
        # Skip missing or empty links
        if pd.isna(raw_link):
            continue
        link = str(raw_link).strip()
        if not link:
            continue

        print(f"[{i}] Cloning {link}...")

        try:
            subprocess.run(
                ["git", "clone", link],
                cwd=OUTPUT_DIR,
                check=True
            )
            print(f"✅ Successfully cloned {link}")
        except subprocess.CalledProcessError:
            print(f"❌ Failed to clone {link}")
            # Remove the corresponding row from the DataFrame and persist the CSV immediately
            df.drop(index=row_idx, inplace=True)
            df.to_csv(CSV_FILE, index=False)
            print(f"🗑️ Removed row {row_idx} from CSV due to clone failure.")

if __name__ == "__main__":
    main()
