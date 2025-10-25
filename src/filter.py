import pandas as pd
import re
from fuzzywuzzy import fuzz

def filter_csv():
    input_csv = pd.read_csv('./data/1_initial_itch_data.csv')

    github_pattern = re.compile(r'^(http://|https://|http://www\.|https://www\.)?github\.com/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+$')

    valid_github_links = input_csv[(input_csv['Github_links'].str.match(github_pattern))].copy()
    valid_github_links['Repo_Name'] = valid_github_links['Github_links'].str.extract(r'github\.com/([^/]+)/([^/]+)')[1]
    valid_github_links = valid_github_links[
        valid_github_links.apply(lambda row: fuzz.ratio(row['Name'], row['Repo_Name']) > 30, axis=1)
    ]

    valid_github_links.to_csv('./data/filtered_itch_data.csv', index=False)


if __name__ == "__main__":
    filter_csv()