from os import path, walk, listdir, makedirs
import csv
import shutil
import pandas as pd

ROOT_DIR = "/Users/shuvi/Documents/GitHub/cnpq_project/all_games"
OUTPUT_CSV = "/Users/shuvi/Documents/GitHub/cnpq_project/data/docs_summary.csv"

CSV_FILE = "/Users/shuvi/Documents/GitHub/cnpq_project/data/repo_data.csv"
COLUMN_NAME_LINKS = "Github_links"

DOCS_OUTPUT_DIR = "/Users/shuvi/Documents/GitHub/cnpq_project/docs_copied"

# Special documentation file names
SPECIAL_DOCS = {"contributing", "readme", "development", "testing"}

# Keywords to detect test mentions inside docs
TEST_KEYWORDS = {"test", "tests", "testing", "unittest", "unit test", "integration test", "automated test"}


def file_mentions_tests(file_path: str) -> bool:
    """Check if file mentions tests by scanning content."""
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read().lower()
            return any(keyword in content for keyword in TEST_KEYWORDS)
    except Exception as e:
        print(f"⚠️ Could not read {file_path}: {e}")
        return False


def scan_project(project_path: str, project_name: str):
    """Return markdown files, special docs, mentions, and copy docs."""
    markdown_files = []
    special_docs = []
    mentions_tests = False

    for root, dirs, files in walk(project_path):
        dirs[:] = [d for d in dirs if not d.startswith(".")]  # skip hidden dirs

        for file in files:
            if file.startswith("."):
                continue

            name, ext = path.splitext(file)
            file_path = path.join(root, file)
            relative_path = path.relpath(file_path, project_path)  # path relative to project root

            # Markdown files
            if ext.lower() == ".md":
                markdown_files.append(relative_path)
                if file_mentions_tests(file_path):
                    mentions_tests = True
                copy_doc_file(file_path, project_name)

            # Special docs (like README, CONTRIBUTING without extension)
            if name.lower() in SPECIAL_DOCS and ".md" not in ext.lower():
                special_docs.append(relative_path)
                if file_mentions_tests(file_path):
                    mentions_tests = True
                copy_doc_file(file_path, project_name)

    return markdown_files, special_docs, mentions_tests


def copy_doc_file(file_path: str, project_name: str):
    """Copy documentation file into docs_copied/project_name/ folder."""
    dest_dir = path.join(DOCS_OUTPUT_DIR, project_name)
    makedirs(dest_dir, exist_ok=True)
    try:
        shutil.copy(file_path, dest_dir)
    except Exception as e:
        print(f"⚠️ Could not copy {file_path}: {e}")


def scan_all_projects():
    if not path.isdir(ROOT_DIR):
        print(f"Root directory not found: {ROOT_DIR}")
        return

    # Load GitHub links and build mapping: repo_name -> full_url
    df_links = pd.read_csv(CSV_FILE)
    project_links = {}
    for url in df_links[COLUMN_NAME_LINKS].dropna():
        repo_name = url.rstrip("/").split("/")[-1]
        project_links[repo_name] = url

    headers = ["Project", "GithubLink", "HasDocs", "MarkdownFiles", "SpecialDocs", "MentionsTests?"]

    with open(OUTPUT_CSV, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for project in listdir(ROOT_DIR):
            if project.startswith("."):
                continue

            project_path = path.join(ROOT_DIR, project)
            if path.isdir(project_path):
                markdown_files, special_docs, mentions_tests = scan_project(project_path, project)

                row = [
                    project,
                    project_links.get(project, ""),  # map by repo name from URL
                    "Yes" if markdown_files or special_docs else "No",
                    "; ".join(markdown_files) if markdown_files else "",
                    "; ".join(special_docs) if special_docs else "",
                    "Yes" if mentions_tests else "No"
                ]
                writer.writerow(row)

    print(f"✅ Documentation + test mention detection finished. Results saved to {OUTPUT_CSV}")
    print(f"📂 Docs copied into: {DOCS_OUTPUT_DIR}")


if __name__ == "__main__":
    scan_all_projects()
