from os import path, walk, listdir
import csv

ROOT_DIR = "/Users/shuvi/Documents/GitHub/cnpq_project/all_games"
OUTPUT_CSV = "/Users/shuvi/Documents/GitHub/cnpq_project/data/tests_summary.csv"


TEST_DIR_NAMES = {"test", "tests", "__tests__", "spec", "unittest"}
TEST_FILE_KEYWORDS = {"test", "spec"}


TEST_FILE_EXTS = {
    ".py", ".js", ".ts", ".cs", ".cpp", ".h", ".java",
    ".rb", ".go", ".gd", ".lua", ".c", ".rs"
}

def scan_project(project_path: str):
    test_folders = set()
    test_files = []

    for root, dirs, files in walk(project_path):
        
        dirs[:] = [d for d in dirs if not d.startswith(".")]

        
        folder_name = path.basename(root).lower()
        if folder_name in TEST_DIR_NAMES:
            test_folders.add(folder_name)
            
            for file in files:
                if not file.startswith("."):
                    test_files.append(file)
            continue

        
        for file in files:
            if file.startswith("."):  
                continue
            name, ext = path.splitext(file)
            if ext.lower() in TEST_FILE_EXTS:
                lower_name = name.lower()
                if any(keyword in lower_name for keyword in TEST_FILE_KEYWORDS) or lower_name.endswith("_test"):
                    test_files.append(file)

    return test_folders, test_files


def scan_all_projects():
    if not path.isdir(ROOT_DIR):
        print(f"Root directory not found: {ROOT_DIR}")
        return

    headers = ["Project", "hasSomeTestFolder?", "Test Folders", "hasPotentialTestFiles?", "PotentialTestFiles"]

    with open(OUTPUT_CSV, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for project in listdir(ROOT_DIR):
            if project.startswith("."):  
                continue
            project_path = path.join(ROOT_DIR, project)
            if path.isdir(project_path):
                test_folders, test_files = scan_project(project_path)

                row = [
                    project,
                    "Yes" if test_folders else "No",
                    "; ".join(sorted(test_folders)) if test_folders else "",
                    "Yes" if test_files else "No",
                    "; ".join(test_files) if test_files else "",
                ]
                writer.writerow(row)

    print(f"✅ Test detection finished. Results saved to {OUTPUT_CSV}")


if __name__ == "__main__":
    scan_all_projects()
