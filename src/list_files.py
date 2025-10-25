from os import path, walk, listdir
from collections import defaultdict
import csv

ROOT_DIR = "/Users/shuvi/Documents/GitHub/cnpq_project/all_games"
OUTPUT_CSV = "/Users/shuvi/Documents/GitHub/cnpq_project/data/assets_summary.csv"

# Asset type definitions
ASSET_TYPES: dict[str, list[str]] = {
    "Images": [".png", ".jpg", ".jpeg", ".bmp", ".tga", ".gif", ".psd", ".svg"],
    "Audio": [".mp3", ".wav", ".ogg", ".flac", ".aac", ".m4a"],
    "3D Models": [".fbx", ".obj", ".dae", ".3ds", ".blend", ".gltf", ".glb"],
    "Shaders": [".shader", ".frag", ".vert", ".glsl", ".hlsl"],
    "Fonts": [".ttf", ".otf", ".fnt"],
    "Videos": [".mp4", ".avi", ".mov", ".webm", ".mkv"],
    "Tilemaps": [".tmx", ".tsx"],
}

def get_asset_type(ext: str) -> str | None:
    """Return the asset category for a given file extension."""
    for category, extensions in ASSET_TYPES.items():
        if ext.lower() in extensions:
            return category
    return None

def scan_project(project_path: str) -> dict[str, int]:
    """Scan a project folder and return a dictionary of asset counts."""
    asset_counts = defaultdict(int)

    for root, dirs, files in walk(project_path):
        dirs[:] = [d for d in dirs if not d.startswith(".")]

        for file in files:
            if file.startswith("."):
                continue
            _, ext = path.splitext(file)
            asset_type = get_asset_type(ext)
            if asset_type:
                asset_counts[asset_type] += 1
            else:
                asset_counts["Unknown"] += 1

    return asset_counts

def scan_all_projects():
    if not path.isdir(ROOT_DIR):
        print(f"Root directory not found: {ROOT_DIR}")
        return

    headers = ["Project"] + list(ASSET_TYPES.keys()) + ["Unknown"]

    with open(OUTPUT_CSV, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for project in listdir(ROOT_DIR):
            if project.startswith("."):
                continue
            project_path = path.join(ROOT_DIR, project)
            if path.isdir(project_path):
                counts = scan_project(project_path)
                row = [project] + [counts.get(asset_type, 0) for asset_type in ASSET_TYPES.keys()] + [counts.get("Unknown", 0)]
                writer.writerow(row)

    print(f"Asset detection finished. Results saved to {OUTPUT_CSV}")

if __name__ == "__main__":
    scan_all_projects()
