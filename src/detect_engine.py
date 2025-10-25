from __future__ import annotations

from os import path, walk, listdir
import csv
import json
import re

# Reuse the same root/output convention from list_files.py
ROOT_DIR = "/Users/shuvi/Documents/GitHub/cnpq_project/all_games"
OUTPUT_CSV = "/Users/shuvi/Documents/GitHub/cnpq_project/data/engines_summary.csv"


def _safe_read_text(file_path: str, max_bytes: int = 200_000) -> str:
    try:
        with open(file_path, "rb") as f:
            data = f.read(max_bytes)
        return data.decode(errors="ignore")
    except Exception:
        return ""


def _file_exists(p: str) -> bool:
    return path.isfile(p)


def _any_exists(base: str, candidates: list[str]) -> bool:
    for c in candidates:
        if path.exists(path.join(base, c)):
            return True
    return False


def detect_engine(project_path: str) -> tuple[str, float, str]:
    """Detects game engine/framework for a single project directory.

    Returns a tuple of (engine_or_framework, confidence [0-1], evidence_string).
    """
    # Common indicators
    # Unity
    if _any_exists(project_path, ["ProjectSettings/ProjectVersion.txt", "Packages/manifest.json"]):
        evidence = []
        pv = path.join(project_path, "ProjectSettings/ProjectVersion.txt")
        if _file_exists(pv):
            text = _safe_read_text(pv)
            if "m_EditorVersion" in text or "m_EditorVersionWithRevision" in text:
                evidence.append("ProjectVersion.txt")
        manifest = path.join(project_path, "Packages/manifest.json")
        if _file_exists(manifest):
            text = _safe_read_text(manifest)
            if "com.unity" in text:
                evidence.append("Packages/manifest.json")
        if evidence:
            return ("Unity", 0.98, ", ".join(evidence) or "Assets folder present")

    # Godot
    godot_proj = path.join(project_path, "project.godot")
    if _file_exists(godot_proj):
        text = _safe_read_text(godot_proj)
        if "config_version" in text or "[application]" in text:
            return ("Godot", 0.99, "project.godot")

    # Unreal Engine
    if any(fname.endswith(".uproject") for fname in listdir(project_path) if not fname.startswith(".")):
        return ("Unreal Engine", 0.98, ".uproject file")

    # Ren'Py
    if _any_exists(project_path, ["renpy", "game/script.rpy", "game/options.rpy"]):
        return ("Ren'Py", 0.95, "renpy folder or .rpy files in game/")

    # LÖVE (love2d)
    if _any_exists(project_path, ["main.lua", "conf.lua"]):
        return ("LÖVE", 0.8, "main.lua/conf.lua")

    # Pygame
    py_requirements = path.join(project_path, "requirements.txt")
    if _file_exists(py_requirements):
        text = _safe_read_text(py_requirements).lower()
        if "pygame" in text or "arcade" in text:
            return ("Pygame/Python", 0.75, "requirements.txt contains pygame/arcade")

    # Phaser (JS/TS)
    pkg_json = path.join(project_path, "package.json")
    if _file_exists(pkg_json):
        try:
            pkg = json.loads(_safe_read_text(pkg_json))
        except Exception:
            pkg = {}
        deps = {**pkg.get("dependencies", {}), **pkg.get("devDependencies", {})}
        deps_lower = {k.lower(): v for k, v in deps.items()}
        if "phaser" in deps_lower:
            return ("Phaser", 0.9, "package.json dependency phaser")
        if "pixi.js" in deps_lower or "pixi" in deps_lower:
            return ("PixiJS", 0.8, "package.json dependency pixi")

    # MonoGame / XNA
    csproj_files = [f for f in listdir(project_path) if f.endswith(".csproj")] if path.isdir(project_path) else []
    for csproj in csproj_files:
        text = _safe_read_text(path.join(project_path, csproj))
        if "MonoGame.Framework" in text or "Microsoft.Xna.Framework" in text:
            return ("MonoGame/XNA", 0.9, f"{csproj} references MonoGame/XNA")

    # libGDX (Java)
    build_gradle = path.join(project_path, "build.gradle")
    gradle_kts = path.join(project_path, "build.gradle.kts")
    if _file_exists(build_gradle) or _file_exists(gradle_kts):
        text = _safe_read_text(build_gradle) + _safe_read_text(gradle_kts)
        if "com.badlogicgames.gdx" in text:
            return ("libGDX", 0.9, "Gradle uses com.badlogicgames.gdx")

    # Cocos2d
    if _any_exists(project_path, ["cocos2d", "frameworks/cocos2d-x", "cocos2d.h"]):
        return ("Cocos2d", 0.7, "cocos2d indicators")

    # GameMaker
    if any(fname.endswith(".yyp") for fname in listdir(project_path) if not fname.startswith(".")):
        return ("GameMaker", 0.95, ".yyp project file")

    # Defold
    if _file_exists(path.join(project_path, "game.project")):
        return ("Defold", 0.9, "game.project")

    # RPG Maker
    if any(fname.endswith(ext) for ext in (".rvproj2", ".rvproj", ".rpgproject") for fname in listdir(project_path)):
        return ("RPG Maker", 0.85, "RPG Maker project file")

    # HaxeFlixel
    project_xml = path.join(project_path, "Project.xml")
    if _file_exists(project_xml):
        text = _safe_read_text(project_xml)
        if re.search(r"flixel", text, re.IGNORECASE):
            return ("HaxeFlixel", 0.85, "Project.xml mentions flixel")

    # Bevy (Rust)
    cargo = path.join(project_path, "Cargo.toml")
    if _file_exists(cargo):
        text = _safe_read_text(cargo).lower()
        if "bevy" in text:
            return ("Bevy", 0.9, "Cargo.toml depends on bevy")

    # Construct
    if any(fname.endswith(".c3p") for fname in listdir(project_path) if not fname.startswith(".")):
        return ("Construct", 0.85, ".c3p project file")

    # Fallback when unknown
    return ("Unknown", 0.0, "No known engine signatures found")


def scan_all_projects_and_write_csv():
    if not path.isdir(ROOT_DIR):
        print(f"Root directory not found: {ROOT_DIR}")
        return

    headers = ["Project", "EngineOrFramework", "Confidence", "Evidence"]

    with open(OUTPUT_CSV, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for project in listdir(ROOT_DIR):
            if project.startswith("."):
                continue
            project_path = path.join(ROOT_DIR, project)
            if not path.isdir(project_path):
                continue
            engine, confidence, evidence = detect_engine(project_path)
            writer.writerow([project, engine, f"{confidence:.2f}", evidence])

    print(f"Engine detection finished. Results saved to {OUTPUT_CSV}")


if __name__ == "__main__":
    scan_all_projects_and_write_csv()


