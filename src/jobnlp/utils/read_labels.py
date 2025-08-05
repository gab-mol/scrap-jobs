import json
from pathlib import Path

def get_labels_from_rules_json(json_path: Path) -> list[str]:
    with open(json_path, "r", encoding="utf-8") as f:
        rules = json.load(f)
    return sorted({rule["label"] for rule in rules})