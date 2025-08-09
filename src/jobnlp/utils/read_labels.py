import json
from pathlib import Path

import jobnlp

PATT_PATH = (Path(jobnlp.__file__).parent / 
             "nlp" / "patterns" / "job_ruler_patterns.jsonl")

def get_labels_from_rules_json(json_path=PATT_PATH) -> list[str]:
    with open(json_path, "r", encoding="utf-8") as f:
        rules = json.load(f)
    return sorted({rule["label"] for rule in rules})