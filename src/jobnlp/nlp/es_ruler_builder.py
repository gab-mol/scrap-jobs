'''
Spanish model builder script with rules.
'''
from pathlib import Path

import jobnlp
from jobnlp.nlp.nlp_custom import NLPRules
from jobnlp.nlp.nlp_models import SpacyModel
from jobnlp.utils.logger import setup_logging, get_logger

DIR = Path(jobnlp.__file__).parent
MOD_RUL_PATH = DIR / "nlp" / "models" / "rules_es"
PATT_PATH = DIR / "nlp" / "patterns" / "job_ruler_patterns.jsonl"
LOG_PATH = Path("log/es_ruler_builder.log")

setup_logging(logfile=LOG_PATH)
log = get_logger(__name__)

def main():
    get_models = SpacyModel(log=log)
    nlp = get_models.get_blank_mod()

    nlp_rul = NLPRules(nlp, log)
    nlp_rul.load_patterns(PATT_PATH)
    print("Created")
    nlp_rul.save_model(MOD_RUL_PATH)

if __name__ == "__main__":

    main()