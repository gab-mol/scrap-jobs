import json
from spacy import load
from pathlib import Path
from spacy.language import Language
from spacy.pipeline import EntityRuler
from spacy.tokens import Doc

from jobnlp.utils.logger import get_logger

log = get_logger(__name__)


class NLPRules:
    '''
    Manage models that implement the EntityRuler class for rule-based NLP.  
    Rules added through this class have priority over entities found by the ner 
    element.
    '''
    def __init__(
            self, nlp: Language, 
            ruler_name: str = "entity_ruler",
            modelpath: Path|None = None):
        '''
        nlp: SpaCy Language object.
        ruler_name: EntityRuler component name.
        '''
        self.nlp = nlp
        self.ruler_name = ruler_name
        self.ruler: EntityRuler
        self.modelpath = modelpath

        before_el = "ner"
        if ruler_name in self.nlp.pipe_names:
            self.ruler = self.nlp.get_pipe(ruler_name)
        else:
            if before_el in self.nlp.pipe_names:
                # NOTE: ensure priority of rules over ner
                self.ruler = self.nlp.add_pipe(
                    ruler_name, 
                    name=ruler_name, 
                    before=before_el
                    )
            else:
                self.ruler = self.nlp.add_pipe(
                    ruler_name, 
                    name=ruler_name, 
                    last=True
                    )


    def add_patterns(self, patterns: list[dict]) -> None:
        '''
        Add a list of patterns to the EntityRuler.
        Each pattern must be a SpaCy-compatible dictionary.
        '''
        self.ruler.add_patterns(patterns)

    def save_patterns(self, rulespath: Path) -> None:
        '''
        Saves the current rules of the EntityRuler as a JSON file.
        '''
        rulespath.parent.mkdir(parents=True, exist_ok=True)
        with open(rulespath, "w", encoding="utf-8") as f:
            json.dump(self.ruler.patterns, f, ensure_ascii=False, 
                      indent=2)
        log.info((f"Save file with rules for pipe '{self.ruler_name}': "
                 f"{rulespath}"))

    def load_patterns(self, rulespath: Path) -> None:
        '''
        Loads rules from a JSON file and adds them to the EntityRuler.
        '''
        if not rulespath.is_file():
            log.error(("Attempted to load rules from file not found:"
                       f"{rulespath}"))
            raise FileNotFoundError(f"{rulespath} does not exist.")
        
        with open(rulespath, "r", encoding="utf-8") as f:
            patterns = json.load(f)

        self.add_patterns(patterns)

        log.info((f"Load rules for pipe '{self.ruler_name}' "
                    f"from file: {rulespath}"))

    def save_model(self, modelpath: Path|None=None):
        if not modelpath and not self.modelpath:
            log.error("Attempted to save model with no route specified.")
            raise TypeError
        if modelpath:
            self.modelpath = modelpath
        
        if not self.nlp:
            log.error("Attempted to save but the NLP model is missing")
            raise TypeError
        
        self.modelpath.parent.mkdir(parents=True, exist_ok=True)
        self.nlp.to_disk(self.modelpath)
        log.info(f"Model save to {self.modelpath}")

    def load_model(self, modelpath: Path|None=None):
        if not modelpath and not self.modelpath:
            log.error("Attempted to load model with no route specified.")
            raise TypeError
        if modelpath:
            if modelpath.exists():
                self.modelpath = modelpath
                log.info(f"Model load from {self.modelpath}")
            else:
                log.error(("An attempt was made to load a model from a "
                          f"directory that was not found: {self.modelpath}."))
                raise FileNotFoundError
            
        self.nlp = load(self.modelpath)

    def __call__(self, text: str) -> Doc:
        '''
        Processes text using the pipeline with EntityRuler.
        '''
        return self.nlp(text)


class NLPTrain:
    '''Placeholder for future training logic.'''
    pass