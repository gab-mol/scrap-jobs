import json
from pathlib import Path
from spacy.language import Language
from spacy.pipeline import EntityRuler
from spacy.tokens import Doc

class NLPRules:
    '''
    Manage models that implement the EntityRuler class for rule-based NLP.  
    Rules added through this class have priority over entities found by the ner 
    element.
    '''
    def __init__(
            self, nlp: Language, 
            ruler_name: str = "entity_ruler"):
        '''
        nlp: SpaCy Language object.
        ruler_name: EntityRuler component name.
        '''
        self.nlp = nlp
        self.ruler_name = ruler_name
        self.ruler: EntityRuler

        if ruler_name in self.nlp.pipe_names:
            self.ruler = self.nlp.get_pipe(ruler_name)
        else:
            # NOTE: ensure priority of rules over ner
            self.ruler = self.nlp.add_pipe("entity_ruler", 
                                           name=ruler_name, 
                                           before="ner")

    def add_patterns(self, patterns: list[dict]) -> None:
        '''
        Add a list of patterns to the EntityRuler.
        Each pattern must be a SpaCy-compatible dictionary.
        '''
        self.ruler.add_patterns(patterns)

    def save_patterns(self, filepath: str | Path) -> None:
        '''
        Saves the current rules of the EntityRuler as a JSON file.
        '''
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.ruler.patterns, f, ensure_ascii=False, indent=2)

    def load_patterns(self, filepath: str | Path) -> None:
        '''
        Loads rules from a JSON file and adds them to the EntityRuler.
        '''
        path = Path(filepath)
        if not path.is_file():
            raise FileNotFoundError(f"El archivo {filepath} no existe.")
        with open(path, "r", encoding="utf-8") as f:
            patterns = json.load(f)
        self.add_patterns(patterns)

    def __call__(self, text: str) -> Doc:
        '''
        Processes text using the pipeline with EntityRuler.
        '''
        return self.nlp(text)


class NLPTrain:
    '''Placeholder for future training logic.'''
    pass