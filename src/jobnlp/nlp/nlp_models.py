
import spacy
from spacy.language import Language
from functools import lru_cache

from jobnlp.utils.logger import Logger

class SpacyModel:
    '''
    Factory for SpaCy pre-trained models.
    '''
    def __init__(self, log: Logger, default_lang="es", default_size="sm"):
        self.default_lang = default_lang
        self.default_size = default_size
        self.model_map = {
            ("es", "sm"): "es_core_news_sm",
            ("es", "md"): "es_core_news_md",
            ("es", "lg"): "es_core_news_lg",
            ("en", "sm"): "en_core_web_sm",
            ("en", "md"): "en_core_web_md",
            ("en", "lg"): "en_core_web_lg",
        }
        self.log = log

    def get_model_name(self, lang: str, size: str) -> str:
        key = (lang, size)
        if key not in self.model_map:
            raise ValueError(f"Model not defined by lang={lang}, size={size}")
        return self.model_map[key]

    def get_model(self, lang=None, size=None) -> Language:
        lang = lang or self.default_lang
        size = size or self.default_size
        model_name = self.get_model_name(lang, size)
        try:
            self.log.info(f"Load spacy model: '{model_name}'")
            return spacy.load(model_name)
        except OSError:
            self.log.error((f"An attempt was made to load the '{model_name}'" 
                       "model, not installed."))
            raise RuntimeError(
                (f"Model '{model_name}' not installed. "
                 f"Exec: python -m spacy download {model_name}")
            )

    def get_blank_mod(self, lang: str=None):
        lang = lang or self.default_lang
        try:
            self.log.info(f"Load spacy model, lang = {lang}")
            return spacy.blank(lang)        
        except OSError as e:
            self.log.error(f"Error at load '{lang}' blank model")
            raise e
            
    @staticmethod
    @lru_cache(maxsize=None)
    def get_model_cached_static(model_name: str) -> Language:
        return spacy.load(model_name)

    def get_model_cached(self, lang=None, size=None) -> Language:
        lang = lang or self.default_lang
        size = size or self.default_size
        model_name = self.get_model_name(lang, size)
        return self.get_model_cached_static(model_name)