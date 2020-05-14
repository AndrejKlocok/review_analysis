import re
from re import finditer
import json
from unidecode import unidecode
from ufal.morphodita import Tagger, Forms, TaggedLemmas, TokenRanges
from stop_words import get_stop_words
from .czech_stemmer import cz_stem


class Preprocess:
    """
    Class handles pre processing of the text.
    """
    def __init__(self):
        self.smiles = {}
        self.stop_words = [unidecode(w) for w in get_stop_words('cz')]
        self.regex_str = r'(:-.|:.)'
        self.regex = re.compile(self.regex_str)
        self.emoji_dict = {":-)": "šťastný", ":)": "šťastná_tvář",
                           ":-D": "velmi_šťastná_tvář", ":D": "velmi_šťastná_tvář",
                           ":-(": "smutný", ":(": "smutná_tvář",
                           ":-*": "líbající_tvář", ":*": "líbající_tvář",
                           ":-O": "překvapená_tvář", ":O": "překvapená_tvář", ":o": "překvapená_tvář",
                           ":0": "překvapená_tvář",
                           ":3": "kočičí_tvář",
                           ":/": "lehce_naštvaná_tvář", ":-/": "lehce_naštvaná_tvář",
                           ":-P": "vyčnívající_jazyk", ":P": "vyčnívající_jazyk", ":p": "vyčnívající_jazyk",
                           ":c": "velmi_smutná_tvář", ":C": "velmi_smutná_tvář",
                           }
        self.wrong_categories = ["tras", "putov", "portal"]

    def find_emoji(self, text: str):
        """
        Find emoji according to dictionary emoji_dict
        :param text: input text
        :return:
        """
        for emoji in re.findall(self.regex_str, text):
            if emoji not in self.smiles:
                self.smiles[emoji] = 0
            else:
                self.smiles[emoji] += 1

    def replace_emoji(self, text: str):
        """
        Replace emoji from input text
        :param text:
        :return:
        """
        to_replace = {}
        # Find smileys
        for match in finditer(self.regex_str, text):
            m = match.group()
            if m in self.emoji_dict:
                if m not in to_replace:
                    to_replace[m] = self.emoji_dict[m]
        # Replace them
        for key, value in to_replace.items():
            text = text.replace(key, " " + value + " ")

        return text


class WordPos:
    """
    Class holds words lemma and morphological tag within itself.
    """
    def __init__(self, lemma, tag, token=None):
        self.lemma = lemma
        self.tag = tag
        self.token = token

    def __str__(self):
        return json.dumps(self.__dict__, ensure_ascii=False).encode('utf8').decode()


class MorphoTagger:
    def __init__(self):
        self.tagger_path = None
        self.tagger = None
        self.tokenizer = None
        self.flexible = ['A', 'D', 'N', 'P', 'V', 'C']
        self.preprocesor = Preprocess()
        self.pos_wp = WordPos("pozitivn", "AA")
        self.neg_wp = WordPos("negativn", "AA")

    def load_tagger(self, path: str):
        """
        Load morphodita tagger from path
        :param path:
        :return:
        """
        self.tagger = Tagger.load(path)
        if self.tagger is None:
            raise Exception("[morpho_tagger] Wrong path in tagger")
        # create tokenizer
        self.tokenizer = self.tagger.newTokenizer()
        if self.tagger is None:
            raise Exception("[morpho_tagger] tokenizer not created")

    def pos_tagging(self, text: str, stem=False, preprocess=True):
        """
        Perform pos tagging of given text
        :param text: input text
        :param stem: use stem of word or just lemma
        :param preprocess: use preprocess
        :return: list of list of tagged words: List[List[WordPos]]
        """
        lemmas = TaggedLemmas()
        tokens = TokenRanges()
        forms = Forms()
        sentences = []

        vanilla_text = text
        # remove diacritic
        text = unidecode(text)
        if preprocess:
            # remove stop words
            text = " ".join([w if w not in self.preprocesor.stop_words else "" for w in text.split()])
            # lower all text
            text = text.lower()
            # replace smileys
            text = self.preprocesor.replace_emoji(text)
            vanilla_text = text

        # POS taging
        self.tokenizer.setText(text)
        while self.tokenizer.nextSentence(forms, tokens):
            sentence = []
            self.tagger.tag(forms, lemmas)
            for i in range(len(lemmas)):
                lemma = lemmas[i].lemma
                tag = lemmas[i].tag
                token = tokens[i]
                token_text = vanilla_text[token.start:token.start+token.length]
                # remove diacritic
                lemma = unidecode(lemma)
                # eng flag
                eng_word = False

                # '-' is not boundary token
                # boundary token
                if tag[0] == "Z" and lemma != "-":
                    if not preprocess:
                        sentence.append(WordPos(lemma, tag, token_text))
                    if sentence:
                        sentences.append(sentence)
                    sentence = []
                    continue
                # dont stem english words
                if lemma.find("angl") != -1:
                    eng_word = True

                # remove additional informations
                lemma = lemma.split("_")[0]
                lemma = re.sub(r'-\d*$', '', lemma)

                # Stem
                if stem and not eng_word:
                    lemma = cz_stem(lemma)
                if lemma and not preprocess or len(lemma) > 2:
                    sentence.append(WordPos(lemma, tag, token_text))
            if sentence:
                sentences.append(sentence)

        return sentences
