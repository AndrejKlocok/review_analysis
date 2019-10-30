from ufal.morphodita import Tagger, Forms, TaggedLemmas, TokenRanges
from stop_words import get_stop_words
from external.czech_stemmer import cz_stem

import re
from re import finditer
import json


class Preproces:
    def __init__(self):
        self.smiles = {}
        self.regex_str = r'(:-.|:.)'
        self.regex = re.compile(self.regex_str)
        self.emoji_dict = {":-)" : "šťastný", ":)" : "šťastný",
                           ":-D": "velmi šťastný", ":D": "velmi šťastný",
                           ":-(" : "smutný", ":(" : "smutný",
                           ":-*": "polibek", ":*": "polibek",
                           ":-O": "překvapený", ":O": "překvapený", ":o": "překvapený", ":0":"překvapený",
                           ":3" : "kočičí tvář",
                           ":/" : "lehce naštvaný", ":-/" : "lehce naštvaný",
                           ":-P": "vyčnívající jazyk", ":P": "vyčnívající jazyk", ":p": "vyčnívající jazyk",
                           ":c" : "velmi smutné", ":C" : "velmi smutné",
                           }

    def find_emoji(self, text):
        for emoji in re.findall(self.regex_str, text):
            if emoji not in self.smiles:
                self.smiles[emoji] = 0
            else:
                self.smiles[emoji] += 1

    def replace_emoji(self, text):
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
    def __init__(self, lemma, tag):
        self.lemma = lemma
        self.tag = tag

    def __str__(self):
        return json.dumps(self.__dict__, ensure_ascii=False).encode('utf8').decode()


class MorphoTagger:
    def __init__(self):
        self.tagger_path = None
        self.tagger = None
        self.tokenizer = None
        self.flexible = ['A', 'D', 'N', 'P', 'V']
        self.preprocesor = Preproces()

    def load_tagger(self, path):
        self.tagger = Tagger.load(path)
        if self.tagger is None:
            raise Exception("[morpho_tagger] Wrong path in tagger")

        self.tokenizer = self.tagger.newTokenizer()
        if self.tagger is None:
            raise Exception("[morpho_tagger] tokenizer not created")

    def pos_tagging(self, text, stem=True):
        lemmas = TaggedLemmas()
        tokens = TokenRanges()
        forms = Forms()
        sentences = []

        # replace smileys
        text = self.preprocesor.replace_emoji(text)

        # lower all text
        text = text.lower()

        # POS taging
        self.tokenizer.setText(text)
        while self.tokenizer.nextSentence(forms, tokens):
            sentence = []
            self.tagger.tag(forms, lemmas)
            for i in range(len(lemmas)):
                lemma = lemmas[i].lemma
                tag = lemmas[i].tag

                # we want to work with flexible POS, thus we dont need stop words
                if tag[0] not in self.flexible:
                    continue

                # remove additional informations
                lemma =  lemma.split("_")[0]
                lemma = re.sub(r'-\d*$', '',lemma)

                # Stem
                if stem:
                    lemma = cz_stem(lemma)
                sentence.append(WordPos(lemma, tag))
            sentences.append(sentence)

        return sentences
