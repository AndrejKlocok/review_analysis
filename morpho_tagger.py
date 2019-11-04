from ufal.morphodita import Tagger, Forms, TaggedLemmas, TokenRanges
from stop_words import get_stop_words
from external.czech_stemmer import cz_stem

import re
from re import finditer
import json
from enum import Enum
from unidecode import unidecode


class Preproces:
    def __init__(self):
        self.smiles = {}
        self.stop_words = [unidecode(w) for w in get_stop_words('cz')]
        self.regex_str = r'(:-.|:.)'
        self.regex = re.compile(self.regex_str)
        self.emoji_dict = {":-)": "šťastný", ":)": "šťastný",
                           ":-D": "velmi šťastný", ":D": "velmi šťastný",
                           ":-(": "smutný", ":(": "smutný",
                           ":-*": "polibek", ":*": "polibek",
                           ":-O": "překvapený", ":O": "překvapený", ":o": "překvapený", ":0": "překvapený",
                           ":3": "kočičí tvář",
                           ":/": "lehce naštvaný", ":-/": "lehce naštvaný",
                           ":-P": "vyčnívající jazyk", ":P": "vyčnívající jazyk", ":p": "vyčnívající jazyk",
                           ":c": "velmi smutné", ":C": "velmi smutné",
                           }
        self.wrong_categories = ["tras", "putov", "portal"]

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


class RevEnum(Enum):
    pos = 1
    con = 2
    sum = 3


class MorphoTagger:
    def __init__(self):
        self.tagger_path = None
        self.tagger = None
        self.tokenizer = None
        self.flexible = ['A', 'D', 'N', 'P', 'V', 'C']
        self.preprocesor = Preproces()
        self.pos_wp = WordPos("pozitivn", "AA")
        self.neg_wp = WordPos("negativn", "AA")

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

        # remove diacritic
        text = unidecode(text)

        # remove stop words
        text = " ".join([w if w not in self.preprocesor.stop_words else "" for w in text.split()])

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

                # remove diacritic
                lemma = unidecode(lemma)
                # eng flag
                eng_word = False

                # '-' is not boundary token
                # boundary token
                if tag[0] == "Z" and lemma != "-":
                    sentences.append(sentence)
                    sentence = []
                # we want to work with flexible POS, thus we dont need stop words
                if tag[0] not in self.flexible:
                    continue

                # dont stem english words
                if lemma.find("angl") != -1:
                    # m = re.search(r'angl\._\w*', lemma)
                    # if m:
                    #    lemma = m.group().split("_")[1]
                    # else:
                    eng_word = True

                # remove additional informations
                lemma = lemma.split("_")[0]
                lemma = re.sub(r'-\d*$', '', lemma)

                # Stem
                if stem and not eng_word:
                    lemma = cz_stem(lemma)
                sentence.append(WordPos(lemma, tag))
            sentences.append(sentence)

        return sentences

    def parse_review(self, review: dict, stem=True):
        pos_stem = self.pos_tagging(". ".join(review["pros"]), stem)
        i = 0
        for sentence in pos_stem:
            found_sentiment = False
            for stem in sentence:
                if stem.tag[0] == 'A':
                    found_sentiment = True
                    break
            if not found_sentiment:
                pos_stem[i] = [self.pos_wp] + pos_stem[i]
            i += 1

        con_stem = self.pos_tagging(". ".join(review["cons"]), stem)
        i = 0
        for sentence in con_stem:
            found_sentiment = False
            for stem in sentence:
                if stem.tag[0] == 'A':
                    found_sentiment = True
                    break
            if not found_sentiment:
                con_stem[i] = [self.neg_wp] + con_stem[i]

        sum_stem = self.pos_tagging(review["summary"], stem)

        return pos_stem + con_stem + sum_stem


def main():
    tagger = MorphoTagger()
    tagger.load_tagger("external/morphodita/czech-morfflex-pdt-161115-no_dia-pos_only.tagger")

    s = tagger.pos_tagging("games")
    for sentence in s:
        for wp in sentence:
            print(wp)
    pass


if __name__ == '__main__':
    main()
