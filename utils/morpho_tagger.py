import argparse
import re
from re import finditer
import json
from enum import Enum
from unidecode import unidecode
from ufal.morphodita import Tagger, Forms, TaggedLemmas, TokenRanges
from stop_words import get_stop_words
from .czech_stemmer import cz_stem


class Preprocess:
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
    def __init__(self, lemma, tag, token=None):
        self.lemma = lemma
        self.tag = tag
        self.token = token

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
        self.preprocesor = Preprocess()
        self.pos_wp = WordPos("pozitivn", "AA")
        self.neg_wp = WordPos("negativn", "AA")

    def load_tagger(self, path=None):
        if not path:
            path = "/home/andrej/Documents/school/Diplomka/review_analysis/external/morphodita/czech-morfflex-pdt-161115-no_dia-pos_only.tagger"

        self.tagger = Tagger.load(path)
        if self.tagger is None:
            raise Exception("[morpho_tagger] Wrong path in tagger")

        self.tokenizer = self.tagger.newTokenizer()
        if self.tagger is None:
            raise Exception("[morpho_tagger] tokenizer not created")

    def pos_tagging(self, text, stem=True, preprocess=True):
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
                # we want to work with flexible POS, thus we dont need stop words
                # if tag[0] not in self.flexible:
                #    continue

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
                if lemma and not preprocess or len(lemma) > 2:
                    sentence.append(WordPos(lemma, tag, token_text))
            if sentence:
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
    from termcolor import colored

    parser = argparse.ArgumentParser(
        description="Script morpho tagger")
    # parser.add_argument('-file', '--file', help='Aspect file', required=True)
    # parser.add_argument('-name', '--name', help='Name of aspect category', required=True)
    # parser.add_argument('-w', '--words', help='Key words')
    # args = vars(parser.parse_args())

    # file = open(args["file"], "a")

    tagger = MorphoTagger()
    tagger.load_tagger()

    # aspect = {"name":args["name"], "lemma_list":[]}
    # s = tagger.pos_tagging(args["words"])#"ceny hodnota financne finančně drahý korun czk")
    while True:
        try:
            query = input(colored('your query: ', 'green'))
            s = tagger.pos_tagging(query)
            for sentence in s:
                for wp in sentence:
                    print(wp.lemma)

        except Exception as e:
            print("[tagger] Exception: " + str(e))

            # if wp.lemma not in aspect["lemma_list"]:
            # aspect["lemma_list"].append(wp.lemma)
    # file.write(str(aspect))


if __name__ == '__main__':
    main()
