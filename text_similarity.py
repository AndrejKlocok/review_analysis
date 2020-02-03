import numpy as np
import matplotlib.pyplot as plt
import nltk
from utils.morpho_tagger import MorphoTagger
from gensim.models.fasttext import FastText
from functools import reduce
import argparse


class FastTextConfig:
    def __init__(self, embedding_size, window_size, min_word, down_sampling):
        self.embedding_size = embedding_size
        self.window_size = window_size
        self.min_word = min_word
        self.down_sampling = down_sampling


class FastTextSimilarityModel:
    def __init__(self, file_path, conf):
        self.sentences = []
        self.sentences_pos = []
        self.model_conf = conf
        self.model = None

        with open(file_path, "r", encoding='utf-8') as file:
            for line in file:
                self.sentences.append(line[:-1])

    def preprocess(self, tagger: MorphoTagger, sen=None):
        sentences_pos = []

        if not sen:
            sentences = self.sentences
        else:
            sentences = sen

        for sentence in sentences:
            s = []
            l = reduce(lambda x, y: x + y, tagger.pos_tagging(sentence, False))
            for idx, wp in enumerate(l):
                    s.append(wp.lemma)
            sentences_pos.append(s)

        if not sen:
            self.sentences_pos = sentences_pos
            return []
        else:
            return sentences_pos

    def train_similarity(self):
        #word_punctuation_tokenizer = nltk.WordPunctTokenizer()
        #word_tokenized_corpus = [word_punctuation_tokenizer.tokenize(sentence) for sentence in self.sentences_pos]
        self.model = FastText(self.sentences_pos,
                            size=self.model_conf.embedding_size,
                            window=self.model_conf.window_size,
                            min_count=self.model_conf.min_word,
                            sample=self.model_conf.down_sampling,
                            sg=1,
                            iter=100)
        self.model.wmdistance()





def main():
    parser = argparse.ArgumentParser(
        description="Fastext clustering")
    parser.add_argument('-in', '--input_file', help='Input text file.')

    args = vars(parser.parse_args())
    tagger = MorphoTagger()
    tagger.load_tagger("external/morphodita/czech-morfflex-pdt-161115-no_dia-pos_only.tagger")

    conf = FastTextConfig(60, 5, 5, 1e-2)

    if args['input_file']:
        fastTextModel = FastTextSimilarityModel(args['input_file'], conf)
        fastTextModel.preprocess(tagger)
        fastTextModel.train_similarity()

        semantically_similar_words = {words: [item[0] for item in fastTextModel.model.wv.most_similar([words], topn=5)]
                                      for words in
                                      ['cena', 'nic', 'doba', 'zatím', 'vysavač', 'hlučný']}

        for k, v in semantically_similar_words.items():
            print(k + ":" + str(v))


if __name__ == '__main__':
    main()