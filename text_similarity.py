import numpy as np
import matplotlib.pyplot as plt
import nltk, os
from utils.morpho_tagger import MorphoTagger
from gensim.models.fasttext import FastText
from functools import reduce
import argparse
from wordcloud import WordCloud
from time import time
from gensim.similarities import WmdSimilarity
from nltk.cluster import KMeansClusterer
from datetime import datetime
import random
from collections import Counter
from sklearn.decomposition import PCA
from fse.models import Average, SIF
from fse import IndexedList

class FastTextConfig:
    def __init__(self, embedding_size, window_size, min_word, down_sampling):
        self.embedding_size = embedding_size
        self.window_size = window_size
        self.min_word = min_word
        self.down_sampling = down_sampling


class FastTextSimilarityModel:
    def __init__(self, file_path, conf):
        def _load(file, l):
            with open(file_path + file, "r", encoding='utf-8') as file:
                for line in file:
                    line = line[:-1]
                    # just different sentences
                    if line not in l:
                        l.append(line)

        self.sentences_pos = []
        self.sentences_pos_processed = []
        self.sentences_neg = []
        self.sentences_neg_processed = []
        self.model_conf = conf
        self.model_pos = None
        self.model_neg = None

        _load('dataset_positive.txt', self.sentences_pos)
        _load('dataset_negative.txt', self.sentences_neg)

        self.sentences_pos_len = len(self.sentences_pos)
        self.sentences_neg_len = len(self.sentences_neg)

        self.sentences_pos_len = 500
        self.sentences_pos = self.sentences_pos[:self.sentences_pos_len]

    def preprocess(self, tagger: MorphoTagger, sentences):
        sentences_processed = []

        for sentence in sentences:
            s = []
            l = reduce(lambda x, y: x + y, tagger.pos_tagging(sentence, False))
            for idx, wp in enumerate(l):
                s.append(wp.lemma)
            sentences_processed.append(s)

        return sentences_processed

    def train_similarity(self, sentences_processed):
        model = FastText(sentences_processed,
                         size=self.model_conf.embedding_size,
                         window=self.model_conf.window_size,
                         min_count=self.model_conf.min_word,
                         sample=self.model_conf.down_sampling,
                         sg=1,
                         iter=100)
        model.init_sims(replace=True)
        return model

    def set_pos(self, sentences):
        self.sentences_pos_processed = sentences

    def set_neg(self, sentences):
        self.sentences_neg_processed = sentences

    def set_pos_model(self, model):
        self.model_pos = model

    def set_neg_model(self, model):
        self.model_neg = model

    def word_cloud(self, sentences, category, className):
        tokens = [token for sentence in sentences for token in sentence]
        text = ' '.join(tokens)
        wordcloud = WordCloud(max_font_size=40, width=600,
                              height=400, background_color='white',
                              max_words=200, relative_scaling=1.0).generate_from_text(text)

        plt.imshow(wordcloud, interpolation="bilinear")
        plt.axis("off")
        wordcloud.to_file('./tmp/' + category + '-' + className + '.jpg')


def main():
    tagger = MorphoTagger()
    tagger.load_tagger("external/morphodita/czech-morfflex-pdt-161115-no_dia-pos_only.tagger")

    conf = FastTextConfig(300, 10, 5, 1e-2)

    fastTextModel = FastTextSimilarityModel("./", conf)
    fastTextModel.set_pos(fastTextModel.preprocess(tagger, fastTextModel.sentences_pos))
    # fastTextModel.set_neg(fastTextModel.preprocess(tagger, fastTextModel.sentences_neg))

    fastTextModel.set_pos_model(fastTextModel.train_similarity(fastTextModel.sentences_pos_processed))
    # fastTextModel.set_neg_model(fastTextModel.train_similarity(fastTextModel.sentences_neg_processed))

    # model = Average(fastTextModel.model_pos)
    model = SIF(fastTextModel.model_pos)
    model.train(IndexedList(fastTextModel.sentences_pos_processed))

    sentences_vectors = []
    i = 0
    for vector in model.sv:
        i += 1
        sentences_vectors.append(vector)
        if i == 500:
            break

    s = IndexedList(fastTextModel.sentences_pos_processed)
    model.sv.most_similar(0, indexable=s.items)

    # sentence2vec similarities
    sim_matrix = []
    dist_matrix = []

    for i in range(fastTextModel.sentences_pos_len):
        vec_sims = []
        vec_dists = []
        for j in range(fastTextModel.sentences_pos_len):
            val = model.sv.similarity(i, j)
            if val < 0.0:
                val = 0.0
            if val > 1.0:
                val = 1.0
            vec_sims.append(val)
            vec_dists.append(1.0 - val)
        sim_matrix.append(np.array(vec_sims))
        dist_matrix.append(np.array(vec_dists))

    print(len(sim_matrix))
    print(len(dist_matrix))
    print(len(sentences_vectors))

    dist_matrix = np.array(dist_matrix)
    sim_matrix = np.array(sim_matrix)

    num_clusters = 15
    rng = random.Random(datetime.now())
    kclusterer = KMeansClusterer(num_clusters, distance=nltk.cluster.util.cosine_distance, repeats=60,
                                 avoid_empty_clusters=True, rng=rng)

    labels = kclusterer.cluster(sim_matrix, assign_clusters=True)
    cnt = Counter(labels)
    print(cnt)

    with open('kmeans_wmd_similarity_cos' + str(num_clusters) + '.tsv', 'w') as file:
        for j, sen in enumerate(fastTextModel.sentences_pos):
            file.write(sen + '\t' + str(labels[j]) + '\n')


if __name__ == '__main__':
    main()