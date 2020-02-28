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
from sklearn.metrics import silhouette_score
from gensim.models.fasttext import load_facebook_model


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

    def save(self):
        from gensim.test.utils import get_tmpfile
        pos_name = get_tmpfile("fasttext-pos.model")
        neg_name = get_tmpfile("fasttext-neg.model")

        self.model_pos.save(pos_name)
        if self.model_neg:
            self.model_neg.save(neg_name)


def kmeans(matrix, fastTextModel, num_clusters, do_write=True):
    rng = random.Random(datetime.now())
    kclusterer = KMeansClusterer(num_clusters, distance=nltk.cluster.util.cosine_distance, repeats=60,
                                 avoid_empty_clusters=True, rng=rng)

    labels = kclusterer.cluster(matrix, assign_clusters=True)
    cnt = Counter(labels)

    print(silhouette_score(matrix, labels, metric='precomputed'))

    if do_write:
        print(cnt)
        with open('kmeans_cos' + str(num_clusters) + '.tsv', 'w') as file:
            for j, sen in enumerate(fastTextModel.sentences_pos):
                file.write(sen + '\t' + str(labels[j]) + '\n')
    return labels


def check_kmeans(matrix, fastTextModel, kmax=18):
    sil = []

    for k in range(8, kmax + 1):
        print('{} Iteration'.format(k))
        labels = kmeans(matrix, fastTextModel, k, False)
        sil.append(silhouette_score(matrix, labels, metric='precomputed'))

    for val in sil:
        print(val)


def load_matrixes(path):
    sim = np.load(path + 'sim_matrix.npy')
    dist = np.load(path + 'dist_matrix.npy')
    return sim, dist


def create_sim_dist_matrix(fastTextModel, model):
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

    dist_matrix = np.array(dist_matrix)
    sim_matrix = np.array(sim_matrix)

    return sim_matrix, dist_matrix


def db_scan(matrix, fastTextModel):
    from sklearn.cluster import DBSCAN

    clustering = DBSCAN(eps=0.3, metric='precomputed', min_samples=10, algorithm='brute').fit(matrix)

    labels = clustering.labels_
    no_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    print(no_clusters)

    with open('DBSCAN_wmd_sim.tsv', 'w') as file:
        for j, sen in enumerate(fastTextModel.sentences_pos):
            file.write(sen + '\t' + str(labels[j]) + '\n')


def algomerative(dist_matrix, fastTextModel):
    from sklearn.cluster import AgglomerativeClustering

    clustering = AgglomerativeClustering(affinity='precomputed', linkage='ward', n_clusters=5)

    labels = clustering.fit_predict(dist_matrix)

    no_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    print(no_clusters)

    with open('AgglomerativeClustering_wmd.tsv', 'w') as file:
        for j, sen in enumerate(fastTextModel.sentences_pos):
            file.write(sen + '\t' + str(labels[j]) + '\n')


def affinity(dist_matrix, fastTextModel):
    from sklearn.cluster import AffinityPropagation

    clustering = AffinityPropagation(damping=0.7, affinity='precomputed', convergence_iter=20).fit(dist_matrix)

    labels = clustering.labels_

    no_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    print(no_clusters)

    with open('AffinityPropagation_wmd_sim.tsv', 'w') as file:
        for j, sen in enumerate(fastTextModel.sentences_pos):
            file.write(sen + '\t' + str(labels[j]) + '\n')

def main():
    tagger = MorphoTagger()
    tagger.load_tagger("external/morphodita/czech-morfflex-pdt-161115-no_dia-pos_only.tagger")

    conf = FastTextConfig(300, 10, 5, 1e-2)

    fastTextModel = FastTextSimilarityModel("./", conf)
    fastTextModel.set_pos(fastTextModel.preprocess(tagger, fastTextModel.sentences_pos))
    # fastTextModel.set_neg(fastTextModel.preprocess(tagger, fastTextModel.sentences_neg))

    # fastTextModel.set_pos_model(fastTextModel.train_similarity(fastTextModel.sentences_pos_processed))
    # fastTextModel.set_neg_model(fastTextModel.train_similarity(fastTextModel.sentences_neg_processed))
    #ft = load_facebook_model('../model/fasttext/cc.cs.300.bin')
    model = Average(fastTextModel.model_pos)
    model = SIF(ft, workers=10)
    model.train(IndexedList(fastTextModel.sentences_pos_processed))

    # sentences_vectors = []
    # i = 0
    # for vector in model.sv:
    #    i += 1
    #    sentences_vectors.append(vector)
    #    if i == fastTextModel.sentences_pos_len:
    #        break

    # s = IndexedList(fastTextModel.sentences_pos_processed)
    # model.sv.most_similar(0, indexable=s.items)

    sim_matrix, dist_matrix = load_matrixes('tmp/kmeans_300d_cz_cc_500s/')
    sim_matrix, dist_matrix = create_sim_dist_matrix(fastTextModel, model)

    # fastTextModel.save()
    # model.save('fse.model')
    np.fill_diagonal(sim_matrix, 0)
    np.fill_diagonal(dist_matrix, 0)
    # np.save('dist_matrix.npy', dist_matrix)
    # np.save('sim_matrix.npy', sim_matrix)

    # kmeans(dist_matrix, fastTextModel, 22, do_write=True)
    # check_kmeans(dist_matrix, fastTextModel, 30)


if __name__ == '__main__':
    main()
