from sklearn.decomposition import PCA
from fse.models import Average, SIF, uSIF
from fse import IndexedList
from sklearn.metrics import silhouette_score
from gensim.models.fasttext import load_facebook_model, FastText
from enum import Enum
from nltk.cluster import KMeansClusterer
from datetime import datetime

import numpy as np, random, nltk


class EmbeddingType(Enum):
    sentence_vectors = 1
    distance_matrix = 2
    similarity_matrix = 3


class ClusterMethod(Enum):
    kmeans = 1


class FastTextConfig:
    def __init__(self, embedding_size, window_size, min_word, down_sampling):
        self.embedding_size = embedding_size
        self.window_size = window_size
        self.min_word = min_word
        self.down_sampling = down_sampling


class FastTextModel:
    def __init__(self):
        try:
            self.pretrained_model = load_facebook_model('../model/fasttext/cc.cs.300.bin')
        except Exception as e:
            self.pretrained_model = None

    @staticmethod
    def get_pretrained():
        return load_facebook_model('../model/fasttext/cc.cs.300.bin')

    def __create_model(self, sentences_processed, pretrained: bool = False, model_conf: FastTextConfig = None):
        if not model_conf:
            model_conf = FastTextConfig(300, 10, 5, 1e-2)

        if pretrained:
            model = self.pretrained_model
        else:
            model = FastText(sentences_processed,
                             size=model_conf.embedding_size,
                             window=model_conf.window_size,
                             min_count=model_conf.min_word,
                             sample=model_conf.down_sampling,
                             sg=1,
                             iter=100)
            model.init_sims(replace=True)
            # model = Average(model)

        model = SIF(model, workers=10)
        model.train(IndexedList(sentences_processed))
        return model

    def __create_sim_dist_matrix(self, sentences: list, model: SIF, isDistance: bool, doSave: bool = False):
        # sentence2vec similarities
        matrix = []
        sentences_len = len(sentences)
        for i in range(sentences_len):
            vec = []
            for j in range(sentences_len):
                val = model.sv.similarity(i, j)

                if val < 0.0:
                    val = 0.0
                if val > 1.0:
                    val = 1.0
                if isDistance:
                    vec.append(1.0 - val)
                else:
                    vec.append(val)
            matrix.append(np.array(vec))

        matrix = np.array(matrix)
        np.fill_diagonal(matrix, 0)

        if doSave:
            if isDistance:
                np.save('dist_matrix.npy', matrix)
            else:
                np.save('sim_matrix.npy', matrix)

        return matrix

    def __kmeans(self, matrix: np.array, num_clusters: int):
        rng = random.Random(datetime.now())
        kclusterer = KMeansClusterer(num_clusters, distance=nltk.cluster.util.cosine_distance, repeats=60,
                                     avoid_empty_clusters=True, rng=rng)

        labels = kclusterer.cluster(matrix, assign_clusters=True)

        return labels

    def __sentence_vectors(self, sentences: list, model: SIF, doSave: bool = False):
        sentences_len = len(sentences)
        matrix = []
        i = 0
        for vector in model.sv:
            i += 1
            matrix.append(np.array(vector))
            if i == sentences_len:
                break
        matrix = np.array(matrix)
        np.fill_diagonal(matrix, 0)

        if doSave:
            np.save('vectors_matrix.npy', matrix)

        return matrix

    def cluster_similarity(self, sentences_pos: list, pretrained: bool, embedding: EmbeddingType,
                           cluster: ClusterMethod, cluster_cnt: int):
        model = self.__create_model(sentences_pos, pretrained, None)

        if embedding.distance_matrix:
            matrix = self.__create_sim_dist_matrix(sentences_pos, model, isDistance=True, doSave=False)

        elif embedding.similarity_matrix:
            matrix = self.__create_sim_dist_matrix(sentences_pos, model, isDistance=False, doSave=False)

        elif embedding.sentence_vectors:
            matrix = self.__sentence_vectors(sentences_pos, model, doSave=False)
        else:
            return None

        if cluster.kmeans:
            labels = self.__kmeans(matrix, cluster_cnt)
        else:
            raise NotImplementedError()

        return labels
