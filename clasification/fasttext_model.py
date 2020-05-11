from fse.models import Average, SIF, uSIF
from fse import IndexedList
from gensim.models.fasttext import load_facebook_model, FastText
from enum import Enum
from nltk.cluster import KMeansClusterer
from datetime import datetime

import numpy as np, random, nltk


class EmbeddingType(Enum):
    """
    Enum represents embedding type for text.
    """
    sentence_vectors = 1
    distance_matrix = 2
    similarity_matrix = 3


class ClusterMethod(Enum):
    """
    Enum represents used clustering algorithms.
    """
    kmeans = 1


class EmbeddingModel(Enum):
    """
    Enum represents use of pre-trained models.
    """
    fasttext_pretrained = 1
    fasttext_300d = 2


class FastTextConfig:
    """
    Class represents configuration for fast text model
    """
    def __init__(self, embedding_size, window_size, min_word, down_sampling):
        self.embedding_size = embedding_size
        self.window_size = window_size
        self.min_word = min_word
        self.down_sampling = down_sampling


class FastTextModel:
    """
    Class handles operations with fasttext model during sentence clustering.
    """
    def __init__(self):
        try:
            self.pretrained_model = load_facebook_model('../model/fasttext/cc.cs.300.bin')
        except Exception as e:
            self.pretrained_model = None

    @staticmethod
    def get_pretrained():
        """
        Load pre trained model.
        :return: FastText
        """
        return load_facebook_model('../model/fasttext/cc.cs.300.bin')

    def __create_model(self, sentences_processed, embedding_model, model_conf: FastTextConfig = None):
        """
        Create SIF model from fasttext with usage of pre-trained model or train model on given sentences.
        :param sentences_processed: list of lemmatized sentences
        :param embedding_model:
        :param model_conf:
        :return:
        """
        if not model_conf:
            model_conf = FastTextConfig(300, 10, 5, 1e-2)

        if embedding_model == EmbeddingModel.fasttext_pretrained and self.pretrained_model:
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

        model = SIF(model, workers=10)
        model.train(IndexedList(sentences_processed))
        return model

    def __create_sim_dist_matrix(self, sentences: list, model: SIF, isDistance: bool, doSave: bool = False):
        """
        Use model to obtain sentence similarity or distance matrix.
        :param sentences: list of lemamtized text of sentence
        :param model: SIF model instance
        :param isDistance: boolean for type of matrix type (metric used)
        :param doSave: option for saving matrix
        :return:
        """
        matrix = []
        sentences_len = len(sentences)
        # loop over sentences
        for i in range(sentences_len):
            vec = []
            for j in range(sentences_len):
                # calculate text similarity between all sentences
                val = model.sv.similarity(i, j)
                # corner cases
                if val < 0.0:
                    val = 0.0
                if val > 1.0:
                    val = 1.0
                # distance
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
        """
        Perform kmeans clustering with given number of clusters and data represented by matrix.
        :param matrix: data matrix
        :param num_clusters:  count of clusters
        :return:
        """
        rng = random.Random(datetime.now())
        kclusterer = KMeansClusterer(num_clusters, distance=nltk.cluster.util.cosine_distance, repeats=60,
                                     avoid_empty_clusters=True, rng=rng)

        labels = kclusterer.cluster(matrix, assign_clusters=True)

        return labels

    def __sentence_vectors(self, sentences: list, model: SIF, doSave: bool = False):
        """
        Ge representation of sentence/text
        :param sentences: list of lemmantized text/sentences.
        :param model: embedding model instance
        :param doSave: option for saving matrix
        :return:
        """
        sentences_len = len(sentences)
        matrix = []
        i = 0
        # get sentence vector representation and store it to matrix
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

    def cluster_similarity(self, sentences_pos: list, embedding_model, embedding: EmbeddingType,
                           cluster: ClusterMethod, cluster_cnt: int):
        """
        Perform sentence clustering on given sentences sentences_pos with configuration given by enums.
        :param sentences_pos:  list of lemmatized sentences
        :param embedding_model: enum value of selected embedding model
        :param embedding: enum value of selected embedding type
        :param cluster: enum value of selected clustering algorithm
        :param cluster_cnt: count of clusters for clustering method
        :return:
        """

        # Get clustering model
        model = self.__create_model(sentences_pos, embedding_model, None)

        # embedding of sentence is calculated as distance of sentence to all other sentences
        if embedding.distance_matrix:
            matrix = self.__create_sim_dist_matrix(sentences_pos, model, isDistance=True, doSave=False)

        # embedding is calculated as similarity of sentence to all other sentences
        elif embedding.similarity_matrix:
            matrix = self.__create_sim_dist_matrix(sentences_pos, model, isDistance=False, doSave=False)

        # embedding is calculated from given sentence vector
        elif embedding.sentence_vectors:
            matrix = self.__sentence_vectors(sentences_pos, model, doSave=False)
        else:
            return None

        # clustering algorithm
        if cluster.kmeans:
            labels = self.__kmeans(matrix, cluster_cnt)
        else:
            raise NotImplementedError()

        return labels
