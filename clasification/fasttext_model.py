from sklearn.decomposition import PCA
from fse.models import Average, SIF
from fse import IndexedList
from sklearn.metrics import silhouette_score
from gensim.models.fasttext import load_facebook_model, FastText
import numpy as np
from enum import Enum


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
        self.pretrained_model = None  # load_facebook_model('../model/fasttext/cc.cs.300.bin')
        pass

    def create_model(self, sentences_processed, pretrained: bool = False, model_conf: FastTextConfig = None):
        if not model_conf:
            model_conf = FastTextConfig(100, 10, 5, 1e-2)

        model = None

        if pretrained:
            model = FastText(sentences_processed,
                             size=model_conf.embedding_size,
                             window=model_conf.window_size,
                             min_count=model_conf.min_word,
                             sample=model_conf.down_sampling,
                             sg=1,
                             iter=100)
            model.init_sims(replace=True)
            model = Average(model)
        else:
            model = self.pretrained_model

        model = SIF(model, workers=10)
        model.train(IndexedList(sentences_processed))
        return model

    def create_sim_dist_matrix(self, fastTextModel, model):
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

    def cluster_similarity_matrix(self, sentences: list, cluster_cnt: int, pretrained: bool, embedding: EmbeddingType,
                                  cluster: ClusterMethod):

        model = self.create_model(sentences, pretrained, None)

        if embedding.distance_matrix:
            pass
        elif embedding.similarity_matrix:
            pass
        elif embedding.sentence_vectors:
            pass
        else:
            return None

        pass
