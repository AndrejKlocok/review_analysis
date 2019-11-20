import argparse, time
from functools import reduce
import numpy as np
#np.random.seed(time.time_ns())
from gensim import corpora, models

from morpho.morpho_tagger import MorphoTagger


class LDA_model:
    def __init__(self, path):
        self.sentences = []
        self.sentences_pos = []
        self.bow_corpus = {}
        self.tf_idf_corpus = None
        self.dictionary = {}
        self.lda_model = None
        self.lda_model_tfidf = None

        with open(path, "r") as file:
            for line in file:
                self.sentences.append(line[:-1])

        assert (len(self.sentences) > 0), "Empty file"

    def create_lda_model(self, tagger:MorphoTagger):
        self.preprocess(tagger)
        self.bow_model()
        self.tf_idf()
        self.lda(debug=True)

    def preprocess(self, tagger:MorphoTagger):
        for sentence in self.sentences:
            self.sentences_pos.append( [wp.lemma for wp in reduce(lambda x,y: x+y, tagger.pos_tagging(sentence))])

    def bow_model(self):
        self.dictionary = corpora.Dictionary(self.sentences_pos)
        self.dictionary.filter_extremes(no_below=15, no_above=0.5, keep_n=100000)
        self.bow_corpus = [self.dictionary.doc2bow(doc) for doc in self.sentences_pos]

    def tf_idf(self):
        tfidf = models.TfidfModel(self.bow_corpus)
        self.tf_idf_corpus = tfidf[self.bow_corpus]

    def lda(self, debug=False):
        self.lda_model = models.LdaMulticore(self.bow_corpus, num_topics=10, id2word=self.dictionary, passes=2, workers=2)
        if debug:
            for idx, topic in self.lda_model.print_topics(-1):
                print('Topic: {} \nWords: {}'.format(idx, topic))

        self.lda_model_tfidf = models.LdaMulticore(self.tf_idf_corpus, num_topics=10, id2word=self.dictionary, passes=2,
                                                     workers=4)
        if debug:
            for idx, topic in self.lda_model_tfidf.print_topics(-1):
                print('Topic: {} Word: {}'.format(idx, topic))


def main():
    parser = argparse.ArgumentParser(
        description="LDA topic modeling")
    parser.add_argument('-in', '--input_file', help='File from which we generate topic modeling', required=True)
    args = vars(parser.parse_args())

    tagger = MorphoTagger()
    tagger.load_tagger("external/morphodita/czech-morfflex-pdt-161115-no_dia-pos_only.tagger")
    model = LDA_model(args['input_file'])
    model.create_lda_model(tagger)


if __name__ == '__main__':
    main()