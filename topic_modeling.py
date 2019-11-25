import argparse
from functools import reduce
#np.random.seed(time.time_ns())
from gensim import corpora, models
from os import listdir
from os.path import isfile, join
from utils.morpho_tagger import MorphoTagger


class LDA_model:
    def __init__(self, path):
        self.sentences = []
        self.sentences_pos = []
        self.bow_corpus = {}
        self.tf_idf_corpus = None
        self.dictionary = {}
        self.lda_model = None
        self.lda_model_tfidf = None

        with open(path, "r", encoding='utf-8') as file:
            for line in file:
                self.sentences.append(line[:-1])

        assert (len(self.sentences) > 0), "Empty file"

    def create_lda_model(self, tagger:MorphoTagger, topics=10):
        self.sentences_pos = self.preprocess(tagger)
        self.bow_model()
        self.tf_idf()
        self.lda(num_topics=topics,debug=True)

    def preprocess(self, tagger:MorphoTagger, sentences=None):
        sentences_pos = []
        if not sentences:
            sentences = self.sentences

        for sentence in sentences:
            s = []
            l = reduce(lambda x,y: x+y, tagger.pos_tagging(sentence, False))
            for wp in l:
                if wp.tag[0] in ['N']:
                    s.append(wp.lemma)
            sentences_pos.append(s)
        return sentences_pos

    def bow_model(self):
        self.dictionary = corpora.Dictionary(self.sentences_pos)
        self.dictionary.filter_extremes(no_below=15, no_above=0.8, keep_n=100000)
        self.bow_corpus = [self.dictionary.doc2bow(doc) for doc in self.sentences_pos]

    def tf_idf(self):
        tfidf = models.TfidfModel(self.bow_corpus)
        self.tf_idf_corpus = tfidf[self.bow_corpus]

    def lda(self, num_topics=10, debug=False):
        self.lda_model = models.LdaMulticore(self.bow_corpus, num_topics=num_topics, id2word=self.dictionary, passes=10)
        if debug:
            for idx, topic in self.lda_model.print_topics(-1):
                print('Topic: {} \nWords: {}'.format(idx, topic))

        #self.lda_model_tfidf = models.LdaMulticore(self.tf_idf_corpus, num_topics=num_topics, id2word=self.dictionary, passes=10,
        #                                             workers=4)
        # if debug:
        #    for idx, topic in self.lda_model_tfidf.print_topics(-1):
        #        print('Topic: {} Word: {}'.format(idx, topic))
        #self.lda_model_tfidf.save("model.lda")

    def test_model(self, test_file_path, tagger):
        sentences = []
        with open(test_file_path, "r", encoding='utf-8') as file:
            for line in file:
                sentences.append(line[:-1])
        sentences_preprocess = self.preprocess(tagger, sentences)
        res = open("results.tsv", "w", encoding='utf-8')
        for i, sentence in enumerate(sentences_preprocess):
                v = self.dictionary.doc2bow(sentence)
                index, score = sorted(self.lda_model[v], key=lambda tup: -1 * tup[1])[0]
                res.write(sentences[i] + "\t" + str(index) + "\t"+ self.lda_model.print_topic(index, 4)+'\n')


def main():
    parser = argparse.ArgumentParser(
        description="LDA topic modeling")
    parser.add_argument('-dir', '--input_dir', help='Directory with documents')
    parser.add_argument('-in', '--input_file', help='File from which we generate topic modeling')
    parser.add_argument('-t', '--topics', help='Count of topics', default=1)
    parser.add_argument('-test', '--test', help='Test model on sentences')
    args = vars(parser.parse_args())

    tagger = MorphoTagger()
    tagger.load_tagger("external/morphodita/czech-morfflex-pdt-161115-no_dia-pos_only.tagger")
    if args['input_file']:
        model = LDA_model(args['input_file'])
        model.create_lda_model(tagger, args['topics'])
        if args['test']:
            model.test_model(args['test'], tagger)

    elif args['input_dir']:
        try:
            onlyfiles = [f for f in listdir(args['input_dir']) if isfile(join(args['input_dir'], f))]
            for file in onlyfiles:
                try:
                    print(file)
                    model = LDA_model(args['input_dir']+ file)
                    model.create_lda_model(tagger, args['topics'])
                except Exception as e:
                    print("[onlyfiles] Exception: "+ str(e))

        except Exception as e:
            print("[main] Exception: "+str(e))
        


if __name__ == '__main__':
    main()