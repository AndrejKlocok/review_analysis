import argparse
from functools import reduce
from gensim import corpora, models
from os import listdir
from os.path import isfile, join
import pyLDAvis
import pyLDAvis.gensim
import pandas as pd
import matplotlib.pyplot as plt
import sys
sys.path.append('../')
from utils.morpho_tagger import MorphoTagger


class LDA_model:
    def __init__(self, args, path=None):
        self.sentences = []
        self.sentences_pos = []
        self.bow_corpus = {}
        self.tf_idf_corpus = None
        self.dictionary = {}
        self.lda_model = None
        self.lda_model_tfidf = None
        self.document_topic = None
        self.num_topics = args['topics']
        self.ngram = int(args['ngram'])
        self.justNouns = args['nouns']
        if not path:
            path = args['input_file']
        with open(path, "r", encoding='utf-8') as file:
            for line in file:
                self.sentences.append(line[:-1])

        assert (len(self.sentences) > 0), "Empty file"

    def create_lda_model(self, tagger: MorphoTagger):
        self.sentences_pos = self.preprocess(tagger)
        self.bow_model()
        self.tf_idf()
        self.lda(debug=True)

    def preprocess(self, tagger: MorphoTagger, sentences=None):
        sentences_pos = []
        if not sentences:
            sentences = self.sentences

        for sentence in sentences:
            s = []
            l = reduce(lambda x, y: x + y, tagger.pos_tagging(sentence, False))
            for idx, wp in enumerate(l):
                # use just nouns
                if self.justNouns:
                    if wp.tag[0] in ['N']:
                        s.append(wp.lemma)
                # ngram method
                elif self.ngram > 1:
                    if idx + 1 < len(l):
                        s.append(wp.lemma+'_'+l[idx + 1].lemma)
                # use all words
                else:
                    s.append(wp.lemma)
            sentences_pos.append(s)
        return sentences_pos

    def bow_model(self):
        self.dictionary = corpora.Dictionary(self.sentences_pos)
        self.dictionary.filter_extremes(no_below=10, no_above=0.5, keep_n=100000)
        self.bow_corpus = [self.dictionary.doc2bow(doc) for doc in self.sentences_pos]

    def tf_idf(self):
        tfidf = models.TfidfModel(self.bow_corpus)
        self.tf_idf_corpus = tfidf[self.bow_corpus]

    def topics_document_to_dataframe(self, topics_document, num_topics):
        res = pd.DataFrame(columns=range(num_topics))
        for topic_weight in topics_document:
            res.loc[0, topic_weight[0]] = topic_weight[1]
        return res

    def lda(self, debug=False):
        self.lda_model_tfidf = models.LdaMulticore(self.tf_idf_corpus, num_topics=self.num_topics, id2word=self.dictionary,
                                                   passes=10, workers=4)
        if debug:
            for idx, topic in self.lda_model_tfidf.print_topics(-1):
                print('Topic: {} Word: {}'.format(idx, topic))
        # self.lda_model_tfidf.save("model.lda")

        topics = [self.lda_model_tfidf[self.tf_idf_corpus[i]] for i in range(len(self.sentences))]
        self.document_topic = pd.concat([self.topics_document_to_dataframe(topics_document, num_topics=int(self.num_topics))
                                         for topics_document in topics]).reset_index(drop=True).fillna(0)

    def test_model(self, test_file_path, tagger):
        sentences = []
        with open(test_file_path, "r", encoding='utf-8') as file:
            for line in file:
                sentences.append(line[:-1])
        res = None
        try:
            res = open("results.tsv", "w", encoding='utf-8')
            sentences_preprocess = self.preprocess(tagger, sentences)
            lda_model = self.lda_model if self.lda_model else self.lda_model_tfidf
            for i, sentence in enumerate(sentences_preprocess):
                v = self.dictionary.doc2bow(sentence)
                d_sorted = sorted(lda_model[v], key=lambda tup: -1 * tup[1])
                index, score = d_sorted[0]
                index_sub, score_sub = (-1, 0)
                try:
                    index_sub, score_sub = d_sorted[1]
                except:
                    pass
                res.write(sentences[i] + "\t" + str(index)+"-{:.2f}".format(score) + "\t" + str(index_sub)+"-{:.2f}".format(score_sub) + "\t" + lda_model.print_topic(index, 4) + '\n')
        except Exception as e:
            print('[test_model] Exception: ' + str(e), file=sys.stderr)

        finally:
            res.close()

    def display(self):
        import seaborn as sns
        sns.set(rc={'figure.figsize': (10, 5)})
        self.document_topic.idxmax(axis=1).value_counts().plot.bar(color='lightblue')
        plt.show()

        vis = pyLDAvis.gensim.prepare(topic_model=self.lda_model_tfidf, corpus=self.tf_idf_corpus,
                                      dictionary=self.dictionary)
        from IPython.core.display import HTML
        html: HTML = pyLDAvis.display(vis)
        with open('out.html', 'w') as f:
            f.write(html.data)

        #print(vis.topic_info)


def main():
    import warnings
    warnings.filterwarnings("ignore", module="matplotlib")
    parser = argparse.ArgumentParser(
        description="LDA topic modeling")
    parser.add_argument('-dir', '--input_dir', help='Directory with documents')
    parser.add_argument('-in', '--input_file', help='File from which we generate topic modeling')
    parser.add_argument('-tsv', '--tsv', help='Get topics from tsv', action='store_true')
    parser.add_argument('-t', '--topics', help='Count of topics', default=1)
    parser.add_argument('-test', '--test', help='Test model on sentences')
    parser.add_argument('-ngram', '--ngram', help='Ngrams', default=1)
    parser.add_argument('-d', '--display', help='Display model', action='store_true')
    parser.add_argument('-nouns', '--nouns', help='Use just nouns', action='store_true', default=False)
    args = vars(parser.parse_args())

    tagger = MorphoTagger()
    tagger.load_tagger("../external/morphodita/czech-morfflex-pdt-161115-no_dia-pos_only.tagger")

    if args['tsv']:
        model = LDA_model(args)
        sentences = {}
        for sentence in model.sentences:
            s = sentence.split('\t')
            if s[1] not in sentences:
                sentences[s[1]] = []
            sentences[s[1]].append(s[0])

        for key in sorted(sentences.keys()):
            value = sentences[key]
            print('Cluster: {} '.format(key))
            model.sentences = value
            model.create_lda_model(tagger)

        #model.sentences
    elif args['input_file']:
        model = LDA_model(args)
        model.create_lda_model(tagger)
        if args['test']:
            model.test_model(args['test'], tagger)
        if args['display']:
            model.display()

    elif args['input_dir']:
        try:
            onlyfiles = [f for f in listdir(args['input_dir']) if isfile(join(args['input_dir'], f))]
            for file in onlyfiles:
                try:
                    print(file)
                    model = LDA_model(args, path = args['input_dir'] + file)
                    model.create_lda_model(tagger)
                except Exception as e:
                    print("[onlyfiles] Exception: " + str(e))

        except Exception as e:
            print("[main] Exception: " + str(e))


if __name__ == '__main__':
    main()
