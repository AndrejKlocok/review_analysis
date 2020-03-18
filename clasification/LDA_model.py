from gensim import corpora, models
import pyLDAvis
import pyLDAvis.gensim
import pandas as pd
import matplotlib.pyplot as plt
import sys
from review_analysis.utils.morpho_tagger import MorphoTagger

class LDA_model:
    def __init__(self, topic_count: int):
        self.sentences = []
        self.sentences_pos = []
        self.bow_corpus = {}
        self.tf_idf_corpus = None
        self.dictionary = {}
        self.lda_model_tfidf = None
        self.document_topic = None
        self.num_topics = topic_count

    def load_sentences_from_api(self, clusters: dict, tagger: MorphoTagger) -> list:
        salient = []
        for cluster_num, cluster_d in clusters.items():
            for sentence_d in cluster_d['sentences']:
                self.sentences.append(sentence_d['sentence'])
                self.sentences_pos.append(sentence_d['sentence_pos'])
            try:
                self.create_lda_model()
                for idx, topic in self.lda_model_tfidf.show_topics(num_topics=-1, num_words=3, formatted=False):
                    topic_words = ' '.join([word for word, _ in topic])
                    cluster_d['topics'].append(topic_words)

                # most salient nouns per topic
                for idx, topic in self.lda_model_tfidf.show_topics(num_topics=-1, formatted=False):
                    for word, _ in topic:
                        try:
                            wp = tagger.pos_tagging(word, stem=False)[0][0]
                            if wp.tag[0] in 'N'  and word not in salient:
                                salient.append(word)
                        except Exception as e:
                            pass

                # cluster_d['ldavis'] = self.display()

                for sentence_d in cluster_d['sentences']:
                    v = self.dictionary.doc2bow(sentence_d['sentence_pos'])
                    d_sorted = sorted(self.lda_model_tfidf[v], key=lambda tup: -1 * tup[1])
                    index, score = d_sorted[0]
                    sentence_d['topic_number'] = index

            except ValueError as e:
                e_str = 'LDA_model-load_sentences_from_api-cluster-{}-{}: {}'.format(str(cluster_num),
                                                                                     len(cluster_d['sentences']),
                                                                                     str(e))
                print(e_str, file=sys.stderr)

        return salient

    def create_lda_model(self):
        self.bow_model()
        self.tf_idf()
        self.lda()

    def bow_model(self):
        self.dictionary = corpora.Dictionary(self.sentences_pos)
        self.dictionary.filter_extremes(no_below=10, no_above=0.6, keep_n=100000)
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
        self.lda_model_tfidf = models.LdaMulticore(self.tf_idf_corpus, num_topics=self.num_topics,
                                                   id2word=self.dictionary, passes=10, workers=4)
        if debug:
            for idx, topic in self.lda_model_tfidf.print_topics(-1):
                print('Topic: {} Word: {}'.format(idx, topic))
        # self.lda_model_tfidf.save("model.lda")

        topics = [self.lda_model_tfidf[self.tf_idf_corpus[i]] for i in range(len(self.sentences))]
        self.document_topic = pd.concat(
            [self.topics_document_to_dataframe(topics_document, num_topics=int(self.num_topics))
             for topics_document in topics]).reset_index(drop=True).fillna(0)

    def display(self):
        vis = pyLDAvis.gensim.prepare(topic_model=self.lda_model_tfidf, corpus=self.tf_idf_corpus,
                                      dictionary=self.dictionary)
        from IPython.core.display import HTML
        html: HTML = pyLDAvis.display(vis)
        return html.data
