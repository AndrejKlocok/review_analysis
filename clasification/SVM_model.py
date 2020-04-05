from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn import metrics
from sklearn.svm import SVC
from stop_words import get_stop_words
import regex as re
import random, pickle, argparse

from gensim.models.fasttext import load_facebook_model
from fse.models import uSIF


class SVM_Classifier:
    def __init__(self):
        self.model = None
        self.usif_model = None
        path = '/tmp/xkloco00/athena18/model/'
        #path = '/mnt/data/xkloco00_a18/model/'
        self.irrelevant_path = path + 'irrelevant.tsv'
        self.svm_path = path + 'SVM/irrelevant_SVM.pkl'
        self.sent2vec_path = path + 'sent2vec/sent2vec.model'
        self.fasttext_path = path + 'fasttext/cc.cs.300.bin'
        self.embedding_path = path + 'embeddings.txt'

    def load_sentences(self, path):
        data = []
        with open(path, "r", encoding='utf-8') as file:
            for line in file:
                line = line[:-1]
                row = line.split('\t')
                text = row[3].lower()
                text = re.sub(r'\d+', '', text)  # numbers
                text = re.sub(r'\p{P}+', '', text)  # punc
                c = row[1]
                data.append((text, c))

        print('Sentences count: {} '.format(len(data)))
        random.shuffle(data)
        return data

    def load_models(self):
        with open(self.svm_path, 'rb') as f:
            self.model = pickle.load(f)

        self.usif_model = uSIF.load(self.sent2vec_path)

    def init__usif(self):
        embeddings = []
        i = 0
        with open(self.embedding_path, 'r') as f:
            for line in f:
                line = line[:-1]
                line = line.lower()
                line = re.sub(r'\d+', '', line)  # numbers
                line = re.sub(r'\p{P}+', '', line)  # punc
                embeddings.append( (line.split(), i) )
                i+=1
        print('Total lines {}'.format(str(i)))
        fasttext = load_facebook_model(self.fasttext_path)
        self.usif_model = uSIF(fasttext, workers=16)
        self.usif_model.train(embeddings)
        self.usif_model.save(self.sent2vec_path)

    def create_model(self):
        data = self.load_sentences(self.irrelevant_path)
        model = SVC(C=100, gamma=0.01, kernel='rbf')
        data_x, data_y = zip(*data)

        data_embed = []
        i = 0
        for sentence, index in data:
            data_embed.append((sentence.split(), i))
            i += 1

        self.usif_model = uSIF.load(self.sent2vec_path)
        features = self.usif_model.infer(data_embed)

        labels = data_y
        X_train, X_test, y_train, y_test = train_test_split(features, labels, test_size=0.20)
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        print(metrics.classification_report(y_test, y_pred, target_names=['Irrelevant', 'Normal']))
        with open(self.svm_path, 'wb') as f:
            pickle.dump(model, f)

    def eval_example(self, sentence):
        sentence = sentence.lower()
        sentence = re.sub(r'\d+', '', sentence)  # numbers
        sentence = re.sub(r'\p{P}+', '', sentence)  # punc
        features = self.usif_model.infer([(sentence.split(), 0)])
        y_pred = self.model.predict(features)
        if y_pred[0] == '0':
            return 'irrelevant'
        else:
            return 'normal'


def main():
    parser = argparse.ArgumentParser(
        description="SVM model")
    parser.add_argument('-usif', '--usif', help='Init usif sent2vec model', action='store_true')
    parser.add_argument('-cls', '--cls', help='Init SVM classifier', action='store_true')

    args = vars(parser.parse_args())
    cls = SVM_Classifier()

    if args['usif']:
        cls.init__usif()
    elif args['cls']:
        cls.create_model()
    else:
        cls.load_models()
        data = cls.load_sentences('../../irrelevant.tsv')
        f = open('test.txt', 'w')
        for sentence, label in data:
            pred = cls.eval_example(sentence)
            if pred != label:
                f.write(sentence + '\t' + label + '\t' + pred + '\n')


if __name__ == '__main__':
    main()
