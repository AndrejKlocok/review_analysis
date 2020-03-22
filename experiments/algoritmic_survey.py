import argparse

from sklearn.feature_extraction.text import TfidfVectorizer, TfidfTransformer, CountVectorizer
from sklearn.model_selection import GridSearchCV, train_test_split
from sklearn import metrics
from sklearn.pipeline import Pipeline

# Classifiers used in survey
from sklearn.svm import SVC, NuSVC
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.neural_network import MLPClassifier
from sklearn.naive_bayes import MultinomialNB
from nltk import MaxentClassifier

from stop_words import get_stop_words
import time, string
import regex as re
import nltk

import logging
import random


class Tokenizer(object):
    def __init__(self):
        nltk.download('punkt', quiet=True, raise_on_error=True)

    def __call__(self, line):
        tokens = nltk.word_tokenize(line)
        return list(tokens)


class Tester:
    def __init__(self, czech=None):
        if not czech:
            czech = nltk.word_tokenize(' '.join(get_stop_words('cz')))
        self._classifiers = [SVC(), NuSVC(), RandomForestClassifier(), LogisticRegression(),
                             # MLPClassifier(),
                             MultinomialNB(), ]  # MaxentClassifier()]
        hidden_layers = ((10, 1), (20, 1), (40, 1), (60, 1), (80, 1), (100, 1),
                         (10, 2), (20, 2), (40, 2), (60, 2), (80, 2), (100, 2),
                         (10, 3), (20, 3), (40, 3), (60, 3), (80, 3), (100, 3),
                         (10, 4), (20, 4), (40, 4), (60, 4), (80, 4), (100, 4))
        self.parameters = [
            # SVC
            {
                'vect__max_df': (0.5, 0.75, 1.0),
                'vect__ngram_range': ((1, 1), (1, 2), (1, 3)),
                'vect__norm': ('l1', 'l2', None),
                'vect__stop_words': (czech, None),
                'cls__C': (0.0001, 0.001, 0.01, 0.1, 1, 10, 100, 1000),
                'cls__gamma': (0.5, 0.1, 0.05, 0.01, 0.005, 0.001, 0.0005, 0.0001),
                'cls__kernel': ('linear', 'rbf', 'poly', 'sigmoid')
            },
            # NuSVC
            # {
            #    'vect__max_df': (0.5, 0.75, 1.0),
            #    'vect__ngram_range': ((1, 1), (1, 2), (1, 3)),
            #    'vect__norm': ('l1', 'l2', None),
            #    'vect__stop_words': (czech, None),
            #    'cls__nu': (0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65),
            #    'cls__kernel': ('linear', 'rbf', 'poly', 'sigmoid')
            # },
            # Random Forrest
            # {
            #    'vect__max_df': (0.5, 0.75, 1.0),
            #    'vect__ngram_range': ((1, 1), (1, 2), (1, 3)),
            #    'vect__norm': ('l1', 'l2', None),
            #    'vect__stop_words': (czech, None),
            #    'cls__max_depth': (None, 10, 20, 30, 40, 50, 60, 70, 80, 90),
            #    'cls__max_feat': (10, 20, 30, 40, 50, 'sqrt', None),
            # },
            # Logistic regression
            # {
            #    'vect__max_df': (0.5, 0.75, 1.0),
            #    'vect__ngram_range': ((1, 1), (1, 2), (1, 3)),
            #    'vect__norm': ('l1', 'l2', None),
            #    'vect__stop_words': (czech, None),
            #    'cls__C': (0.0001, 0.001, 0.01, 0.1, 1, 10, 100, 1000),
            #    'cls__class_weight': ('balanced', None),
            #    'cls__penalty': ('l1', 'l2')
            # },
            # Multi-layer Perceptron
            # {
            #    'vect__max_df': (0.5, 0.75, 1.0),
            #    'vect__ngram_range': ((1, 1), (1, 2), (1, 3)),
            #    'vect__norm': ('l1', 'l2', None),
            #    'vect__stop_words': (czech, None),
            #    'cls__alpha':   (0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5),
            #    'cls__hidden_layer_sizes': hidden_layers,
            #    'cls__activation': ('identity', 'logistic', 'tanh', 'relu'),
            #   'cls__solver': ('lbfgs', 'sgd', 'adam')
            # },
            # Naive Bayes
            # {
            #    'vect__max_df': (0.5, 0.75, 1.0),
            #    'vect__ngram_range': ((1, 1), (1, 2), (1, 3)),
            #    'vect__norm': ('l1', 'l2', None),
            #    'vect__stop_words': (czech, None),
            #    'cls__alpha': (0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5),
            #    'cls__fit_prior': (True, False)
            # },
            # Maximum Entropy
            # {
            #    'vect__max_df': (0.5, 0.75, 1.0),
            #    'vect__ngram_range': ((1, 1), (1, 2), (1, 3)),
            #    'vect__norm': ('l1', 'l2', None),
            #    'vect__stop_words': (czech, None),
            #    'cls__method': ('gis', 'iis', 'megam', 'tadm')
            # }

        ]
        self.pipeline_data = zip(self._classifiers, self.parameters)

    def get_best_params(self, data_x, data_y):
        from time import time
        for cls, parameters in self.pipeline_data:
            # #############################################################################
            # Define a pipeline combining a text feature extractor with a simple
            # classifier
            pipeline = Pipeline([
                ('vect', TfidfVectorizer(tokenizer=Tokenizer())),
                ('cls', cls),
            ])
            t0 = time()
            # classifier
            jobs = 4

            grid_search = GridSearchCV(pipeline, parameters, cv=3,
                                       n_jobs=jobs, verbose=1, pre_dispatch=2 * jobs)

            print("Performing grid search...")
            print("pipeline:", [name for name, _ in pipeline.steps])

            grid_search.fit(data_x, data_y)
            print("done in %0.3fs" % (time() - t0))

            print("Best score: %0.3f" % grid_search.best_score_)
            print("Best parameters set:")
            best_parameters = grid_search.best_estimator_.get_params()
            for param_name in sorted(parameters.keys()):
                print("\t%s: %r" % (param_name, best_parameters[param_name]))


class Classifier:
    def __init__(self, data_x, data_y):
        self.data_x = data_x
        self.data_y = data_y
        self.czech = nltk.word_tokenize(' '.join(get_stop_words('cz')))

    def cls(self, vectorizer, model):
        features = vectorizer.fit_transform(self.data_x)
        labels = self.data_y
        X_train, X_test, y_train, y_test = train_test_split(features, labels, test_size=0.20)
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        print(metrics.classification_report(y_test, y_pred, target_names=['Positive', 'Negative']))
        with open('stats.txt', 'w') as file:
            for index, s in enumerate(X_test):
                pred = str(y_pred[index])
                test = str(y_test[index])
                file.write(test + '\t' + pred + '\n')

    def cls_svm(self):
        vectorizer = TfidfVectorizer(max_df=0.5, ngram_range=(1, 2), norm='l2', stop_words=None, smooth_idf=False)
        model = SVC(C=100, gamma=0.01, kernel='rbf')
        self.cls(vectorizer, model)

    def cls_nusvm(self):
        vectorizer = TfidfVectorizer(max_df=0.5, ngram_range=(1, 2), norm='l2', stop_words=None, smooth_idf=False)
        model = NuSVC(nu=0.45, kernel='linear')
        self.cls(vectorizer, model)

    def cls_rf(self):
        vectorizer = TfidfVectorizer(max_df=0.5, ngram_range=(1, 1), norm=None, stop_words=self.czech, smooth_idf=False)
        model = RandomForestClassifier(max_depth=90, max_features='sqrt', n_estimators=100)
        self.cls(vectorizer, model)

    def cls_lr(self):
        vectorizer = TfidfVectorizer(max_df=0.5, ngram_range=(1, 2), norm='l2', stop_words=None, smooth_idf=True)
        model = LogisticRegression(C=10, class_weight=None, penalty='l2')
        self.cls(vectorizer, model)

    def cls_mlp(self):
        vectorizer = TfidfVectorizer(max_df=0.5, ngram_range=(1, 2), norm='l2', stop_words=None, smooth_idf=False)
        model = MLPClassifier(alpha=0.01, hidden_layer_sizes=(40, 2), activation='relu', solver='adam')
        self.cls(vectorizer, model)

    def cls_nb(self):
        vectorizer = TfidfVectorizer(max_df=0.5, ngram_range=(1, 2), norm='l1', stop_words=None, smooth_idf=True)
        model = MultinomialNB(alpha=0.05, fit_prior=False)
        self.cls(vectorizer, model)


def load_sentences(path, c):
    data = []
    with open(path, "r", encoding='utf-8') as file:
        for line in file:
            line = line[:-1]
            line = line.lower()
            line = re.sub(r'\d+', '', line)  # numbers
            line = re.sub(r'\p{P}+', '', line)  # punc
            data.append((line, c))
    return data


def main():
    parser = argparse.ArgumentParser(
        description="Scrip test dataset on classifiers like in https://arxiv.org/pdf/1901.02780.pdf")
    parser.add_argument('-bp', '--best_params', help='Dump best params for algorithms',
                        action='store_true')
    parser.add_argument('-svm', '--svm', help='C-Support Vector Classification.',
                        action='store_true')
    parser.add_argument('-nuSvm', '--nuSvm', help='Nu-Support Vector Classification.',
                        action='store_true')
    parser.add_argument('-rf', '--rf', help='A random forest classifier.',
                        action='store_true')
    parser.add_argument('-lr', '--lr', help='Logistic Regression (aka logit, MaxEnt) classifier.',
                        action='store_true')
    parser.add_argument('-mlp', '--mlp', help='Multi-layer Perceptron classifier.',
                        action='store_true')
    parser.add_argument('-nb', '--nb', help='Naive Bayes classifier.',
                        action='store_true')
    parser.add_argument('-me', '--me', help='Maximum Entropy classifier.',
                        action='store_true')
    parser.add_argument('-inP', '--inP', help='Dataset of positive sentences',
                        required=True)
    parser.add_argument('-inN', '--inN', help='Dataset of negative sentences',
                        required=True)

    args = vars(parser.parse_args())

    start = time.time()
    data_pos = load_sentences(args['inP'], 0)
    data_neg = load_sentences(args['inN'], 1)

    print('Positive: ' + str(len(data_pos)))
    print('Negative: ' + str(len(data_neg)))
    data = data_pos + data_neg

    random.shuffle(data)
    # Display progress logs on stdout
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s')

    data_x, data_y = zip(*data)
    classifier = Classifier(data_x, data_y)

    if args['best_params']:
        tester = Tester()
        tester.get_best_params(data_x, data_y)
    elif args['svm']:
        print('C-Support Vector Classification')
        classifier.cls_svm()
    elif args['nuSvm']:
        print('Nu-Support Vector Classification')
        classifier.cls_nusvm()
    elif args['rf']:
        print('A random forest classifier')
        classifier.cls_rf()
    elif args['lr']:
        print('Logistic Regression (aka logit, MaxEnt) classifier')
        classifier.cls_lr()
    elif args['mlp']:
        print('Multi-layer Perceptron classifier')
        classifier.cls_mlp()
    elif args['nb']:
        print('Naive Bayes classifier.')
        classifier.cls_nb()
    elif args['inN']:
        print('Maximum Entropy classifier.')
        raise NotImplemented('Maximum Entropy classifier is not implemented')
    print(time.time() - start)


if __name__ == '__main__':
    main()
