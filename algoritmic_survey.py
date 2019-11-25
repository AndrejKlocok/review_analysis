from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import SGDClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.svm import SVC
from stop_words import get_stop_words

from pprint import pprint
from time import time
import logging
import random

def load_sentences(path, c):
    data = []
    with open(path, "r") as file:
        for line in file:
            line = line[:-1]
            data.append((line, c))
    return data


def main():

    data_pos = load_sentences("dataset_positive.txt", 0)
    data_neg = load_sentences("dataset_negative.txt", 1)
    data = data_pos[:500]+data_neg[:500]
    random.shuffle(data)
    # Display progress logs on stdout
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s')

    data_x, data_y = zip(*data)
    # #############################################################################
    # Define a pipeline combining a text feature extractor with a simple
    # classifier
    pipeline = Pipeline([
        ('vect', TfidfVectorizer()),
        #('tfidf', TfidfTransformer()),
        ('SVM', SVC()),
    ])

    # uncommenting more parameters will give better exploring power but will
    # increase processing time in a combinatorial way
    czech = get_stop_words('cz')
    parameters = {
        'vect__max_df': (0.5, 0.75, 1.0),
        'vect__ngram_range': ((1, 1), (1, 2), (1, 3)),
        'vect__norm': ('l1', 'l2', None),
        'vect__stop_words': (czech, None),
        'SVM__C': (0.0001, 0.001, 0.01, 0.1, 1, 10, 100, 1000),
        'SVM__gamma': (0.5, 0.1, 0.05, 0.01, 0.005, 0.001, 0.0005, 0.0001),
        'SVM__kernel': ('linear', 'rbf', 'poly', 'sigmoid')
    }
    # multiprocessing requires the fork to happen in a __main__ protected
    # block

    # find the best parameters for both the feature extraction and the
    # classifier
    grid_search = GridSearchCV(pipeline, parameters, cv=5,
                               n_jobs=-1, verbose=1)

    print("Performing grid search...")
    print("pipeline:", [name for name, _ in pipeline.steps])
    print("parameters:")
    pprint(parameters)
    t0 = time()
    grid_search.fit(data_x, data_y)
    print("done in %0.3fs" % (time() - t0))
    print()

    print("Best score: %0.3f" % grid_search.best_score_)
    print("Best parameters set:")
    best_parameters = grid_search.best_estimator_.get_params()
    for param_name in sorted(parameters.keys()):
        print("\t%s: %r" % (param_name, best_parameters[param_name]))


if __name__ == '__main__':
    main()