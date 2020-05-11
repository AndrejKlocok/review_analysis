import pandas as pd
from sklearn.metrics import mean_squared_error
import random, sys
import argparse
sys.path.append('../')
from generate_dataset import statistics, Generator
from utils.elastic_connector import Connector


def load_datasets(dataset_path):
    sentences_cons = []
    sentences_pros = []

    with open(dataset_path + "negative.txt", encoding='utf-8') as file:
        sentences_cons = [(line[:-1], 1) for line in file]

    with open(dataset_path + "positive.txt", encoding='utf-8') as file:
        sentences_pros = [(line[:-1], 0) for line in file]

    return sentences_pros, sentences_cons


def see(data_path):
    # read the results data for the probabilities
    df_result = pd.read_csv(data_path + 'eval_text.tsv', sep='\t')

    wrong = []

    for index, row in df_result.iterrows():
        if row['label'] != row['prediction']:
            wrong.append([row['sentence'], row['label'], row['prediction']])

    df_results_wrong = pd.DataFrame(wrong, columns=['sentence', 'label', 'prediction'])

    df_results_wrong.to_csv(data_path + 'results_wrong.tsv', sep='\t', header=True, index=False)


def mse(data_path):
    df_result = pd.read_csv(data_path + 'eval_text.tsv', sep='\t')
    d = {}
    for index, row in df_result.iterrows():
        if row['label'] not in d:
            d[row['label']] = []
        if len(d[row['label']]) < 93:
            d[row['label']].append(row['prediction'])

    for key, value in d.items():
        key_l = [key]*len(value)
        mse = mean_squared_error(key_l, value)
        max_value = max(value)
        min_value = min(value)
        ret = {
            "cat": key,
            "mse": mse,
            "max": max_value,
            "min": min_value
        }
        print(ret)


def mall(path):
    def _getlines(file):
        return [ line for line in file]
    negative_sentences = []
    positive_sentences = []
    with open(path+'negative.txt') as file:
        negative_sentences = _getlines(file)
    with open(path+'positive.txt') as file:
        positive_sentences = _getlines(file)
    random.shuffle(negative_sentences)
    random.shuffle(positive_sentences)

    neg_l = len(negative_sentences)
    positive_sentences = positive_sentences[:neg_l]
    statistics([positive_sentences, negative_sentences])
    d = {
        'bert': True,
        'neuron': False,
        'sentences': 2,
        'equal_dataset': True,
        'num_category': 1,
        'category': '',
    }
    con = Connector()
    gen = Generator('mall', con, d)
    gen.bert([positive_sentences, negative_sentences])


def main():
    parser = argparse.ArgumentParser(
        description="Scrip generates desired dataset from utils db")

    parser.add_argument('-in', '--data_path_in', help='Dataset path', default='.')
    parser.add_argument('-s', '--see', help='See wrong results of classification', action='store_true')
    parser.add_argument('-mse', '--mse', help='Compute mse err of regression', action='store_true')
    parser.add_argument('-mall', '--mall', help='Path to mall directory')

    args = vars(parser.parse_args())

    if args['see']:
        see(args['data_path_in'])
    elif args['mse']:
        mse(args['data_path_in'])
    elif args['mall']:
        mall(args['mall'])


if __name__ == '__main__':
    main()
