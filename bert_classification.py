import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import matthews_corrcoef, f1_score, confusion_matrix, mean_squared_error
from scipy.stats import pearsonr, spearmanr
import random
import argparse


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
        d[row['label']].append(row['prediction'])

    for key, value in d.items():
        key_l = [key]*len(value)
        mse = mean_squared_error(key_l, value)
        ret = {
            "cat": key,
            "mse": mse,
        }
        print(ret)




def main():
    parser = argparse.ArgumentParser(
        description="Scrip generates desired dataset from utils db")

    parser.add_argument('-in', '--data_path_in', help='Dataset path', default='.')
    parser.add_argument('-s', '--see', help='Dataset path', action='store_true')
    parser.add_argument('-mse', '--mse', help='Dataset path', action='store_true')

    args = vars(parser.parse_args())

    if args['see']:
        see(args['data_path_in'])
    elif args['mse']:
        mse(args['data_path_in'])


if __name__ == '__main__':
    main()
