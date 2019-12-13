import pandas as pd
from sklearn.model_selection import train_test_split
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

    prediction = []
    wrong = []
    stat_good = 0
    stat_wrong = 0
    stat_count = 0

    for index, row in df_result.iterrows():
        if row['label'] != row['prediction']:
            wrong.append([row['sentence'], row['label'], row['prediction']])

    df_results_wrong = pd.DataFrame(wrong, columns=['sentence', 'label', 'prediction'])

    df_results_wrong.to_csv(data_path + 'results_wrong.tsv', sep='\t', header=True, index=False)



def main():
    parser = argparse.ArgumentParser(
        description="Scrip generates desired dataset from utils db")

    parser.add_argument('-in', '--data_path_in', help='Dataset path', default='.')

    args = vars(parser.parse_args())


    see(args['data_path_in'])


if __name__ == '__main__':
    main()
