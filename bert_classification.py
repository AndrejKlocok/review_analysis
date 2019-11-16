import pandas as pd
from sklearn.model_selection import train_test_split
import random
import argparse


def split_list(sentences, ratio):
    n = len(sentences)
    count = int(n * ratio)
    return sentences[:count], sentences[count:]


def bert():
    sentences_cons = []
    sentences_pros = []
    with open("dataset_cons.txt", encoding='utf-8') as file:
        sentences_cons = [(line[:-1], 1) for line in file]

    with open("dataset_pros.txt", encoding='utf-8') as file:
        sentences_pros = [(line[:-1], 0) for line in file]

    sentences_pros_train, sentences_pros = split_list(sentences_pros, 0.2)
    sentences_cons_train, sentences_cons = split_list(sentences_cons, 0.2)

    dataset = []
    dataset_train = []
    i = 0
    for s, label in sentences_pros + sentences_cons:
        dataset.append([i, label, 'a', s])
        i += 1

    for s, _ in sentences_pros_train + sentences_cons_train:
        dataset_train.append([i, s])
        i += 1

    random.shuffle(dataset)
    random.shuffle(dataset_train)

    bert_data_frame = pd.DataFrame(dataset, columns=['id', 'label', 'alpha', 'text'])
    df_bert_train, df_bert_dev = train_test_split(bert_data_frame, test_size=0.040)

    # Creating test dataframe according to BERT
    df_bert_test = pd.DataFrame(dataset_train, columns=['id', 'text'])

    # Saving dataframes to .tsv format as required by BERT
    df_bert_train.to_csv('../data/train.tsv', sep='\t', index=False, header=False)
    df_bert_dev.to_csv('../data/dev.tsv', sep='\t', index=False, header=False)
    df_bert_test.to_csv('../data/test.tsv', sep='\t', index=False, header=True)


def see():
    # read the original test data for the text and id
    df_test = pd.read_csv('../data/test.tsv', sep='\t')

    # read the results data for the probabilities
    df_result = pd.read_csv('../data_out/test_results.tsv', sep='\t', header=None)
    # create a new dataframe
    df_map_result = pd.DataFrame({'id': df_test['id'],
                                  'text': df_test['text'],
                                  'label': df_result.idxmax(axis=1)})
    # view sample rows of the newly created dataframe
    df_map_result.sample(10)

    df_map_result.to_csv('results.csv', sep='\t', header=True)


def main():
    parser = argparse.ArgumentParser(
        description="Scrip generates desired dataset from elastic db")
    parser.add_argument('-see', '--see', help='See', action='store_true')
    parser.add_argument('-bert', '--bert', help='Generate bert files', action='store_true')

    args = vars(parser.parse_args())

    if args['bert']:
        bert()
    elif args['see']:
        see()


if __name__ == '__main__':
    main()
