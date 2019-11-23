import pandas as pd
from sklearn.model_selection import train_test_split
import random
import argparse

def load_datasets(dataset_path):
    sentences_cons = []
    sentences_pros = []

    with open(dataset_path+"negative.txt", encoding='utf-8') as file:
        sentences_cons = [(line[:-1], 1) for line in file]

    with open(dataset_path+"positive.txt", encoding='utf-8') as file:
        sentences_pros = [(line[:-1], 0) for line in file]

    return sentences_pros, sentences_cons


def see(data_path, out_path):
    # read the original test data for the text and id
    df_test = pd.read_csv(data_path+'test.tsv', sep='\t')

    # read the results data for the probabilities
    df_result = pd.read_csv(out_path+'test_results.tsv', sep='\t', header=None)

    sentences_pro, sentences_con = load_datasets(data_path)
    prediction = []

    df_map_result = pd.DataFrame({'id': df_test['id'],
                                  'text': df_test['text'],
                                  'label': df_result.idxmax(axis=1),
                                  'confidence_pro': df_result.iloc[:,0],
                                  'confidence_con': df_result.iloc[:,1]})
    wrong = []
    stat_good = 0
    stat_wrong = 0
    stat_count = 0

    for index, row in df_map_result.iterrows():
        label = -1
        if (row['text'], 0) in sentences_pro:
            label = 0
        elif (row['text'], 1) in sentences_con:
            label = 1
        else:
            raise Exception(row['text'] + ' not found in dataset')

        confidence = row['confidence_pro'] if row['label'] == 0 else row['confidence_con']

        if label != row['label']:
            wrong.append([row['text'], row['label'], confidence, label])
            stat_wrong += 1
        else:
            stat_good += 1

        stat_count += 1

        prediction.append(label)

    df_map_result['actual'] = prediction
    df_results_wrong = pd.DataFrame(wrong, columns=['text', 'predicted_label', 'confidence', 'actual_label'])

    df_map_result.to_csv(out_path+'results.tsv', sep='\t', header=True, float_format='%.3f')
    df_results_wrong.to_csv(out_path+'results_wrong.tsv', sep='\t', header=True, float_format='%.3f')
    s = "{:1.4f}".format(stat_good/stat_count)
    print("Statistics for n: "+str(stat_count) +" prediction: "+ s)


def main():
    parser = argparse.ArgumentParser(
        description="Scrip generates desired dataset from utils db")
    parser.add_argument('-see', '--see', help='See', action='store_true')
    parser.add_argument('-bert', '--bert', help='Generate bert files', action='store_true')
    parser.add_argument('-comp', '--comp', help='Compare results  files', action='store_true')
    parser.add_argument('-in', '--data_path_in', help='Dataset path', default='.')
    parser.add_argument('-out', '--data_path_out', help='Path to folder with results', default='.')

    args = vars(parser.parse_args())

    #if args['bert']:
        #bert(args['data_path_in'], args['data_path_out'])
    if args['see']:
        see(args['data_path_in'], args['data_path_out'])


if __name__ == '__main__':
    main()
