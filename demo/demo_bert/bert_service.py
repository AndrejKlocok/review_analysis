import argparse
from bert_serving.client import BertClient
import numpy as np


def load_sentences(file):
    sentences = []
    with open(file, "r") as file:
        for line in file:
            sentences.append(line[:-1])
    return sentences


def main():
    parser = argparse.ArgumentParser(
        description="Script visualizes bert embedings with bert-as-service")
    requiredNamed = parser.add_argument_group('required named arguments')
    requiredNamed.add_argument('-input', help='File of sentences[str]', required=True)
    args = parser.parse_args()

    bc = BertClient()
    try:
        query = load_sentences(args.input)
        query_vec = bc.encode(query)

        score = np.sum(query_vec * doc_vecs, axis=1) / np.linalg.norm(doc_vecs, axis=1)
        topk_idx = np.argsort(score)[::-1][:topk]
        print('top %d questions similar to "%s"' % (topk, colored(query, 'green')))
        for idx in topk_idx:
            print('> %s\t%s' % (colored('%.1f' % score[idx], 'cyan'), colored(questions[idx], 'yellow')))

    except Exception as e:
        print(e)




if __name__ == '__main__':
    main()