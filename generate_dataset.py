import argparse
import time
import re
import random
import pandas as pd
import sys
from sklearn.model_selection import train_test_split

from elastic_connector import Connector


class Generator:
    def __init__(self, category: str, connector: Connector, args: argparse, is_pro: bool = True,
                 is_con: bool = True, is_summary: bool = True):
        self.__category = category
        self.__con = connector
        self.is_pro = is_pro
        self.is_con = is_con
        self.is_summary = is_summary
        self.is_bert = args['bert']
        self.is_one_neuron = args['neuron']
        # TODO enum not int
        self.is_just_sentence = args['sentences']
        self.is_equal = args['equal_dataset']
        self.n_categories = args['num_category']

    def get_sentences(self, top_categories, shuffle=False, len_min=3, len_max=32):
        data = self.__con.get_subcategories_count(self.__category)
        sentences = []
        for name, count in data[:top_categories]:
            print("Dataset of " + name + " with " + str(count) + " reviews")
            review_list = self.__con.get_reviews_from_subcategory(self.__category, name)
            for review in review_list:
                # write data TODO refactor this code
                if self.is_pro:
                    if self.is_just_sentence == 2:
                        sen = []
                        [self.__get_sentence(pro, sen, len_min, len_max) for pro in review["pros"]]
                        if sen:
                            sentences.append(" ".join(s[:-1]for s in sen).strip())
                    else:
                        [self.__get_sentence(pro, sentences, len_min, len_max) for pro in review["pros"]]

                if self.is_con:
                    if self.is_just_sentence == 2:
                        sen = []
                        [self.__get_sentence(c, sen, len_min, len_max) for c in review["cons"]]
                        if sen:
                            sentences.append(" ".join(s[:-1]for s in sen).strip())
                    else:
                        [self.__get_sentence(c, sentences, len_min, len_max) for c in review["cons"]]

                if self.is_summary:
                    if self.is_just_sentence == 2:
                        sen = []
                        self.__get_sentence(review["summary"], sen, len_min, len_max)
                        if sen:
                            sentences.append(" ".join(s[:-1]for s in sen).strip())
                    else:
                        self.__get_sentence(review["summary"], sentences, len_min, len_max)

        if shuffle:
            random.shuffle(sentences)

        return sentences

    def __get_sentence(self, s: str, sentences: list, len_min, len_max, regex=r'[,.]'):
        if self.is_just_sentence == 0:
            l = re.split(regex, s)
        else:
            # add Case
            # tmp = []
            # for sen in s.split('.'):
            #    tmp.append(sen.capitalize()+'.')
            # s = " ".join(tmp)
            l = [s]

        for sentence in l:
            if sentence and len_min < len(sentence.split()) < len_max and sentence.split()[0].isalpha():
                # file.write(sentence.strip().capitalize() + ".\n")
                sentence = sentence.strip().capitalize()
                sentence = re.sub(r'\.{2,}', "", sentence)
                if sentence[-1] != '.':
                    sentence += '.'
                sentences.append(sentence + "\n")

    def __split_list(self, sentences, ratio):
        n = len(sentences)
        count = int(n * ratio)
        return sentences[:count], sentences[count:]

    def bert(self, s_pros, s_cons):
        try:
            # add label
            sentences_pros = [(s[:-1], 0) for s in s_pros]
            sentences_cons = [(s[:-1], 1) for s in s_cons]

            sentences_pros_train, sentences_pros = self.__split_list(sentences_pros, 0.2)
            sentences_cons_train, sentences_cons = self.__split_list(sentences_cons, 0.2)

            dataset = []
            dataset_train = []

            if self.is_one_neuron:
                for s, label in sentences_pros + sentences_cons:
                    dataset.append([label, s])

                for s, label in sentences_pros_train + sentences_cons_train:
                    dataset_train.append([label, s])

                random.shuffle(dataset)
                random.shuffle(dataset_train)

                neuron_data_frame = pd.DataFrame(dataset, columns=['label', 'sentence'])
                df_neuron_train, df_neuron_dev = train_test_split(neuron_data_frame, test_size=0.040)

                # Creating test dataframe
                df_neuron_test = pd.DataFrame(dataset_train, columns=['label', 'sentence'])
                # Saving dataframes to .csv format
                df_neuron_train.to_csv('train_binary.csv', sep=',', index=False, header=True)
                df_neuron_dev.to_csv('dev_binary.csv', sep=',', index=False, header=True)
                df_neuron_test.to_csv('test_binary.csv', sep=',', index=False, header=True)

            elif self.is_bert:
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
                df_bert_train.to_csv('train.tsv', sep='\t', index=False, header=False)
                df_bert_dev.to_csv('dev.tsv', sep='\t', index=False, header=False)
                df_bert_test.to_csv('test.tsv', sep='\t', index=False, header=True)
        except Exception as e:
            print("[bert] Exception: " + str(e))


def task_emb(generator: Generator):
    try:
        with open("dataset_emb.txt", "w") as file:
            sentences = generator.get_sentences(1)
            c = int(len(sentences) * 0.2)
            i = 0
            f_train = open("dataset_emb_test.txt", "w")
            for s in sentences:
                if i < c:
                    f_train.write(s)
                else:
                    file.write(s)
                i += 1
            f_train.close()

    except Exception as e:
        print("[task_emb] Exception: " + str(e))


def task_cls(generator: Generator):
    try:
        generator.is_summary = False
        name = "dataset"

        generator.is_con = False
        sentences_pro = generator.get_sentences(generator.n_categories, True)
        generator.is_con = True
        generator.is_pro = False
        sentences_con = generator.get_sentences(generator.n_categories, True)

        if generator.is_equal:
            l_pro = len(sentences_pro)
            l_con = len(sentences_con)
            if l_pro > l_con:
                sentences_pro = sentences_pro[:l_con]
            elif l_pro < l_con:
                sentences_con = sentences_pro[:l_pro]

            assert (len(sentences_pro) == len(sentences_con)), "Same length"

        if generator.is_bert:
            generator.bert(sentences_pro, sentences_con)

        with open(name + "_positive.txt", "w") as f_pros:
            [f_pros.write(s) for s in sentences_pro]
        with open(name + "_negative.txt", "w") as f_cons:
            [f_cons.write(s) for s in sentences_con]

    except Exception as e:
        print("[task_cls] Exception: " + str(e))


def main():
    parser = argparse.ArgumentParser(
        description="Scrip generates desired dataset from elastic db")
    parser.add_argument('-emb', '--embeddings', help='Generate dataset for embeddings with all sentences, 80-20',
                        action='store_true')
    parser.add_argument('-cls', '--classification', help='Generate dataset for sentiment classification +-',
                        action='store_true')
    parser.add_argument('-b', '--bert', help='Generate also data for Bert model (train.tsv, test.tsv, dev.tsv)',
                        action='store_true')
    parser.add_argument('-neur', '--neuron',
                        help='Generate dataset for one-neuron model (train.csv, test.csv, dev.csv)',
                        action='store_true')
    parser.add_argument('-s', '--sentences', help="Sentences in dataset 0 -> one sentence one row\n"
                                                  + "1 -> one \"section\" of pro/con review one row\n"
                                                  + "2 -> whole section pro/con as one row\n", type=int,
                        default=0)
    parser.add_argument('-n', '--num_category', help='Number of categories from bert', type=int)
    parser.add_argument('-e', '--equal_dataset', help='Generate pos/neg equal size', action='store_true')

    args = vars(parser.parse_args())

    # Elastic
    con = Connector()

    # categories
    categories = [
        # 'Elektronika',
        'Bile zbozi',
        # 'Dum a zahrada',
        # 'Chovatelstvi',
        # 'Auto-moto',
        # 'Detske zbozi',
        # 'Obleceni a moda',
        # 'Filmy, knihy, hry',
        # 'Kosmetika a zdravi',
        # 'Sport',
        # 'Hobby',
        # 'Jidlo a napoje',
        # 'Stavebniny',
        # 'Sexualni a eroticke pomucky'
    ]
    if args['sentences'] >2:
        print('--sentences out of scope, see --help', sys.stderr)

    start = time.time()
    if args['embeddings']:
        for category in categories:
            gen = Generator(category, con)
            task_emb(gen)
    elif args['classification']:
        for category in categories:
            gen = Generator(category, con, args)
            task_cls(gen)

    print(time.time() - start)

    pass


if __name__ == '__main__':
    main()
