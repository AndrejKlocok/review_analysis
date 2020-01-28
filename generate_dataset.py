import argparse
import time
import re
import random
import pandas as pd
import sys
from sklearn.model_selection import train_test_split

from utils.elastic_connector import Connector


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
        self.rating = False
        self.idx = 0
        self.category = args['category']
        self.len_min = args['min']
        self.len_max = args['max']

    def parse_reviews(self, cat_name, sentences):
        review_list = self.__con.get_reviews_from_subcategory(self.__category, cat_name)
        for review in review_list:
            review_sentences = []
            rating = round((int(review['rating'][:-1])) / 100, 3)

            # write data TODO refactor this code
            if self.is_pro:
                if self.is_just_sentence >= 2:
                    sen = []
                    [self.__get_sentence(pro, sen) for pro in review["pros"]]
                    if sen:
                        sen_txt = " ".join(s[:-1] for s in sen).strip()
                        review_sentences.append(sen_txt + "\n")
                else:
                    [self.__get_sentence(pro, review_sentences) for pro in review["pros"]]

            if self.is_con:
                if self.is_just_sentence >= 2:
                    sen = []
                    [self.__get_sentence(c, sen) for c in review["cons"]]
                    if sen:
                        sen_txt = " ".join(s[:-1] for s in sen).strip()
                        review_sentences.append(sen_txt + "\n")
                else:
                    [self.__get_sentence(c, review_sentences) for c in review["cons"]]

            if self.is_summary:
                if self.is_just_sentence >= 2:
                    sen = []
                    self.__get_sentence(review["summary"], sen)
                    if sen:
                        sen_txt = " ".join(s[:-1] for s in sen).strip()
                        review_sentences.append(sen_txt + "\n")
                else:
                    self.__get_sentence(review["summary"], review_sentences)

            if self.is_just_sentence == 3 and review_sentences:
                review_sentences = [(" ".join(s[:-1] for s in review_sentences).strip() + "\n")]

            if self.rating:
                review_sentences = [(s, rating) for s in review_sentences]

            sentences += review_sentences

    def get_sentences(self, shuffle=False):
        data = self.__con.get_subcategories_count(self.__category)
        sentences = []
        # desired subcategory is selected
        if self.category:
            for name, count in data:
                if name == self.category:
                    print("Dataset of " + name + " with " + str(count) + " reviews")
                    self.parse_reviews(name, sentences)
                    break
            if not sentences:
                raise Exception('Category: {} not found.'.format(self.category))
        # else according to top categories
        else:
            for name, count in data[:self.n_categories]:
                print("Dataset of " + name + " with " + str(count) + " reviews")
                self.parse_reviews(name, sentences)

        if shuffle:
            random.shuffle(sentences)

        return sentences

    def __get_sentence(self, s: str, sentences: list, regex=r'[,.]'):
        if self.is_just_sentence == 0:
            l = re.split(regex, s)
        else:
            l = [s]

        for sentence in l:
            if sentence and self.len_min <= len(sentence.split()) <= self.len_max and sentence.split()[0].isalpha():
                # file.write(sentence.strip().capitalize() + ".\n")
                sentence = sentence.strip().capitalize()
                sentence = re.sub(r'\.{2,}', "", sentence)
                sentence = re.sub(r'\t+', ' ', sentence)
                if sentence[-1] != '.':
                    sentence += '.'
                sentences.append(sentence + "\n")

    def __split_list(self, sentences, ratio):
        n = len(sentences)
        count = int(n * ratio)
        return sentences[:count], sentences[count:]

    def bert(self, s_list, regression=False):
        try:
            all_sentences = []
            dev_size = 0.2
            test_size = 0.2
            if not regression:
                # add label
                i = 0
                for s_sentences in s_list:
                    all_sentences.append([(s[:-1], i) for s in s_sentences])
                    i += 1
            else:
                for s_sentences in s_list:
                    all_sentences.append([(s[:-1], rating) for s, rating in s_sentences])

            s_sentences_train = []
            s_sentences_test = []
            for s_sentences in all_sentences:
                strain, stest = self.__split_list(s_sentences, test_size)
                s_sentences_train += strain
                s_sentences_test += stest

            dataset = []
            dataset_train = []

            if self.is_one_neuron:
                for s, label in s_sentences_test:
                    dataset.append([label, s])

                for s, label in s_sentences_train:
                    dataset_train.append([label, s])

                random.shuffle(dataset)
                random.shuffle(dataset_train)

                neuron_data_frame = pd.DataFrame(dataset, columns=['label', 'sentence'])
                df_neuron_train, df_neuron_dev = train_test_split(neuron_data_frame, test_size=dev_size)

                # Creating test dataframe
                df_neuron_test = pd.DataFrame(dataset_train, columns=['label', 'sentence'])
                # Saving dataframes to .csv format
                df_neuron_train.to_csv('train_binary_sent.csv', sep=',', index=False, header=True)
                df_neuron_dev.to_csv('dev_binary_sent.csv', sep=',', index=False, header=True)
                df_neuron_test.to_csv('test_binary_sent.csv', sep=',', index=False, header=True)

            elif self.is_bert:
                i = 0
                for s, label in s_sentences_test:
                    dataset.append([i, label, 'a', s])
                    i += 1

                for s, label in s_sentences_train:
                    dataset_train.append([i, label, 'a', s])
                    i += 1

                random.shuffle(dataset)
                random.shuffle(dataset_train)

                df_bert_train = pd.DataFrame(dataset, columns=['id', 'label', 'alpha', 'text'])

                # Creating test dataframe according to BERT
                df_bert_dev = pd.DataFrame(dataset_train, columns=['id', 'label', 'alpha', 'text'])
                # Saving dataframes to .tsv format as required by BERT
                df_bert_train.to_csv('train.tsv', sep='\t', index=False, header=False)
                df_bert_dev.to_csv('dev.tsv', sep='\t', index=False, header=False)
        except Exception as e:
            print("[bert] Exception: " + str(e), file=sys.stderr)


def task_emb(generator: Generator):
    try:
        with open("dataset_emb.txt", "w") as file:
            sentences = generator.get_sentences(generator.n_categories)
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
        print("[task_emb] Exception: " + str(e), file=sys.stderr)


def statistics(dataset):
    i = 0
    regex = r'[.]'
    for category_dataset in dataset:
        print("For category: " + str(i) + "there is: " + str(len(category_dataset)) + " lines")
        sentences_per_line = 0
        tokens_per_line = 0

        for sentence in category_dataset:
            tokens = 0
            sentence = sentence[:-1]

            for s in re.split(regex, sentence):
                if s:
                    tokens += len(s.split())
                    sentences_per_line += 1
            tokens_per_line += tokens

        print("\t\t sentences per line " + str((sentences_per_line / len(category_dataset))))
        print("\t\t tokens per line " + str((tokens_per_line / len(category_dataset))))
        i += 1


def task_cls(generator: Generator):
    try:
        generator.is_summary = False
        name = "dataset"

        generator.is_con = False
        sentences_pro = generator.get_sentences(True)
        generator.is_con = True
        generator.is_pro = False
        sentences_con = generator.get_sentences(True)

        if generator.is_equal:
            l_pro = len(sentences_pro)
            l_con = len(sentences_con)
            if l_pro > l_con:
                sentences_pro = sentences_pro[:l_con]
            elif l_pro < l_con:
                sentences_con = sentences_pro[:l_pro]

            assert (len(sentences_pro) == len(sentences_con)), "Same length"

        if generator.is_bert:
            generator.bert([sentences_pro, sentences_con])

        statistics([sentences_pro, sentences_con])

        with open(name + "_positive.txt", "w") as f_pros:
            [f_pros.write(s) for s in sentences_pro]
        with open(name + "_negative.txt", "w") as f_cons:
            [f_cons.write(s) for s in sentences_con]

    except Exception as e:
        print("[task_cls] Exception: " + str(e), file=sys.stderr)


def reviews_nratings(generator: Generator, n_cat):
    try:
        from time import sleep
        generator.is_summary = True
        generator.is_con = True
        generator.is_pro = True
        generator.rating = True
        generator.len_min = 1
        generator.len_max = 30
        name = "dataset"
        regression = False
        d = {}

        if n_cat > 1:
            # TODO handle this for boxing according to [(sentence, rating)]
            sentences = generator.get_sentences(True)
            delta = int(100 / n_cat)
            d = {}
            for x in range(0, n_cat):
                d[str(x)] = sentences
                sleep(1)
        else:
            # regression task, need to sort sentences according to rating from 0 to 100
            # sentences = sorted(sentences, key=lambda x:x[1], reverse=False)
            sentences = generator.get_sentences(shuffle=True)
            regression = True
            for s, rating in sentences:
                if rating not in d:
                    d[rating] = []
                d[rating].append((s, rating))

        if generator.is_equal:
            # TODO handle this for boxing according to [(sentence, rating)], not needed for regression
            d_eq = {}
            min = len(d['0'])
            for key, value in d.items():
                print(key + "has total of: " + str(len(value)))
                if min > len(value):
                    min = len(value)
            for key, value in d.items():
                d_eq[key] = d[key][:min]
            d = d_eq

        if generator.is_bert:
            l = [val for _, val in d.items()]
            generator.bert(l, regression)

        if regression:
            l = []
            l_tmp = sorted(d)
            for val in l_tmp:
                l.append([s for s, rating in d[val]])
            statistics(l)

        else:
            statistics([val for _, val in d.items()])
            for key, value in d.items():
                with open(name + '_' + key, 'w') as file:
                    for s in value:
                        file.write(s)

    except Exception as e:
        print("[reviews_nratings] Exception: " + str(e), file=sys.stderr)


def main():
    domain = {
        'el': 'Elektronika',
        'bz': 'Bile zbozi',
        'daz': 'Dum a zahrada',
        'chv': 'Chovatelstvi',
        'am': 'Auto-moto',
        'dz': 'Detske zbozi',
        'om': 'Obleceni a moda',
        'fkh': 'Filmy, knihy, hry',
        'kaz': 'Kosmetika a zdravi',
        'sp': 'Sport',
        'hob': 'Hobby',
        'jan': 'Jidlo a napoje',
        'svb': 'Stavebniny',
        'sex': 'Sexualni a eroticke pomucky',
    }

    parser = argparse.ArgumentParser(
        description="Scrip generates desired dataset from utils db")
    parser.add_argument('-d', '--domain', help='Generate dataset from domain in.\n'
                                               + str(domain),
                        required=True)
    parser.add_argument('-emb', '--embeddings', help='Generate dataset for embeddings with all sentences, 80-20',
                        action='store_true')
    parser.add_argument('-bi', '--bipolar', help='Generate dataset for sentiment classification +-',
                        action='store_true', default=False)
    parser.add_argument('-rat', '--rating', help='Generate dataset for RATING classes according to review',
                        default=-1, type=int)

    parser.add_argument('-b', '--bert', help='Generate also data for Bert model (train.tsv, test.tsv, dev.tsv)',
                        action='store_true')
    parser.add_argument('-neur', '--neuron',
                        help='Generate dataset for one-neuron model (train.csv, test.csv, dev.csv)',
                        action='store_true')
    parser.add_argument('-s', '--sentences', help="Sentences in dataset \n"
                                                  + "0 -> one sentence one row\n"
                                                  + "1 -> one \"section\" of pro/con review one row\n"
                                                  + "2 -> whole section pro/con as one row\n"
                                                  + "3 -> whole review one row", type=int,
                        default=0)
    parser.add_argument('-n', '--num_category', help='Number of categories from bert', type=int, default=-1)
    parser.add_argument('-e', '--equal_dataset', help='Generate pos/neg equal size', action='store_true')
    parser.add_argument('-c', '--category', help='Concrete category', type=str, default='')
    parser.add_argument('-max', '--max', help='Maximum length of words', type=int, default=20)
    parser.add_argument('-min', '--min', help='Minimum length of words', type=int, default=3)

    args = vars(parser.parse_args())
    if args['domain'] not in domain:
        print(args['domain'] + ' is not a valid option, see -h', file=sys.stderr)
        sys.exit(1)

    category = domain[args['domain']]

    # Elastic
    con = Connector()

    if args['sentences'] > 3:
        print('--sentences out of scope, see --help', file=sys.stderr)
        sys.exit(1)

    start = time.time()
    if args['embeddings']:
        gen = Generator(category, con, args)
        task_emb(gen)
    elif args['bipolar']:
        gen = Generator(category, con, args)
        task_cls(gen)
    elif args['rating']:
        gen = Generator(category, con, args)
        reviews_nratings(gen, args['rating'])

    print(time.time() - start)

    pass


if __name__ == '__main__':
    main()
