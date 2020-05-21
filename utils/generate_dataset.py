"""
This file contains implementation of Generator class, which contains core implementation of extracting text from elastic
search indexes and GeneratorController, which is used by back-end server.

Author: xkloco00@stud.fit.vutbr.cz
"""
import argparse
import time
import re
import random
import pandas as pd
import sys

from review_analysis.utils.elastic_connector import Connector

sentence_type_mapper = {
    'sentence = row': 0,
    'section of +/- = row': 1,
    'whole section of +/- = row': 2,
    'whole review = row': 3
}


class Generator:
    """
    Class holds all necessary information for data exporting with given arguments.
    """

    def __init__(self, domain: str, connector: Connector, args, is_pro: bool = True,
                 is_con: bool = True, is_summary: bool = True):
        """
        Constructor initalize generation process by storing passed arguments.
        :param domain:
        :param connector:
        :param args:
        :param is_pro:
        :param is_con:
        :param is_summary:
        """
        self.__domain = domain
        self.__con = connector
        self.is_pro = is_pro
        self.is_con = is_con
        self.is_summary = is_summary
        self.is_bert = args['bert']

        self.is_just_sentence = args['sentence_type']
        self.is_equal = args['equal']
        self.n_categories = args['num_category'] if 'num_category' in args else -1
        self.rating = False
        self.idx = 0
        self.category = args['category'] if 'category' in args else None
        self.categories = args['categories']
        self.len_min = args['sentence_min_len']
        self.len_max = args['sentence_max_len']

    def parse_reviews(self, cat_name: str, sentences: list):
        """
         Append parsed reviews from category defined by cat_name to sentences list.
        :param cat_name:
        :param sentences:
        :return:
        """
        # get review list either way from shop domain or product domain
        if cat_name == 'shop':
            review_list, _ = self.__con.get_shop_reviews()
        else:
            review_list, _ = self.__con.get_reviews_from_category(cat_name)

        # loop over reviews
        for review in review_list:
            try:
                review_sentences = []
                rating = round((int(review['rating'][:-1])) / 100, 3)

                # for each section of reviews as pro/con/summary export sentences with given sentence type
                # define by sentence_type_mapper
                if self.is_pro:
                    # pro section
                    if self.is_just_sentence >= 2:
                        # whole section of pro as one entry
                        sen = []
                        [self.__get_sentence(pro, sen) for pro in review["pros"]]
                        if sen:
                            sen_txt = " ".join(s[:-1] for s in sen).strip()
                            review_sentences.append(sen_txt + "\n")
                    else:
                        # append each pro section individually
                        [self.__get_sentence(pro, review_sentences) for pro in review["pros"]]

                if self.is_con:
                    # con section
                    if self.is_just_sentence >= 2:
                        # whole section of con as one entry
                        sen = []
                        [self.__get_sentence(c, sen) for c in review["cons"]]
                        if sen:
                            sen_txt = " ".join(s[:-1] for s in sen).strip()
                            review_sentences.append(sen_txt + "\n")
                    else:
                        [self.__get_sentence(c, review_sentences) for c in review["cons"]]

                if self.is_summary:
                    # summary section
                    if self.is_just_sentence >= 2:
                        # whole section of summary as one entry
                        sen = []
                        self.__get_sentence(review["summary"], sen)
                        if sen:
                            sen_txt = " ".join(s[:-1] for s in sen).strip()
                            review_sentences.append(sen_txt + "\n")
                    else:
                        # append summary section by parts ( sentences)
                        self.__get_sentence(review["summary"], review_sentences)

                # if the whole review as option concatenate all
                if self.is_just_sentence == 3 and review_sentences:
                    review_sentences = [(" ".join(s[:-1] for s in review_sentences).strip() + "\n")]
                # rating
                if self.rating:
                    review_sentences = [(s, rating) for s in review_sentences]

                sentences += review_sentences

            except Exception as e:
                pass

    def get_sentences(self, shuffle=False):
        """
        Get sentences from subcategories by n top categories, whole domain or selected category
        :param shuffle: do shuffle output
        :return:
        """
        # get list of pairs [(category, count)]
        data = self.__con.get_subcategories_count(self.__domain)
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

    def get_data_api_call(self, shuffle=False):
        """
        Handle from back-end server, generate dataset with given category list.
        :param shuffle:
        :return:
        """
        sentences = []
        for category in self.categories:
            sentences_cat = []
            self.parse_reviews(category, sentences_cat)
            sentences += sentences_cat

        if shuffle:
            random.shuffle(sentences)

        return sentences

    def __get_sentence(self, s: str, sentences: list, regex=r'[,.]'):
        """
        Export sentence to sentences list with given rules.
        :param s: sentence
        :param sentences: list of sentences
        :param regex:
        :return:
        """
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
        """
        Split list of sentences by given ration
        :param sentences:
        :param ratio:
        :return:
        """
        n = len(sentences)
        count = int(n * ratio)
        return sentences[:count], sentences[count:]

    def bert(self, s_list, regression=False, csv=True):
        """
        Export dataset to train/test with option of bert-style format
        :param s_list:
        :param regression:
        :param csv:
        :return:
        """
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

            dataset_train = []
            dataset_dev = []

            if self.is_bert:
                i = 0
                for s, label in s_sentences_test:
                    dataset_train.append([str(i), str(label), 'a', s])
                    i += 1

                for s, label in s_sentences_train:
                    dataset_dev.append([str(i), str(label), 'a', s])
                    i += 1

                random.shuffle(dataset_train)
                random.shuffle(dataset_dev)
                if csv:
                    df_bert_train = pd.DataFrame(dataset_train, columns=['id', 'label', 'alpha', 'text'])
                    # Creating test dataframe according to BERT
                    df_bert_dev = pd.DataFrame(dataset_dev, columns=['id', 'label', 'alpha', 'text'])
                    # Saving dataframes to .tsv format as required by BERT
                    df_bert_train.to_csv('train.tsv', sep='\t', index=False, header=False)
                    df_bert_dev.to_csv('dev.tsv', sep='\t', index=False, header=False)
                else:
                    train = ['\t'.join(row) + '\n' for row in dataset_train]
                    dev = ['\t'.join(row) + '\n' for row in dataset_dev]
                    return train, dev

        except Exception as e:
            print("[bert] Exception: " + str(e), file=sys.stderr)


class GeneratorController:
    """
    Controller for dataset generation from back-end api.
    """

    def __init__(self, connector: Connector):
        # Elastic
        self.con = connector

    def generate(self, args):
        """
        Export dataset with given arguments
        :param args:
        :return: data dictionary
        """
        data = {
            'error': None
        }
        args['bert'] = False
        if args['model_type'] == 'bert':
            args['bert'] = True

        try:
            t = sentence_type_mapper[args['sentence_type']]
            args['sentence_type'] = t
        except KeyError as e:
            data['error'] = str(e)
            return data

        # at least one category
        if len(args['categories']) == 0:
            data['error'] = 'Empty categories field'
            return data

        # create Generator instance
        generator = Generator(domain='', connector=self.con, args=args)

        if args['task_type'] == 'embeddings':
            data = self.__embeddings_task(generator)

        elif args['task_type'] == 'bipolar classification':
            data = self.__cls_task(generator)

        elif args['task_type'] == 'regression on rating':
            data = self.__regression_task(generator)

        else:
            data['error'] = 'KeyError: Unknown task type'

        return data

    def __embeddings_task(self, generator: Generator):
        """
        Export data in sentences to embeddings.txt
        :param generator:
        :return:
        """
        d = {}

        try:
            sentences = generator.get_data_api_call(True)
            d['embbedings.txt'] = sentences
        except Exception as e:
            print("[__embeddings_task] Exception: " + str(e), file=sys.stderr)
            d['error'] = str(e)
        finally:
            return d

    def __cls_task(self, generator: Generator):
        """
        Export data for classification tasks
        :param generator:
        :return:
        """
        d = {}
        try:
            # setup generator for extracting positive and then negative sentences
            generator.is_summary = False
            generator.is_con = False
            sentences_pro = generator.get_data_api_call(True)
            generator.is_con = True
            generator.is_pro = False
            sentences_con = generator.get_data_api_call(True)
            # equal flag
            if generator.is_equal:
                l_pro = len(sentences_pro)
                l_con = len(sentences_con)
                if l_pro > l_con:
                    sentences_pro = sentences_pro[:l_con]
                elif l_pro < l_con:
                    sentences_con = sentences_pro[:l_pro]

            # bert option
            if generator.is_bert:
                train, dev = generator.bert([sentences_pro, sentences_con], csv=False)
                d['train.tsv'] = train
                d['dev.tsv'] = dev
            else:
                d['dataset_positive.txt'] = sentences_pro
                d['dataset_negative.txt'] = sentences_con

        except Exception as e:
            print("[__cls_task] Exception: " + str(e), file=sys.stderr)
            d['error'] = str(e)
        finally:
            return d

    def __regression_task(self, generator: Generator):
        """
        Generate data as regression task, used in rating model.
        :param generator:
        :return:
        """
        data = {}
        try:
            d = {}
            generator.rating = True
            # regression task, need to sort sentences according to rating from 0 to 100
            # sentences = sorted(sentences, key=lambda x:x[1], reverse=False)
            sentences = generator.get_data_api_call(True)
            regression = True
            for s, rating in sentences:
                if rating not in d:
                    d[rating] = []

                d[rating].append((s, rating))
            # error when reviews has 0% rating on heureka
            d.pop(0.0, None)
            # equal flag, all categories must have the same ammount of sentences
            if generator.is_equal:
                d_eq = {}
                min = len(d[1.0])
                for key, value in d.items():
                    if min > len(value):
                        min = len(value)
                for key, value in d.items():
                    d_eq[key] = d[key][:min]
                d = d_eq
            # export data in bert style
            if generator.is_bert:
                l = [val for _, val in d.items()]
                train, dev = generator.bert(l, regression, csv=False)
                data['train.tsv'] = train
                data['dev.tsv'] = dev
            else:
                for key, value in d.items():
                    name = 'dataset' + '_' + str(key) + '.txt'
                    data[name] = [val for val, _ in value]

            # regression dataset statistics
            l = []
            l_tmp = sorted(d)
            for val in l_tmp:
                l.append([s for s, rating in d[val]])
            data['statistics.txt'] = statistics(l)

        except Exception as e:
            print("[__regression_task] Exception: " + str(e), file=sys.stderr)
            data['error'] = str(e)
        finally:
            return data


def task_emb(generator: Generator):
    """
    Export data to dataset_emb.txt file in working directory.
    :param generator:
    :return:
    """
    try:
        with open("dataset_emb.txt", "w") as file:
            sentences = generator.get_sentences(True)
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
    """
    Compute statistics for given exporting task.
    :param dataset:
    :return:
    """
    i = 0
    stats = []
    regex = r'[.]'
    for category_dataset in dataset:
        stats.append("For category: " + str(i) + "there is: " + str(len(category_dataset)) + " lines\n")
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

        stats.append("\t\t sentences per line " + str((sentences_per_line / len(category_dataset))) + '\n')
        stats.append("\t\t tokens per line " + str((tokens_per_line / len(category_dataset))) + '\n')
        i += 1
    return stats


def task_cls(generator: Generator):
    """
    Export dataset for classification task to the current working dictionary
    :param generator:
    :return:
    """
    try:
        name = "dataset"
        # set up generator to export data
        generator.is_summary = False
        generator.is_con = False
        sentences_pro = generator.get_sentences(True)
        generator.is_con = True
        generator.is_pro = False
        sentences_con = generator.get_sentences(True)

        # equal flag
        if generator.is_equal:
            l_pro = len(sentences_pro)
            l_con = len(sentences_con)
            if l_pro > l_con:
                sentences_pro = sentences_pro[:l_con]
            elif l_pro < l_con:
                sentences_con = sentences_pro[:l_pro]

            assert (len(sentences_pro) == len(sentences_con)), "Same length"
        # bert style
        if generator.is_bert:
            generator.bert([sentences_pro, sentences_con])

        # compute statistics
        statistics([sentences_pro, sentences_con])

        with open(name + "_positive.txt", "w") as f_pros:
            [f_pros.write(s) for s in sentences_pro]
        with open(name + "_negative.txt", "w") as f_cons:
            [f_cons.write(s) for s in sentences_con]

    except Exception as e:
        print("[task_cls] Exception: " + str(e), file=sys.stderr)


def reviews_nratings(generator: Generator):
    """
    Export dataset as regression task with review rating.
    :param generator:
    :return:
    """
    try:
        generator.is_summary = True
        generator.is_con = True
        generator.is_pro = True
        generator.rating = True
        generator.len_min = 1
        generator.len_max = 30
        d = {}

        # regression task, need to sort sentences according to rating from 0 to 100
        # sentences = sorted(sentences, key=lambda x:x[1], reverse=False)
        sentences = generator.get_sentences(shuffle=True)

        for s, rating in sentences:
            if rating not in d:
                d[rating] = []
            d[rating].append((s, rating))

        if generator.is_equal:
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
            generator.bert(l, True)

        l = []
        l_tmp = sorted(d)
        for val in l_tmp:
            l.append([s for s, rating in d[val]])
        statistics(l)

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
        'fkh': 'Filmy knihy hry',
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

    parser.add_argument('-s', '--sentence_type', help="Sentences in dataset \n"
                                                      + "0 -> one sentence one row\n"
                                                      + "1 -> one \"section\" of pro/con review one row\n"
                                                      + "2 -> whole section pro/con as one row\n"
                                                      + "3 -> whole review one row", type=int,
                        default=0)
    parser.add_argument('-n', '--num_category', help='Number of categories from bert', type=int, default=-1)
    parser.add_argument('-e', '--equal', help='Generate pos/neg equal size', action='store_true')
    parser.add_argument('-c', '--category', help='Concrete category', type=str, default='')
    parser.add_argument('-max', '--sentence_max_len', help='Maximum length of words', type=int, default=20)
    parser.add_argument('-min', '--sentence_min_len', help='Minimum length of words', type=int, default=3)

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
        reviews_nratings(gen)

    print(time.time() - start)

    pass


if __name__ == '__main__':
    main()
