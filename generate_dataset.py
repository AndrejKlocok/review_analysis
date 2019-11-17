import argparse
import time
import re
import random

from elastic_connector import Connector


class Generator:

    def __init__(self, category: str, connector: Connector, is_bert: bool = False, is_pro: bool = True,
                 is_con: bool = True, is_summary: bool = True):
        self.__category = category
        self.__con = connector
        self.is_pro = is_pro
        self.is_con = is_con
        self.is_summary = is_summary
        self.is_bert = is_bert

    def get_sentences(self, top_categories, shuffle=False, len_min=3, len_max=23):
        data = self.__con.get_subcategories_count(self.__category)
        sentences = []
        for name, count in data[:top_categories]:
            print("Dataset of " + name + " with " + str(count) + " reviews")
            review_list = self.__con.get_reviews_from_subcategory(self.__category, name)
            for review in review_list:
                # write data
                if self.is_pro:
                    [self.__get_sentence(pro, sentences, len_min, len_max) for pro in review["pros"]]
                if self.is_con:
                    [self.__get_sentence(c, sentences, len_min, len_max) for c in review["cons"]]
                if self.is_summary and review["summary"]:
                    self.__get_sentence(review["summary"], sentences, len_min, len_max)
        if shuffle:
            random.shuffle(sentences)

        return sentences

    def __get_sentence(self, s: str, sentences: list, len_min, len_max, regex=r'[,.]'):
        l = re.split(regex, s)
        for sentence in l:
            if sentence and len_min < len(sentence.split()) < len_max and sentence.split()[0].isalpha():
                # file.write(sentence.strip().capitalize() + ".\n")
                sentences.append(sentence.strip().capitalize() + ".\n")


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


def task_cls(generator: Generator, num_category=1, equal=False):
    try:
        generator.is_summary = False
        name = "dataset"
        if generator.is_bert:
            name += "_bert"

        generator.is_con = False
        sentences_pro = generator.get_sentences(num_category, True)
        generator.is_con = True
        generator.is_pro = False
        sentences_con = generator.get_sentences(num_category, True)

        if equal:
            l_pro = len(sentences_pro)
            l_con = len(sentences_con)
            if l_pro > l_con:
                sentences_pro = sentences_pro[:l_con]
            elif l_pro < l_con:
                sentences_con = sentences_pro[:l_pro]

            assert (len(sentences_pro) == len(sentences_con)), "Same length"

        with open(name+"_positive.txt", "w") as f_pros:
            [f_pros.write(s) for s in sentences_pro]
        with open(name + "_negative.txt", "w") as f_cons:
            [f_cons.write(s) for s in sentences_con]

    except Exception as e:
        print("[task_cls] Exception: " + str(e))


def main():
    parser = argparse.ArgumentParser(
        description="Scrip generates desired dataset from elastic db")
    parser.add_argument('-emb', '--embeddings', help='Generate dataset for embeddings with all sentences, 80-20', action='store_true')
    parser.add_argument('-cls', '--classification', help='Generate dataset for sentiment classification +-', action='store_true')
    #parser.add_argument('-bert', '--bert', help='Use bert form', action='store_true')
    parser.add_argument('-n', '--num_category', help='Number of categories from bert', type=int, default=1)
    parser.add_argument('-e', '--equal_dataset', help='Generate pos/neg equal size', action='store_true', default=False)

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
    start = time.time()
    if args['embeddings']:
        for category in categories:
            gen = Generator(category, con)
            task_emb(gen)
    elif args['classification']:
        for category in categories:
            gen = Generator(category, con, args['bert'])
            task_cls(gen, args['num_category'], args['equal_dataset'])

    print(time.time() - start)

    pass


if __name__ == '__main__':
    main()
