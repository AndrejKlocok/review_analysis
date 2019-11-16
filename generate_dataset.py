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
            if sentence and len_min < len(sentence.split()) < len_max:
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


def task_cls(generator: Generator):
    try:
        generator.is_summary = False
        name = "dataset"
        if generator.is_bert:
            name += "_bert"

        generator.is_con = False
        sentences_pro = generator.get_sentences(1, True)
        generator.is_con = True
        generator.is_pro = False
        sentences_con = generator.get_sentences(1, True)

        with open(name+"_pros.txt", "w") as f_pros:
            [f_pros.write(s) for s in sentences_pro]
        with open(name + "_cons.txt", "w") as f_cons:
            [f_cons.write(s) for s in sentences_con]

    except Exception as e:
        print("[task_emb] Exception: " + str(e))


def main():
    parser = argparse.ArgumentParser(
        description="Scrip generates desired dataset from elastic db")
    parser.add_argument('-emb', '--embeddings', help='Generate dataset for embeddings with all sentences, 80-20', action='store_true')
    parser.add_argument('-cls', '--classification', help='Generate dataset for sentiment classification +-, 80-20', action='store_true')
    parser.add_argument('-bert', '--bert', help='Use bert form', action='store_true')

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
            task_cls(gen)

    print(time.time() - start)

    pass


if __name__ == '__main__':
    main()
