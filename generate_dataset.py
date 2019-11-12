import argparse
import time
import re

from elastic_connector import Connector


def file_write(s, file):
    l = re.split(r'[,.]', s)
    for sentence in l:
        if sentence and len(sentence.split()) > 3:
            file.write(sentence.strip() + "\n")


def task(category, connector:Connector):
    data = connector.get_subcategories_count(category)
    with open("dataset_aspects.txt", "w") as file:
        for name, count in data[:5]:
            print("Writing dataset of " + name + " with " + str(count) + " reviews")
            review_list = connector.get_reviews_from_subcategory(category, name)
            for review in review_list:
                # write data
                [file_write(pro, file) for pro in review["pros"]]
                [file_write(c, file) for c in review["cons"]]
                if review["summary"]:
                    file_write(review["summary"], file)
            break



def main():
    parser = argparse.ArgumentParser(
        description="Scrip generates desired dataset from elastic db")
    args = parser.parse_args()

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

    for category in categories:
        start = time.time()
        task(category, con)
        print(time.time() - start)

    pass

if __name__ == '__main__':
    main()