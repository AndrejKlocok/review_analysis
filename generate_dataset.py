import argparse
import time

from elastic_connector import Connector


def task(category, connector:Connector):
    data = connector.get_subcategories_count(category)
    with open("dataset_aspects.txt", "w") as file:
        for name, count in data[:5]:
            print("Writing dataset of " + name + " with " + str(count) + " reviews")
            review_list = connector.get_reviews_from_subcategory(category, name)
            for review in review_list:
                # write data
                [file.write(pro + "\n") for pro in review["pros"]]
                [file.write(c+ "\n") for c in review["cons"]]
                if review["summary"]:
                    file.write(review["summary"]+ "\n")
            break


    pass


def main():
    parser = argparse.ArgumentParser(
        description="Scrip generates desired dataset from elastic db")
    #parser.add_argument("-", action="store_true", help="Crawl main sections")
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