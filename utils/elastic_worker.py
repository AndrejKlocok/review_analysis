import argparse, time, json

from elastic_connector import Connector
from morpho_tagger import MorphoTagger
from datetime import datetime


def get_str_pos(l):
    s = []
    for sentence in l:
        s.append([str(wb) for wb in sentence])
    return s


def task(category, args, tagger, connection):
    try:
        print(category)
        cat_lower = category.lower().strip()
        domain = cat_lower.replace(' ', '_').replace(',','')

        with open(args.path + category + "_reviews.txt", "r") as file:
            for line in file:
                try:
                    product_dict = json.loads(line[:-1])
                    l = product_dict["name"].split("(")
                    sub_cat_name = l[-1][:-1]
                    product_name = l[0].strip()

                    #rev_elastic = []
                    product_elastic = {
                        "product_name":product_name,
                        "category": sub_cat_name,
                        "domain": domain,
                        "category_list": product_dict["category"],
                        "url":product_dict["url"]
                        }

                    if not connection.index("product", product_elastic):
                        print("Product of " + product_name + " " + " not created")

                    for rev_dic in product_dict["reviews"]:
                        rev_dic["date_str"] = rev_dic["date"]
                        datetime_object = datetime.strptime(rev_dic["date_str"], '%d. %B %Y')
                        rev_dic["date"] = datetime_object.strftime('%Y-%m-%d')
                        rev_dic["category"] = sub_cat_name
                        rev_dic["product_name"] = product_name
                        rev_dic["domain"] = domain
                        pro_pos = []
                        cons_pos = []
                        summary_pos = []

                        for pos in rev_dic["pros"]:
                            pro_pos.append(get_str_pos(tagger.pos_tagging(pos)))

                        for con in rev_dic["cons"]:
                            cons_pos.append(get_str_pos(tagger.pos_tagging(con)))

                        summary_pos = get_str_pos(tagger.pos_tagging(rev_dic["summary"]))

                        rev_dic["pro_POS"] = pro_pos
                        rev_dic["cons_POS"] = cons_pos
                        rev_dic["summary_POS"] = summary_pos
                        if not connection.index(domain, rev_dic):
                            print("Review of " + product_name + " " + " not created")

                except Exception as e:
                    print("[task] " + product_dict["name"] + "- Error: " + str(e))
                    exit(1)

    except Exception as e:
        print("[task]" + category + "- Error: " + str(e))

    pass


def main():
    parser = argparse.ArgumentParser(
        description="Scrip indexes crawled data from heureka to utils")
    requiredNamed = parser.add_argument_group('required named arguments')
    requiredNamed.add_argument('-path', help='Path to the database', required=True)
    args = parser.parse_args()

    # create tagger
    tagger = MorphoTagger()
    tagger.load_tagger("../external/morphodita/czech-morfflex-pdt-161115-no_dia-pos_only.tagger")

    # Elastic
    con = Connector()

    # categories
    categories = [
        #'Elektronika',
        #'Bile zbozi',
        #'Dum a zahrada',
        #'Chovatelstvi',
        #'Auto-moto',
        #'Detske zbozi',
        #'Obleceni a moda',
        'Filmy, knihy, hry',
        #'Kosmetika a zdravi',
        #'Sport',
        #'Hobby',
        #'Jidlo a napoje',
        #'Stavebniny',
        #'Sexualni a eroticke pomucky'
    ]

    for category in categories:
        start = time.time()
        task(category, args, tagger, con)
        print(time.time() - start)

if __name__ == '__main__':
    main()