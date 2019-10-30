from discussion import Files
from unidecode import unidecode
import json
import re
from morpho_tagger import MorphoTagger


def task(category):
    try:
        tagger = MorphoTagger()
        tagger.load_tagger("external/morphodita/czech-morfflex-pdt-161115-no_dia-pos_only.tagger")
        f = Files(category)
        with open(f.reviews_name, "r") as file:
            for line in file:
                product_dict = json.loads(line[:-1])

                for review_dic in product_dict["reviews"]:
                    # remove diacritic
                    text = unidecode(". ".join(review_dic["pros"] + review_dic["cons"]+[review_dic["summary"]]))
                    words = tagger.pos_tagging(text, True)
                    for w in words:
                        print(w)
                    return

    except Exception as e:
        print("[task] Error: "+str(e))


def main():
    categories = [
        # 'Elektronika',
        # 'Bile zbozi',
        # 'Dum a zahrada',
        # 'Chovatelstvi',
        # 'Auto-moto',
        # 'Detske zbozi',
        # 'Obleceni a moda',
        'Filmy, knihy, hry',
        # 'Kosmetika a zdravi',
        # 'Sport',
        # 'Hobby',
        # 'Jidlo a napoje',
        # 'Stavebniny',
        # 'Sexualni a eroticke pomucky'
    ]
    for category in categories:
        task(category)


if __name__ == '__main__':
    main()