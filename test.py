import json

from discussion import Files, Review, Product, Aspect, AspectCategory
from morpho_tagger import MorphoTagger

month_mapper = {
    "ledna": "January",
    "února": "February",
    "března": "March",
    "dubna": "April",
    "května": "May",
    "června": "June",
    "července": "July",
    "srpna": "August",
    "září": "September",
    "října": "October",
    "listopadu": "November",
    "prosince": "December",
    "unora": "February",
    "brezna": "March",
    "kvetna": "May",
    "cervna": "June",
    "cervence": "July",
    "zari": "September",
    "rijna": "October",
}


def map_month_to_english(date_str):
    date_str = date_str.replace('\xa0', ' ')
    m = date_str.split(" ")
    if m[1] in month_mapper:
        m[1] = month_mapper[m[1]]

    return m[0] + " " + m[1] + " " + m[2]


def fix(category):
    f = Files(category)
    # f.backup_reviews()
    f.open_write()
    with open(f.backup_name, "r") as file:
        for line in file:
            product_json = json.loads(line[:-1])
            for r in product_json["reviews"]:
                r["date"] = map_month_to_english(r["date"])
            f.reviews.write(json.dumps(product_json, ensure_ascii=False).encode('utf8').decode() + "\n")

def task(category):
    f = Files(category)
    seed_aspects = f.get_aspects()
    tagger = MorphoTagger()
    tagger.load_tagger("external/morphodita/czech-morfflex-pdt-161115-no_dia-pos_only.tagger")


    with open("aspect_log.txt", "w") as log:
        for _, aspect_category in seed_aspects.items():
            for aspect in aspect_category.aspects:
                words = tagger.pos_tagging(aspect.name)
                name_pos = " ".join(wb.lemma for wb in words[0])
                if name_pos in wrong_categories:
                    log.write(aspect_category.name +" " + name_pos+"\n")
                    for val in aspect.value_list:
                        if len(val.split()) == 1:
                            val_str = " ".join(wb.lemma for wb in tagger.pos_tagging(val)[0])
                            log.write("\t" + val_str + " " + str(val) +"\n")




def main():
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
        task(category)


if __name__ == '__main__':
    main()
