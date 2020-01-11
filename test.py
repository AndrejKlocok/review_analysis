import json

from utils.discussion import Files
from utils.morpho_tagger import MorphoTagger
from utils.elastic_connector import Connector

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


def morpho(category):
    f = Files(category)
    seed_aspects = f.get_aspects()
    tagger = MorphoTagger()
    tagger.load_tagger("external/morphodita/czech-morfflex-pdt-161115-no_dia-pos_only.tagger")
    wrong_categories = []

    with open("aspect_log.txt", "w") as log:
        for _, aspect_category in seed_aspects.items():
            for aspect in aspect_category.aspects:
                words = tagger.pos_tagging(aspect.name)
                name_pos = " ".join(wb.lemma for wb in words[0])
                if name_pos in wrong_categories:
                    log.write(aspect_category.name + " " + name_pos + "\n")
                    for val in aspect.value_list:
                        if len(val.split()) == 1:
                            val_str = " ".join(wb.lemma for wb in tagger.pos_tagging(val)[0])
                            log.write("\t" + val_str + " " + str(val) + "\n")


def statistics_sentences():
    d = {}
    with open('tmp/dataset_negative.txt', 'r') as file:
        for line in file:
            if line[:-1] not in d:
                d[line[:-1]] = 1
            else:
                d[line[:-1]] += 1
    freq_sort = [(k, d[k]) for k in sorted(d, key=d.get, reverse=True)]
    print('TOP sentences: ')
    for k, v in freq_sort:
        print(str(k) + ' | ' + str(v))
        if v < 10:
            break

def test():
    with open('irrelevant.tsv', 'w') as file:
        for i in range(500):
            file.write(str(i)+'\t'+'0'+'\ta\t\n')

def main():
    con = Connector()
    # res = con.get_shop_by_name('test42')
    # print(res)
    # print('\n\n')
    res = con.get_product_by_name('Rowenta Silence Force Extreme AAAA Turbo Animal Care RO6477EA')
    #res = con.get_newest_review('Bile zbozi', 'Gillette Mach3 12 ks')
    shop_d = {
        'name': 'shop_name',
        'url_review': 'shop_url',
        'url_shop': 'shop_exit_url',
        'info': 'shop_info',
        'domain': 'shop',
    }
    # res = con.index('shop', shop_d)
    # print(res)

    r_d = {
        'author': 'Andrej',
        'date': '2011-10-17T16:43:41',
        'recommends': 'YES',
        'delivery_time': '0',
        'rating': '100%',
        'summary': 'jsem velmi spokojena', 'summary_pos': [],
        'pros': [], 'pros_pos': [],
        'cons': [], 'cons_pos': [],
        'domain': 'shop_review',
        'shop_name': 'test',
        'aspect': [],
    }
    r_d['date_str'] = '17. October 2011'
    # res = con.index('shop_review', r_d)
    # print(res)
    # return
    # res = con.get_review_by_shop_author_timestr(r_d['shop_name'], r_d['author'], r_d['date'])
    # print(res)
    # res = con.match_all('shop_review')
    # res = con.get_shop_by_name(shop_d['name'])
    # res = con.get_subcategories_count("Bile zbozi")
    print(res)



if __name__ == '__main__':
    main()
