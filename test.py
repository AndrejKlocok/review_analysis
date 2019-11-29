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


def main():
    con = Connector()
    #res = con.es.search('domain', size=20)["hits"]
    res = con.match_all('product')
    print(len(res))
    print(res[0])
    #datetime_object = datetime.strptime('30. January 2019', '%d. %B %Y')
    #print(datetime_object.strftime('%Y-%m-%d'))
    #res = con.es.search('domain', size=20)["hits"]
    #res = con.get_newest_review('Elektronika', 'Threadripper')
    #print(res)
    #res = con.get_review_by_product_author_timestr('Bile zbozi', 'Gillette Mach3 12 ks', 'Mirka', '11. November 2019')
    #res = con.get_category_urls('Filmy, knihy, hry')
    #res = con.get_product_by_name('Threadripper')
    #print(res)
    #print(res[0])
    #print(len(res))
    #doc = {
    #    'author': 'kimchy',
    #    'text': 'Elasticsearch: cool. bonsai cool.',
    #    'timestamp': datetime.now(),
    #}
    #res = es.index(index="test", doc_type='test', id=1, body=doc)
    #print(res['result'])

    #res = es.index(index="bile_zbozi", doc_type='doc', body=doc)
    #con.es.indices.refresh(index="domain")
    #res = es.search(index="config", body={"query": {"match": {

    #    'name': {'query': 'Bile zbozi', "operator" : "and"}
    #}}})

    #res = con.get_reviews_from_subcategory('Bile zbozi', 'vysavace')
    #print(res[:5])
    #print(len(res))
    #res = con.es.search(index="product", body={"query":{"match_all" : {}}})
    #print(res['result'])
    #indexes = { hit["_source"]["name"]:hit["_source"]["index"] for hit in res['hits']['hits']}

    #print(indexes)
    #res = con.get('domain', 1)
    #print("Got %d Hits:" % res['hits']['total']['value'])
    #for hit in res['hits']['hits']:
    #    print(hit["_source"])

    #es.indices.delete(index='bile_zbozi', ignore=[400, 404])
    #es.indices.delete(index='product', ignore=[400, 404])
    #es.indices.delete(index='domain', ignore=[400, 404])

if __name__ == '__main__':
    main()
