import json, sys

from utils.discussion import Files
from utils.morpho_tagger import MorphoTagger
from utils.elastic_connector import Connector
from utils.generate_dataset import GeneratorController
sys.path.append('../')

from backend.app.controllers.ExperimentController import ExperimentController
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


def statistics_sentences(path):
    d = {}
    cnt = 0
    with open(path, 'r') as file:
        for line in file:
            if line[:-1] not in d:
                d[line[:-1]] = 1
            else:
                d[line[:-1]] += 1
            cnt += 1
    freq_sort = [(k, d[k]) for k in sorted(d, key=d.get, reverse=True)]
    print('Sentences: ' + str(cnt))
    print('TOP sentences: ')
    for k, v in freq_sort:
        print(str(k) + ' | ' + str(v))
        if v < 10:
            break

def test():
    with open('irrelevant.tsv', 'w') as file:
        for i in range(499, 2000):
            file.write(str(i)+'\t'+'0'+'\ta\t\n')

def validate_clusters(file1, file2):
    def _load_tsv(f):
        d = {}
        with open(f, 'r') as file:
            for line in file:
                s = line[:-1].split('\t')
                if s[1] not in d:
                    d[s[1]] = []
                d[s[1]].append(s[0])
        return d

    f_1 = _load_tsv(file1)
    f_2 = _load_tsv(file2)
    f_1_reversed = {}
    f_same = {}
    for key, value in f_1.items():
        for val in value:
            f_1_reversed[val]=key

    for key in f_2.keys():
        f_same[key]=[]

    for key_main, value in f_2.items():
        i = 0
        for key in f_2.keys():
            f_same[key] = 0

        for sentence in value:
            f_same[f_1_reversed[sentence]] += 1

        freq_sort = [(k, f_same[k]) for k in sorted(f_same, key=f_same.get, reverse=True)]
        print('Cluster {} is'.format(str(key_main)))
        print(freq_sort)


def main():
    #validate_clusters('../experiments/clusters/fasttext_300_dim_cz_pretrained/kmeans_cos_dist15.tsv',
    #                  '../experiments/clusters/fasttext_300_dim_cz_pretrained/kmeans_cos15_sentence_vectors/kmeans_cos15_sentence_vectors.tsv')

    config = {
        "topics_per_cluster": 3,
        "download_data": False,
        "clusters_count": 7,
        "cluster_method": "kmeans",
        "embedding_method": "sent2vec_dist",
        "categories": ['aditiva']
    }
    con = Connector()
    #res = con.get_reviews_from_category(config['categories'][0])
    #print(res[0][0])

    cnt = ExperimentController(con)
    cnt.cluster_similarity(config)

    # res = con.index('shop_review', r_d)
    # print(res)
    # return
    # res = con.get_review_by_shop_author_timestr(r_d['shop_name'], r_d['author'], r_d['date'])
    # print(res)
    # res = con.match_all('shop_review')
    # res = con.get_shop_by_name(shop_d['name'])
    #res = con.get_subcategories_count("Bile zbozi")
    #res = con.get_product_breadcrums()
    #print(res)



if __name__ == '__main__':
    main()
