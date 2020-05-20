import json, sys
from datetime import datetime, timezone
from utils.morpho_tagger import MorphoTagger
from utils.elastic_connector import Connector

sys.path.append('../')

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

def test():
    with open('irrelevant.tsv', 'w') as file:
        for i in range(499, 2000):
            file.write(str(i) + '\t' + '0' + '\ta\t\n')


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
            f_1_reversed[val] = key

    for key in f_2.keys():
        f_same[key] = []

    for key_main, value in f_2.items():
        i = 0
        for key in f_2.keys():
            f_same[key] = 0

        for sentence in value:
            f_same[f_1_reversed[sentence]] += 1

        freq_sort = [(k, f_same[k]) for k in sorted(f_same, key=f_same.get, reverse=True)]
        print('Cluster {} is'.format(str(key_main)))
        print(freq_sort)


def initExperimentSentences(con: Connector):
    sentence = {"review_id": "2h43lW4BkCiHlXNo5DF4",
                "experiment_id": "CxI0pnABDlca16lgqpBL",
                "cluster_number": 0,
                "product_name": "produkt",
                "category_name": "category1",
                "topic_number": 0,
                "sentence": "Nevy\u0159e\u0161en\u00e9 d\u00e1vkov\u00e1n\u00ed",
                "sentence_index": 0,
                "sentence_pos": ["vyreseny", "davkovani"],
                "sentence_type": "cons",
                }
    con.index('experiment_sentence', sentence)
    con.es.indices.refresh(index="experiment_sentence")


def initExperiment(con: Connector):
    experiment = {
        "topics_per_cluster": 3,
        "clusters_pos_count": 7,
        "clusters_con_count": 6,
        "cluster_method": "kmeans",
        "embedding_method": "sent2vec_dist",
        "category": 'aditiva2',
        "date": datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
        "pos_sentences": 4200,
        "con_sentences": 4200,
        "topics_pos": [
            {
                'cluster_number': 0,
                'topics': ["topic", "topic"]
            },
            {
                'cluster_number': 1,
                'topics': ["topic", "topic"]
            }
        ],
        "topics_con": [
            {
                'cluster_number': 0,
                'topics': ["topic", "topic"]
            },
            {
                'cluster_number': 1,
                'topics': ["topic", "topic"]
            }
        ],
    }
    res = con.index(index="experiment", doc=experiment)
    con.es.indices.refresh(index="experiment")
    # res = con.delete_experiment("ChIwpnABDlca16lgy5BN")
    print(res)


def init_users(con):
    from werkzeug.security import generate_password_hash

    user = {
        'name': 'basic_user',
        'password_hash': generate_password_hash('heslo'),
        'level': 'user',
    }
    analyst = {
        'name': 'analyst',
        'password_hash': generate_password_hash('heslo'),
        'level': 'analyst',
    }
    res = con.index(index="users", doc=user)
    print(res)
    res = con.index(index="users", doc=analyst)
    print(res)


def init_actualize(con: Connector):
    con.es.indices.refresh(index="users")
    indexes = {
        "experiment_cluster": "experiment_cluster",
        "users": "users",
        "actualize_statistic": "actualize_statistic",
    }

    for k, v in indexes.items():
        d = {
            "name": k,
            "domain": v
        }
        res = con.es.index(index="domain", doc_type='doc', body=d)
        print(res['result'])

    con.es.indices.refresh(index="domain")
    import pandas as pd

    df = pd.read_csv('../stats.csv')

    for _, row in df.iterrows():
        d = {
            'category': row['category'].strip(),
            'review_count': row['reviews'],
            'affected_products': row['affected_products'],
            'new_products': row['new_products'],
            'new_product_reviews': row['new_product_reviews'],
            'date': row['date'].strip()
        }
        con.es.index(index='actualize_statistic', doc_type='doc', body=d, request_timeout=30)
    con.es.indices.refresh(index='actualize_statistic')


def attention():
    def call_html():
        import IPython
        from IPython.core.display import display
        display(IPython.core.display.HTML('''
            <script src="/static/components/requirejs/require.js"></script>
            <script>
              requirejs.config({
                paths: {
                  base: '/static/base',
                  "d3": "https://cdnjs.cloudflare.com/ajax/libs/d3/5.7.0/d3.min",
                  jquery: '//ajax.googleapis.com/ajax/libs/jquery/2.0.0/jquery.min',
                },
              });
            </script>
            '''))
    def show_head_view(model, tokenizer, sentence_a, sentence_b=None):
        inputs = tokenizer.encode_plus(sentence_a, sentence_b, return_tensors='pt', add_special_tokens=True)
        input_ids = inputs['input_ids']
        if sentence_b:
            token_type_ids = inputs['token_type_ids']
            attention = model(input_ids, token_type_ids=token_type_ids)[-1]
            sentence_b_start = token_type_ids[0].tolist().index(1)
        else:
            attention = model(input_ids)[-1]
            sentence_b_start = None
        input_id_list = input_ids[0].tolist()  # Batch index 0
        tokens = tokenizer.convert_ids_to_tokens(input_id_list)
        head_view(attention, tokens, sentence_b_start)

    from tmp.bertviz.bertviz import head_view
    from transformers import BertForSequenceClassification, BertTokenizer

    path = '/home/andrej/Documents/school/Diplomka/model/bert_bipolar'

    model_class = BertForSequenceClassification
    model = model_class.from_pretrained(path, output_attentions=True)
    tokenizer = BertTokenizer.from_pretrained(path)

    call_html()
    show_head_view(model, tokenizer, 'Kyblík se rozbíje pokud spadné na zem.')

def main():
    # validate_clusters('../experiments/clusters/fasttext_300_dim_cz_pretrained/kmeans_cos_dist15.tsv',
    #                  '../experiments/clusters/fasttext_300_dim_cz_pretrained/kmeans_cos15_sentence_vectors/kmeans_cos15_sentence_vectors.tsv')
    # from backend.app.controllers.ReviewExperimentController import ReviewController
    # from backend.app.controllers.ExperimentClusterController import ExperimentClusterController
    # from backend.app.controllers.GenerateDataController import GenerateDataController
    #
    # from backend.app.controllers.ProductController import ProductController
    # from backend.app.controllers.DataController import DataController
    config = {
        "topics_per_cluster": 3,
        "save_data": True,
        "clusters_pos_count": 7,
        "clusters_con_count": 6,
        "cluster_method": "kmeans",
        "embedding_method": "sent2vec_dist",
        "category": 'aditiva'
    }

    config = {
        "topics_per_cluster": 1,
        "save_data": True,
        "clusters_pos_count": 7,
        "clusters_con_count": 6,
        "cluster_method": "kmeans",
        "embedding_method": "sent2vec_dist",
        "category": 'VIF Super Benzin Aditiv 500 ml'
    }

    cluster_from = {
        '_id': "gy1K5HABR2n6xeG4vIBU",
        'cluster_name': "nejaka cena",
        'cluster_number': 2,
        'cluster_sentences_count': 181,
        'experiment_id': "fS1K5HABR2n6xeG4u4DJ",
        'topics': [
            "lepsie sa neda",
            "fungovat nafta opravdu",
            "motor spotreba chod",
        ],
        "type": "pos"
    }
    cluster_to = {
        '_id': "fi1K5HABR2n6xeG4vIAm",
        'cluster_name': "dobra cena",
        'cluster_number': 1,
        'cluster_sentences_count': 162,
        'experiment_id': "fS1K5HABR2n6xeG4u4DJ",
        'topics': [
            "lepsie sa neda",
            "fungovat test dobry",
            "motor spotreba pouzivat"
        ],
        "type": "pos"
    }
    content = {
        'task_type': "embeddings",
        'model_type': "general",
        'sentence_type': "sentence = row",
        'equal': False,
        'sentence_min_len': 3,
        'sentence_max_len': 24,
        'categories': ['shop']
    }
    content = {
        '_id': "V9rrm28B7fzBP-GLWEkv",
        'category': 'aditiva',
    }

    config = {
        'sentence_id': "Fy1s6XABR2n6xeG4z5Gf",
        'cluster_id': "ai1s6XABR2n6xeG4ipB-",
        'topic_number': 1,
    }
    config = { 'cluster_from_id': "gi1K5HABR2n6xeG4vIBQ",
               'cluster_to_id': "gi1K5HABR2n6xeG4vIBQ",
               'topic_from_number': 0,
               'topic_to_number': 1}

    config = {
        "topic_id": "uC3uUHEBR2n6xeG4fKl7",
        "topic_number": 1,
        'cluster_id': 'sC3tUHEBR2n6xeG40KnK',
        'sentence_id': 'ui3uUHEBR2n6xeG4fKnW',
    }

    con = Connector()
    #cnt = ExperimentClusterController(con)
    #res, retcode = cnt.update_sentence(config)
    #print(retcode)

    #res, retcode = con.append_experiment_cluster_topic(config['cluster_id'], config['topics'])

    #con.get_product_by_name('Nokian WetProof 205/55 R16 91V')
    #product = 'Rowenta RO6477EA'
    #res = con.get_product_by_name(product)
    #print(res)
    #res = con.get_reviews_from_product(product)
    #print(len(res[0]))
    # if content['experiment_id']:
    from datetime import date
    print(date.today().strftime('%d-%m'))



if __name__ == '__main__':
    main()
