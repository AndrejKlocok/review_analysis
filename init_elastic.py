"""
This file contains implementation is used for initialization of domain mapping index in elastic-search and users for GUI

Author: xkloco00@stud.fit.vutbr.cz
"""
from utils.elastic_connector import Connector
from werkzeug.security import generate_password_hash


def init_domains(con):
    """
    Initialize domains for review analysis.
    :return:
    """
    indexes = {
        'Elektronika': 'elektronika',
        'Bile zbozi': 'bile_zbozi',
        'Dum a zahrada': 'dum_a_zahrada',
        'Chovatelstvi': 'chovatelstvi',
        'Auto-moto': 'auto-moto',
        'Detske zbozi': 'detske_zbozi',
        'Obleceni a moda': 'obleceni_a_moda',
        'Filmy knihy hry': 'filmy_knihy_hry',
        'Kosmetika a zdravi': 'kosmetika_a_zdravi',
        'Sport': 'sport',
        'Hobby': 'hobby',
        'Jidlo a napoje': 'jidlo_a_napoje',
        'Stavebniny': 'stavebniny',
        'Sexualni a eroticke pomucky': 'sexualni_a_eroticke_pomucky',
        "product": "product",
        "shop": "shop",
        "shop_review": "shop_review",
        "experiment_sentence": "experiment_sentence",
        "experiment": "experiment",
        "experiment_cluster": "experiment_cluster",
        "users": "users",
        "actualize_statistic": "actualize_statistic",
        "experiment_topic": "experiment_topic",
    }

    for k, v in indexes.items():
        d = {
            "name": k,
            "domain": v
        }
        res = con.index(index="domain", doc_type='doc', body=d)
        print(res['result'])

    con.indices.refresh(index="domain")
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


def main():
    con = Connector()
    init_domains(con)


if __name__ == '__main__':
    main()
