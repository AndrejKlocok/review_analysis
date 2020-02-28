from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import NotFoundError

import json, sys
from anytree import Node
from anytree.exporter import JsonExporter, DictExporter
from operator import itemgetter


class Connector:
    def __init__(self):
        # connect to localhost
        self.es = Elasticsearch()
        try:
            res = self.es.search(index='domain', size=20)["hits"]
        except NotFoundError:
            self.init_domains()
            res = self.es.search(index='domain', size=20)["hits"]
            pass

        self.domain = {hit["_source"]["name"]: hit["_source"]["domain"] for hit in res['hits']}
        self.indexes = dict((v, k) for k, v in self.domain.items())
        self.max = 10000
        self.jsonExporter = JsonExporter(indent=2, sort_keys=True, ensure_ascii=False)
        self.dictExporter = DictExporter()
        self.category_to_domain = self.get_product_breadcrums(breadcrumbs=False)

    def init_domains(self):
        """
        Initialize domains of products to ES
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
            "shop_review": "shop_review"
        }

        for k, v in indexes.items():
            d = {
                "name": k,
                "domain": v
            }
            res = self.es.index(index="domain", doc_type='doc', body=d)
            print(res['result'])

        self.es.indices.refresh(index="domain")

    def index(self, index: str, doc: dict):
        try:
            res = self.es.index(index=index, doc_type='doc', body=doc)
            return res['result']

        except Exception as e:
            print("[Connector-index] Error: " + str(e), file=sys.stderr)
            return None

    def get_count(self, category: str, subcategory=None):
        try:
            index = self.get_domain(category)
            if not subcategory:
                res = self.es.count(index=index)['count']
            else:
                # subcategory_domain = self.get_domain(subcategory)
                body = {
                    "query": {
                        "term": {
                            "category.keyword": {
                                "value": subcategory,
                                "boost": 1.0
                            }
                        }
                    },
                }
                res = self.es.count(index=index, body=body)['count']

            return res
        except Exception as e:
            print("[get_count] Error: " + str(e), file=sys.stderr)
            return None

    def get_domain(self, category):
        try:
            return self.domain[category]
        except Exception as e:
            print("[get_domain] Error: " + str(e), file=sys.stderr)
            return None

    def _get_data(self, category, subcategory, body):
        index = self.domain[category]
        cnt = self.get_count(category, subcategory)
        if cnt > self.max:
            return self.__scroll(index, body)
        else:
            res = self.es.search(index=index, body=body)
            l = []
            for d in res["hits"]["hits"]:
                source = d["_source"]
                source["_id"] = d["_id"]
                l.append(source)
            return l

    def __scroll(self, index, query):
        """
        Execute query and return all hits by scrolling
        :param index: index for utils
        :param query: json object
        :return: dict
        """
        out = []
        try:
            # Init scroll by search
            data = self.es.search(
                index=index,
                scroll='10s',
                size=self.max,
                body=query
            )

            # Get the scroll ID
            sid = data['_scroll_id']
            scroll_size = len(data['hits']['hits'])

            while scroll_size > 0:
                l = []
                for d in data["hits"]["hits"]:
                    source = d["_source"]
                    source["_id"] = d["_id"]
                    l.append(source)
                out += l
                data = self.es.scroll(scroll_id=sid, scroll='10s')
                # Update the scroll ID
                sid = data['_scroll_id']
                # Get the number of results that returned in the last scroll
                scroll_size = len(data['hits']['hits'])

        except Exception as e:
            print("[Connector-__scroll] Error: " + str(e), file=sys.stderr)

        finally:
            return out

    def _list_to_tree(self, values):
        tree_dic = {}
        root = Node('Heureka.cz')
        tree_dic['Heureka.cz'] = root

        for val in values:
            parent = root
            for cat in val.split('|')[1:]:
                s = cat.strip()
                if s not in tree_dic:
                    tree_dic[s] = Node(s, parent=parent)
                    # tree_dic[s].parent = parent
                parent = tree_dic[s]

        return self.dictExporter.export(root)

    def match_all(self, category):
        try:
            body = {"query": {"match_all": {}}}
            return self._get_data(category, None, body)

        except Exception as e:
            print("[Connector-match_all] Error: " + str(e), file=sys.stderr)
            return None
        pass

    def get_data_breadcrumbs(self):
        try:
            body = {
                "size": 1000,
                "_source": {
                    "includes": [
                        "category_list"
                    ],
                    "excludes": []
                },
                "sort": [
                    {
                        "_doc": {
                            "order": "asc"
                        }
                    }
                ]
            }

            data = self._get_data('product', None, body)

            values = [val['category_list'] for val in data]
            values = list(set(values))
            values.sort()

            return self._list_to_tree(values), 200

        except Exception as e:
            print("[Connector-match_all] Error: " + str(e), file=sys.stderr)
            return None, 500
        pass

    def get_product_breadcrums(self, breadcrumbs=True):
        try:
            body = {
                "size": 0,
                "_source": False,
                "stored_fields": "_none_",
                "aggregations": {
                    "groupby": {
                        "composite": {
                            "size": 10000,
                            "sources": [
                                {
                                    "505": {
                                        "terms": {
                                            "field": "domain.keyword",
                                            "missing_bucket": True,
                                            "order": "asc"
                                        }
                                    }
                                },
                                {
                                    "501": {
                                        "terms": {
                                            "field": "category.keyword",
                                            "missing_bucket": True,
                                            "order": "asc"
                                        }
                                    }
                                }
                            ]
                        }
                    }
                }
            }

            try:
                res = self.es.search(index='product', body=body)
            except Exception as e:
                print("[Connector-get_index_breadcrums] Error: " + str(e), file=sys.stderr)
                return None, 404
            if breadcrumbs:
                values = [' | '.join(['Heureka.cz', val['key']['505'],
                                      val['key']['501']])  # + ' ' + str(val['doc_count'])
                          for val in res["aggregations"]["groupby"]["buckets"]]

                return self._list_to_tree(values), 200
            else:
                category_to_domain = {}
                for val in res["aggregations"]["groupby"]["buckets"]:
                    if val['key']['501'] not in category_to_domain:
                        category_to_domain[val['key']['501']] = val['key']['505']
                return category_to_domain

        except Exception as e:
            print("[Connector-get_index_breadcrums] Error: " + str(e), file=sys.stderr)
            return None, 500
        pass

    def get_category_products(self, category):
        try:
            body = {
                "size": 1000,
                "query": {
                    "term": {
                        "category.keyword": {
                            "value": category,
                            "boost": 1.0
                        }
                    }
                },
                "_source": {
                    "includes": [
                        "category",
                        "category_list",
                        "domain",
                        "product_name",
                        "url"
                    ],
                    "excludes": []
                },
                "sort": [
                    {
                        "_doc": {
                            "order": "asc"
                        }
                    }
                ]
            }

            res = self._get_data('product', category, body)
            # category not found
            if not res:
                return None, 404
            all_revs = 0
            all_prod = 0

            for product in res:
                reviews, _ = self.get_reviews_from_product(product['product_name'])
                product['reviews_len'] = len(reviews)
                all_revs += len(reviews)
                all_prod += 1

            out = sorted(res, key=itemgetter('reviews_len'), reverse=True)
            d = {
                'products': out,
                'total_products': all_prod,
                'total_reviews': all_revs
            }
            return d, 200

        except Exception as e:
            print("[Connector-get_category_products] Error: " + str(e), file=sys.stderr)
            return None, 500

    def get_subcategories_count(self, category):
        """

        :param category:
        :return: [ ('category', count:int)
        """
        try:
            index = self.domain[category]
            body = {
                "size": 0,
                "_source": False,
                "stored_fields": "_none_",
                "aggregations": {
                    "groupby": {
                        "composite": {
                            "size": 1000,
                            "sources": [
                                {
                                    "category": {
                                        "terms": {
                                            "field": "category.keyword",
                                            "missing_bucket": True,
                                            "order": "asc"
                                        }
                                    }
                                }
                            ]
                        },
                        "aggregations": {
                            "category": {
                                "filter": {
                                    "exists": {
                                        "field": "category",
                                        "boost": 1.0
                                    }
                                }
                            }
                        }
                    }
                }
            }
            res = self.es.search(index=index, body=body)
            out = {}
            for val in res["aggregations"]["groupby"]["buckets"]:
                out[val["key"]["category"]] = val["doc_count"]

            freq_sort = [(k, out[k]) for k in sorted(out, key=out.get, reverse=True)]
            return freq_sort

        except Exception as e:
            print("[Connector-match_all] Error: " + str(e), file=sys.stderr)
            return None

    def get_reviews_from_subcategory(self, category, subcategory):
        try:
            body = {
                "size": 10000,
                "query": {
                    "term": {
                        "category.keyword": {
                            "value": subcategory,
                            "boost": 1.0
                        }
                    }
                },
                "_source": {
                    "includes": [
                        "author",
                        "category",
                        "cons",
                        "cons_POS",
                        "date",
                        "domain",
                        "pro_POS",
                        "product_name",
                        "pros",
                        "rating",
                        "recommends",
                        "summary",
                        "summary_POS"
                    ],
                    "excludes": []
                },
                "sort": [
                    {
                        "_doc": {
                            "order": "asc"
                        }
                    }
                ]
            }
            return self._get_data(category, subcategory, body)

        except Exception as e:
            print("[get_reviews_from_subcategory] Error: " + str(e), file=sys.stderr)
            return None
        pass

    def get_reviews_from_product(self, product):
        try:
            body = {
                "size": 1,
                "query": {
                    "term": {
                        "product_name.keyword": {
                            "value": product,
                            "boost": 1.0
                        }
                    }
                },
                "_source": {
                    "includes": [
                        "category", "domain"
                    ]
                }
            }
            # get product category and domain from es
            res = self.es.search(index='product', body=body)
            try:
                product_category = res["hits"]["hits"][0]["_source"]['category']
                product_domain = res["hits"]["hits"][0]["_source"]['domain']
                product_domain = self.indexes[product_domain]
            except Exception as e:
                print("[get_reviews_from_product] Error: " + str(e), file=sys.stderr)
                return None, 404
            body = {
                "size": 1000,
                "query": {
                    "bool": {
                        "must": [
                            {
                                "term": {
                                    "product_name.keyword": {
                                        "value": product,
                                        "boost": 1.0
                                    }
                                }
                            },
                            {
                                "term": {
                                    "category.keyword": {
                                        "value": product_category,
                                        "boost": 1.0
                                    }
                                }
                            }
                        ],
                        "adjust_pure_negative": True,
                        "boost": 1.0
                    }
                },
                "_source": {
                    "includes": [
                        "author",
                        "category",
                        "cons",
                        "cons_POS",
                        "date_str",
                        "date",
                        "domain",
                        "pro_POS",
                        "product_name",
                        "pros",
                        "rating",
                        "recommends",
                        "summary",
                        "summary_POS"
                    ],
                    "excludes": []
                },
                "docvalue_fields": [
                    {
                        "field": "date",
                        "format": "epoch_millis"
                    }
                ],
                "sort": [
                    {
                        "_doc": {
                            "order": "asc"
                        }
                    }
                ]
            }
            # get product category and domain from es, not more than 10k?
            res = self._get_data(product_domain, product_category, body)
            if res:
                return res, 200
            else:
                return res, 404

        except Exception as e:
            print("[get_reviews_from_product] Error: " + str(e), file=sys.stderr)
            return None, 500
        pass

    def get_reviews_from_category(self, category):
        try:
            domain = self.indexes[self.category_to_domain[category]]

            body = {
                "size": 10000,
                "query": {
                    "term": {
                        "category.keyword": {
                            "value": category,
                            "boost": 1.0
                        }
                    }
                },
                "_source": {
                    "includes": [
                        "author",
                        "category",
                        "cons",
                        "cons_POS",
                        "date",
                        "domain",
                        "pro_POS",
                        "product_name",
                        "pros",
                        "rating",
                        "recommends",
                        "summary",
                        "summary_POS"
                    ],
                    "excludes": []
                },
                "sort": [
                    {
                        "_doc": {
                            "order": "asc"
                        }
                    }
                ]
            }
            # get reviews
            res = self._get_data(domain, category, body)
            if res:
                return res, 200
            else:
                return res, 404

        except Exception as e:
            print("[get_reviews_from_product] Error: " + str(e), file=sys.stderr)
            return None, 500
        pass

    def get_newest_review(self, category, product_name):
        try:
            index = self.domain[category]
            body = {"query": {"term": {"product_name.keyword": product_name}}, "sort": [{"date": {"order": "desc"}}],
                    "size": 1}
            res = self.es.search(index=index, body=body)
            # just one
            return res["hits"]["hits"][0]["_source"]

        except Exception as e:
            print("[get_newest_reviews] Error: " + str(e), file=sys.stderr)
            return None
        pass

    def get_review_by_product_author_timestr(self, category, product_name, author, date_str):
        try:
            index = self.domain[category]
            body = {
                "size": 1000, "query": {"bool": {"must": [{
                    "bool": {"must": [
                        {"term": {"product_name.keyword": {"value": product_name, "boost": 1.0}}},
                        {"term": {"date_str.keyword": {"value": date_str, "boost": 1.0}}}
                    ], "adjust_pure_negative": True, "boost": 1.0}
                },
                    {
                        "term": {
                            "author.keyword": {
                                "value": author,
                                "boost": 1.0
                            }
                        }}], "adjust_pure_negative": True, "boost": 1.0}},
                "_source": {"includes": ["author", "category", "cons", "cons_POS", "date_str", "domain", "pro_POS",
                                         "product_name",
                                         "pros", "rating", "recommends", "summary", "summary_POS"], "excludes": []},
                "docvalue_fields": [{"field": "date", "format": "epoch_millis"}], "sort": [{"_doc": {"order": "asc"}}]
            }
            res = self.es.search(index=index, body=body)
            # just one
            if res["hits"]["hits"]:
                return res["hits"]["hits"][0]["_source"]
            else:
                return None

        except Exception as e:
            print("[get_review_by_product_author_timestr] Error: " + str(e), file=sys.stderr)
            return None
        pass

    def get_review_by_shop_author_timestr(self, shop_name, author, date):
        try:
            index = 'shop_review'
            body = {
                "size": 1000,
                "query": {
                    "bool": {
                        "must": [
                            {
                                "bool": {
                                    "must": [
                                        {
                                            "term": {
                                                "shop_name.keyword": {
                                                    "value": shop_name,
                                                    "boost": 1.0
                                                }
                                            }
                                        },
                                        {
                                            "term": {
                                                "author.keyword": {
                                                    "value": author,
                                                    "boost": 1.0
                                                }
                                            }
                                        }
                                    ],
                                    "adjust_pure_negative": True,
                                    "boost": 1.0
                                }
                            },
                            {
                                "term": {
                                    "date": {
                                        "value": date,
                                        "boost": 1.0
                                    }
                                }
                            }
                        ],
                        "adjust_pure_negative": True,
                        "boost": 1.0
                    }
                },
                "_source": {
                    "includes": [
                        "author", "pros", "pros_pos", "cons", "cons_pos", "summary_pos", "date",
                        "date_str", "delivery_time", "domain", "rating", "recommends", "shop_name", "summary",
                        "aspect"],
                    "excludes": []
                },
                "docvalue_fields": [{"field": "date", "format": "epoch_millis"}],
                "sort": [{"_doc": {"order": "asc"}}]
            }
            res = self.es.search(index=index, body=body)

            # just one
            if res["hits"]["hits"]:
                return res["hits"]["hits"][0]["_source"]
            else:
                return None

        except Exception as e:
            print("[get_review_by_shop_author_timestr] Error: " + str(e), file=sys.stderr)
            return None
        pass

    def get_product_by_name(self, product_name):
        try:
            index = "product"
            body = {
                "size": 1,
                "query": {"term": {"product_name.keyword": {"value": product_name, "boost": 1.0}}},
                "_source": {
                    "includes": ["category", "category_list", "domain", "product_name", "url"],
                    "excludes": []},
                "sort": [{"_doc": {"order": "asc"}}]}
            res = self.es.search(index=index, body=body)
            # just one
            if res["hits"]["hits"]:
                return res["hits"]["hits"][0]["_source"]
            else:
                return None
        except Exception as e:
            print("[get_product_by_name] Error: " + str(e), file=sys.stderr)
            return None

    def get_shop_by_name(self, shop_name):
        try:
            index = "shop"
            body = {
                "size": 1,
                "query": {"term": {"name.keyword": {"value": shop_name, "boost": 1.0}}},
                "_source": {
                    "includes": ["name", "url_review", "domain", "url_shop", "info"],
                    "excludes": []},
                "sort": [{"_doc": {"order": "asc"}}]}
            res = self.es.search(index=index, body=body)
            # just one
            if res["hits"]["hits"]:
                return res["hits"]["hits"][0]["_source"]
            else:
                return None
        except Exception as e:
            print("[get_shop_by_name] Error: " + str(e), file=sys.stderr)
            return None

    def get_category_urls(self, category_str):
        try:
            category = self.domain[category_str]
            print(category)
            body = {"size": 1000,
                    "query": {"term": {"domain.keyword": {"value": category, "boost": 1.0}}},
                    "_source": {"includes": ["url"], "excludes": []}, "sort": [{"_doc": {"order": "asc"}}]}
            # res = self.es.search(index="product", body=body)
            # just one
            return self.__scroll("product", body)

        except Exception as e:
            print("[get_newest_reviews] Error: " + str(e), file=sys.stderr)
            return None
        pass

    def delete_index(self, category):
        try:
            index = self.domain[category]
            res = self.es.indices.delete(index=index, ignore=[400, 404])
            return res

        except Exception as e:
            print("[delete_index] Error: " + str(e), file=sys.stderr)
            return None
        pass

    def delete_product_by_domain(self, domain):
        try:
            body = {"query": {"match": {"domain": domain}}}
            res = self.es.delete_by_query(index="product", body=body)
            return res

        except Exception as e:
            print("[delete_index] Error: " + str(e), file=sys.stderr)
            return None
        pass

    def update(self, domain):
        pass

    def get_indexes_health(self):
        try:

            res = self.es.cat.indices(params={'format': 'JSON'})
            return res, 200

        except Exception as e:
            print("[get_indexes_health] Error: " + str(e), file=sys.stderr)
            return None, 500
        pass
