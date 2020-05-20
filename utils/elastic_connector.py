"""
This file contains implementation of Connector class, which represents dataset of the project. This class connects to
elastic-search node and offers API of methods for manipulation with dataset, mainly reviews and working with training
models and experiments.

Author: xkloco00@stud.fit.vutbr.cz
"""
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError

import json, sys
from anytree import Node
from anytree.exporter import JsonExporter, DictExporter
from operator import itemgetter


class Connector:
    """
    Class handles requests to elastic search and provides CRUD operations for products/shops/reviews/users/clustering
    experiments/topics.
    """
    def __init__(self, host=None, port=None):
        """
        Constructor initializes elastic connection with given host and port information and initializes domains mapping.
        :param host:
        :param port:
        """
        # connect to localhost
        if not host:
            host = 'localhost'
        if not port:
            port = 9200
        self.es = Elasticsearch([{
            'host': host,
            'port': port
        }])
        # get domain indexes from elastic
        try:
            res = self.es.search(index='domain', size=40)["hits"]
        except NotFoundError:
            # on the error init those domains
            self.init_domains()
            res = self.es.search(index='domain', size=40)["hits"]
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
            res = self.es.index(index="domain", doc_type='doc', body=d)
            print(res['result'])

        self.es.indices.refresh(index="domain")

    def index(self, index: str, doc: dict):
        """
        Index document doc with index index to elastic
        :param index:
        :param doc:
        :return:
        """
        try:
            res = self.es.index(index=index, doc_type='doc', body=doc)
            if res['result'] != 'created':
                raise Exception('Document was not indexed')
            self.es.indices.refresh(index=index)
            return res

        except Exception as e:
            print("[Connector-index] Error: " + str(e), file=sys.stderr)
            return None

    def get_count(self, category: str, subcategory=None, body=None):
        """
        Get count of documents (reviews) from index defined by category. If subcategory is specified get count
        of reviews from that subcategory and if body is specified (shop reviews) get count of reviews from query
        characterizing shop name
        :param category: domain
        :param subcategory:
        :param body:
        :return:
        """
        try:
            index = self.get_domain(category)

            if not subcategory:
                res = self.es.count(index=index)['count']
            elif body:
                res = self.es.count(index='shop_review', body=body)['count']
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
        """
        Get domain index from category
        :param category:
        :return:
        """
        try:
            return self.domain[category]
        except Exception as e:
            print("[get_domain] Error: " + str(e), file=sys.stderr)
            return category

    def _get_data(self, domain, subcategory, body):
        """
        Get data from index characterized by domain. According to count of resulting reviews perform normal search
        or scroll over data.
        :param domain:
        :param subcategory:
        :param body:
        :return: list of documents
        """
        index = self.domain[domain]
        if domain == 'shop':
            cnt = self.get_count(domain, subcategory, body)
        else:
            cnt = self.get_count(domain, subcategory)
        # maximum size of returned documents from search is 10000
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

    def _list_to_tree(self, values: list):
        """
        Transform list of separated strings by '|' to tree structure, mainly for breadcrumbs.
        :param values:  list of strings separated by '|'
        :return: dict representation of Tree
        """
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
        """
        Get ALL documents from domain.
        :param category: domain category
        :return:
        """
        try:
            body = {"size": 10000, "query": {"match_all": {}}}
            return self._get_data(category, None, body)

        except Exception as e:
            print("[Connector-match_all] Error: " + str(e), file=sys.stderr)
            return None
        pass

    def get_data_breadcrumbs(self):
        """
        Return breadcrumbs of categories to products with full length.
        :return:
        """
        try:
            body = {
                "size": 10000,
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
            print("[Connector-get_data_breadcrumbs] Error: " + str(e), file=sys.stderr)
            return None, 500
        pass

    def get_product_breadcrums(self, breadcrumbs=True):
        """
        Get simplified path of product breadcrumbs from domains to subcategories to products or map categories to
        domain, shop names to domain.
        :param breadcrumbs:
        :return:
        """
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
                print("[Connector-get_product_breadcrums] Error: " + str(e), file=sys.stderr)
                return None, 404

            if breadcrumbs:
                # get breadcrumbs
                values = [' | '.join(['Heureka.cz', val['key']['505'],
                                      val['key']['501']])
                          for val in res["aggregations"]["groupby"]["buckets"]]
                values.append('Heureka.cz | shop')

                return self._list_to_tree(values), 200
            else:
                # get category to domain mapping
                category_to_domain = {}
                for val in res["aggregations"]["groupby"]["buckets"]:
                    if val['key']['501'] not in category_to_domain:
                        category_to_domain[val['key']['501']] = val['key']['505']
                # shop name to domain mapping
                body = {"size": 10000, "query": {"match_all": {}}}
                for shop in self._get_data('shop', None, body):
                    category_to_domain[shop['name']] = 'shop_review'

                return category_to_domain

        except Exception as e:
            print("[Connector-get_product_breadcrumbs] Error: " + str(e), file=sys.stderr)
            return None, 500

    def get_shops(self):
        """
        Get shop documents with metadata statistics.
        :return: dict
        """
        try:
            body = {"size": 10000, "query": {"match_all": {}}}

            res = self._get_data('shop', None, body)
            all_revs = 0
            all_prod = 0

            for shop in res:
                reviews_len = self.get_shop_rev_cnt(shop['name'])
                shop['reviews_len'] = reviews_len
                all_revs += reviews_len
                all_prod += 1

            out = sorted(res, key=itemgetter('reviews_len'), reverse=True)
            d = {
                'products': out,
                'total_products': all_prod,
                'total_reviews': all_revs
            }
            return d, 200

        except Exception as e:
            print("[Connector-get_shops] Error: " + str(e), file=sys.stderr)
            return None, 500

    def get_shop_rev_cnt(self, shop_name: str):
        """
        Get count of shop reviews defined by shop_name
        :param shop_name:
        :return:
        """
        try:
            body = {
                "query": {
                    "term": {
                        "shop_name.keyword": {
                            "value": shop_name,
                            "boost": 1.0
                        }
                    }
                }
            }

            res = self.es.count(index='shop_review', body=body)['count']
            return res

        except Exception as e:
            print("[Connector-get_shops] Error: " + str(e), file=sys.stderr)
            return 0

    def get_product_rev_cnt(self, product_name: str):
        """
        Get count of product reviews defined by product_name.
        :param product_name:
        :return:
        """
        try:
            body = {
                "size": 1,
                "query": {
                    "term": {
                        "product_name.keyword": {
                            "value": product_name,
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
                product_domain = res["hits"]["hits"][0]["_source"]['domain']
            except Exception as e:
                print("[get_reviews_from_product] Error: " + str(e), file=sys.stderr)
                return 0

            body = {
                "query": {
                    "term": {
                        "product_name.keyword": {
                            "value": product_name,
                            "boost": 1.0
                        }
                    }
                }
            }

            res = self.es.count(index=product_domain, body=body)['count']
            return res

        except Exception as e:
            print("[get_product_rev_cnt] Error: " + str(e), file=sys.stderr)
            return 0

    def get_category_products(self, category: str):
        """
        Get products from given category and metadata about count of products and reviews.
        :param category:
        :return:
        """
        try:
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

            products = []
            # loop over products
            for product in res:
                reviews_len = self.get_product_rev_cnt(product['product_name'])
                all_revs += reviews_len
                all_prod += 1
                # only products with more then 10 reviews are viable
                if reviews_len < 10:
                    continue
                product['reviews_len'] = reviews_len
                products.append(product)

            out = sorted(products, key=itemgetter('reviews_len'), reverse=True)
            d = {
                'products': out,
                'total_products': all_prod,
                'total_reviews': all_revs
            }
            return d, 200

        except Exception as e:
            print("[Connector-get_category_products] Error: " + str(e), file=sys.stderr)
            return None, 500

    def get_subcategories_count(self, domain: str):
        """
        Get count of reviews for subcategories of domain
        :param domain:
        :return: [ ('category', count:int)
        """
        try:
            index = self.domain[domain]
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
            print("[Connector-get_subcategories_count] Error: " + str(e), file=sys.stderr)
            return None

    def get_shop_reviews(self):
        """
        Get all reviews from shop domain.
        :return: list of review documents
        """
        try:
            body = {"size": 10000, "query": {"match_all": {}}}

            res = self._get_data('shop_review', None, body)

            if res:
                return res, 200
            else:
                return res, 404

        except Exception as e:
            print("[get_shop_reviews] Error: " + str(e), file=sys.stderr)
            return None, 500

    def get_reviews_from_product(self, product: str):
        """
        Get products reviews.
        :param product:
        :return:
        """
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
                "size": 10000,
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
            index = self.domain[product_domain]
            res = self.es.search(index=index, body=body)
            l = []
            for d in res["hits"]["hits"]:
                source = d["_source"]
                source["_id"] = d["_id"]
                l.append(source)

            if l:
                return l, 200
            else:
                return l, 404

        except Exception as e:
            print("[get_reviews_from_product] Error: " + str(e), file=sys.stderr)
            return None, 500

    def get_reviews_from_category(self, sub_category):
        """
        Get reviews from products of subcategory.
        :param sub_category:
        :return: list of documents
        """
        try:
            domain = self.indexes[self.category_to_domain[sub_category]]

            body = {
                "size": 10000,
                "query": {
                    "term": {
                        "category.keyword": {
                            "value": sub_category,
                            "boost": 1.0
                        }
                    }
                }
            }
            # get reviews
            res = self._get_data(domain, sub_category, body)
            if res:
                return res, 200
            else:
                return res, 404
        except KeyError as e:
            return [], 404

        except Exception as e:
            print("[get_reviews_from_category] Error: " + str(e), file=sys.stderr)
            return None, 500

    def get_reviews_from_shop(self, shop_name: str, isEnough=False):
        """
        Get shop reviews from shop defined by shop_name.
        :param shop_name: shop name
        :param isEnough: maximum number of reviews, so we dont use all memory within web browser
        :return: list of review documents.
        """
        try:

            body = {
                "size": 10000,
                "query": {
                    "term": {
                        "shop_name.keyword": {
                            "value": shop_name,
                            "boost": 1.0
                        }
                    }
                },
                "sort": [
                    {
                        "date": {
                            "order": "desc"
                        }
                    }
                ]
            }
            if isEnough:
                # 10 000 documents are max
                res = self.es.search('shop_review', body=body)
                l = []
                for d in res["hits"]["hits"]:
                    source = d["_source"]
                    source["_id"] = d["_id"]
                    l.append(source)
                res = l
            else:
                # get reviews
                res = self._get_data('shop_review', None, body)

            if res:
                return res, 200
            else:
                return res, 404

        except KeyError as e:
            return [], 404

        except Exception as e:
            print("[get_reviews_from_product] Error: " + str(e), file=sys.stderr)
            return None, 500

    def get_newest_review(self, domain: str, product_name: str):
        """
        Get newest review from product
        :param domain: domain of product
        :param product_name:  the name of product
        :return:
        """
        try:
            index = self.domain[domain]
            body = {"query": {"term": {"product_name.keyword": product_name}}, "sort": [{"date": {"order": "desc"}}],
                    "size": 1}
            res = self.es.search(index=index, body=body)
            # just one
            return res["hits"]["hits"][0]["_source"]

        except Exception as e:
            print("[get_newest_reviews] Error: " + str(e), file=sys.stderr)
            return None
        pass

    def get_review_by_product_author_timestr(self, category: str, product_name: str, author: str, date_str: str):
        """
        Get review from domain category characterized by function arguments. Used to find if the review already exists
        in elastic.
        :param category: name of category domain
        :param product_name: name of product
        :param author: the name of author
        :param date_str: string representation of date
        :return: review document: dict
        """
        try:
            if category not in self.domain:
                index = category
            else:
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

    def get_review_by_shop_author_timestr(self, shop_name: str, author: str, date):
        """
        Get review from shop domain  characterized by function arguments. Used to find if the review already exists
        in elastic.
        :param shop_name: the name of the shop
        :param author:  the name of the author
        :param date: date
        :return:
        """
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

    def get_product_by_name(self, product_name: str):
        """
        Get products document by its name.
        :param product_name: the name of product
        :return: document representing product: dict
        """
        try:
            index = "product"
            body = {
                "size": 1,
                "query": {"term": {"product_name.keyword": {"value": product_name, "boost": 1.0}}},
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

    def get_shop_by_name(self, shop_name: str):
        """
        Get shop document by its name.
        :param shop_name: the name of shop
        :return: document representing shop: dict
        """
        try:
            index = "shop"
            body = {
                "size": 1,
                "query": {"term": {"name.keyword": {"value": shop_name, "boost": 1.0}}}
            }
            res = self.es.search(index=index, body=body)
            # just one
            if res["hits"]["hits"]:
                return res["hits"]["hits"][0]["_source"]
            else:
                return None
        except Exception as e:
            print("[get_shop_by_name] Error: " + str(e), file=sys.stderr)
            return None

    def get_category_urls(self, category_str: str):
        """
        Get category urls from products.
        :param category_str:
        :return: list of documents of urls
        """
        try:
            category = self.domain[category_str]

            body = {"size": 1000,
                    "query": {"term": {"domain.keyword": {"value": category, "boost": 1.0}}},
                    "_source": {"includes": ["url"], "excludes": []}, "sort": [{"_doc": {"order": "asc"}}]}
            # res = self.es.search(index="product", body=body)
            # just one
            return self.__scroll("product", body)

        except Exception as e:
            print("[get_category_urls] Error: " + str(e), file=sys.stderr)
            return None
        pass

    def delete_index(self, domain: str):
        """
        Delete index from elastic and its documents characterized by domain category.
        :param domain:
        :return:
        """
        try:
            index = self.domain[domain]
            res = self.es.indices.delete(index=index, ignore=[400, 404])
            return res

        except Exception as e:
            print("[delete_index] Error: " + str(e), file=sys.stderr)
            return None
        pass

    def delete_product_by_domain(self, domain: str):
        """
        Delete product documents by domain category.
        :param domain: domain of products
        :return:
        """
        try:
            body = {"query": {"match": {"domain": domain}}}
            res = self.es.delete_by_query(index="product", body=body)
            return res

        except Exception as e:
            print("[delete_index] Error: " + str(e), file=sys.stderr)
            return None
        pass

    def get_indexes_health(self):
        """
        Get indexes health as json.
        :return:
        """
        try:
            res = self.es.cat.indices(params={'format': 'JSON'})
            return res, 200

        except Exception as e:
            print("[get_indexes_health] Error: " + str(e), file=sys.stderr)
            return None, 500

    def get_experiments(self):
        """
        Get clustering experiments in given format.
        :return: list of clustering experiments
        """
        try:
            body = {
                "sort": [
                    {
                        "_doc": {
                            "order": "asc"
                        }
                    }
                ]
            }
            # get all clustering experiments
            res = self.es.search(index='experiment', body=body)

            if res["hits"]["hits"]:
                l = []
                for d in res["hits"]["hits"]:
                    source = d["_source"]
                    source["_id"] = d["_id"]
                    # get experiment clusters
                    source['clusters_pos'], _ = self.get_experiment_clusters(source["_id"], 'pos')
                    source['clusters_con'], _ = self.get_experiment_clusters(source["_id"], 'con')
                    # count of clusters
                    source['clusters_pos_count'] = len(source['clusters_pos'])
                    source['clusters_con_count'] = len(source['clusters_con'])
                    # count of sentences in positive clusters
                    source['pos_sentences'] = 0
                    for cluster in source['clusters_pos']:
                        source['pos_sentences'] += cluster['cluster_sentences_count']
                    # count of sentences in negative clusters
                    source['con_sentences'] = 0
                    for cluster in source['clusters_con']:
                        source['con_sentences'] += cluster['cluster_sentences_count']

                    l.append(source)
                return l, 200
            else:
                return None, 200

        except Exception as e:
            print("[get_experiments] Error: " + str(e), file=sys.stderr)
            return None, 500

    def get_experiments_by_category(self, category_name: str, product_name: str = ''):
        """
        Get experiment by the subcategory or product name or shop name.
        :param category_name:
        :param product_name:
        :return:
        """
        try:
            body = {
                "query": {
                    "term": {
                        "category.keyword": {
                            "value": category_name,
                            "boost": 1.0
                        }
                    }
                },
                "sort": [
                    {
                        "_doc": {
                            "order": "asc"
                        }
                    }
                ]
            }
            res = self.es.search(index='experiment', body=body)

            if res["hits"]["hits"]:
                l = []
                for d in res["hits"]["hits"]:
                    source = d["_source"]
                    source["_id"] = d["_id"]
                    # get experiment clusters
                    source['clusters_pos'], _ = self.get_experiment_clusters(source["_id"], 'pos', product_name)
                    source['clusters_con'], _ = self.get_experiment_clusters(source["_id"], 'con', product_name)
                    # count of clusters
                    source['clusters_pos_count'] = len(source['clusters_pos']) if source['clusters_pos'] else 0
                    source['clusters_con_count'] = len(source['clusters_con']) if source['clusters_con'] else 0
                    # count of sentences in positive clusters
                    source['pos_sentences'] = 0
                    for cluster in source['clusters_pos']:
                        source['pos_sentences'] += cluster['cluster_sentences_count']
                    # count of sentences in negative clusters
                    source['con_sentences'] = 0
                    for cluster in source['clusters_con']:
                        source['con_sentences'] += cluster['cluster_sentences_count']

                    l.append(source)
                return l, 200
            else:
                return None, 400

        except Exception as e:
            print("[get_experiments_by_category] Error: " + str(e), file=sys.stderr)
            return None, 500

    def get_experiment_clusters(self, experiment_id: str, cluster_type: str, product_name: str = ''):
        """
        Get clusters of experiment defined by method arguments.
        :param experiment_id: elastics id of experiment
        :param cluster_type: type of cluster pos/con
        :param product_name: the name of product for partial extraction of sentences
        :return:
        """
        try:
            body = {
                "size": 10000,
                "query": {
                    "bool": {
                        "must": [
                            {
                                "term": {
                                    "experiment_id.keyword": {
                                        "value": experiment_id,
                                        "boost": 1.0
                                    }
                                }
                            },
                            {
                                "term": {
                                    "type.keyword": {
                                        "value": cluster_type,
                                        "boost": 1.0
                                    }
                                }
                            }
                        ],
                        "adjust_pure_negative": True,
                        "boost": 1.0
                    }
                }
            }
            res = self.es.search(index='experiment_cluster', body=body)

            if res["hits"]["hits"]:
                l = []
                for d in res["hits"]["hits"]:
                    source = d["_source"]
                    source["_id"] = d["_id"]
                    source['sentences'], _ = self.get_experiment_clusters_sentences(source["_id"], product_name)
                    source['topics'], _ = self.get_experiment_clusters_topics(source["_id"])
                    source['cluster_sentences_count'] = len(source['sentences']) if source['sentences'] else 0
                    l.append(source)
                return l, 200
            else:
                return [], 200

        except Exception as e:
            print("[get_experiment_clusters] Error: " + str(e), file=sys.stderr)
            return None, 500

    def get_experiment_clusters_topics(self, cluster_id: str):
        """
        Get topics of clusters experiment defined by id of cluster.
        :param cluster_id:
        :return: list of topic documents
        """
        try:
            body = {
                "size": 10000,
                "query": {
                    "term": {
                        "cluster_number.keyword": {
                            "value": cluster_id,
                            "boost": 1.0
                        }
                    }
                }
            }
            res = self.es.search(index='experiment_topic', body=body)

            if res["hits"]["hits"]:
                l = []
                for d in res["hits"]["hits"]:
                    source = d["_source"]
                    source["_id"] = d["_id"]
                    l.append(source)
                return l, 200
            else:
                return [], 404

        except Exception as e:
            print("[get_experiment_clusters_topics] Error: " + str(e), file=sys.stderr)
            return None, 500

    def get_experiment_clusters_sentences(self, cluster_id: str, product_name: str = ''):
        """
        Get sentences from clustering experiment define by methods arguments.
        :param cluster_id: elastic id of cluster
        :param product_name: the name of product
        :return:
        """
        try:
            body = {
                "size": 10000,
                "query": {
                    "term": {
                        "cluster_number.keyword": {
                            "value": cluster_id,
                            "boost": 1.0
                        }
                    }
                },
                "sort": [
                    {
                        "_doc": {
                            "order": "asc"
                        }
                    }
                ]
            }
            res = self.es.search(index='experiment_sentence', body=body)

            if res["hits"]["hits"]:
                l = []
                for d in res["hits"]["hits"]:
                    source = d["_source"]
                    source["_id"] = d["_id"]

                    if product_name:
                        # if the object of experiment is product, we want only product relevant sentences
                        if source['product_name'] != product_name:
                            continue
                    l.append(source)
                return l, 200
            else:
                return [], 404

        except Exception as e:
            print("[get_experiment_clusters_sentences] Error: " + str(e), file=sys.stderr)
            return None, 500

    def delete_experiment(self, experiment_id: str):
        """
        Recursively remove experiment from elastic with its clusters, topics and sentences.
        :param experiment_id: elastic experiment id
        :return:
        """
        try:
            body = {
                "query": {
                    "term": {
                        "_id": experiment_id
                    }
                }
            }
            # delete experiment document
            res = self.es.delete_by_query(index="experiment", body=body)

            body = {
                "query": {
                    "term": {
                        "experiment_id.keyword": {
                            "value": experiment_id,
                            "boost": 1.0
                        }
                    }
                }
            }
            # delete clusters, topics, sentences
            self.es.delete_by_query(index="experiment_cluster", body=body)
            self.es.delete_by_query(index="experiment_topic", body=body)
            self.es.delete_by_query(index="experiment_sentence", body=body)
            return res, 200

        except Exception as e:
            print("[delete_experiment] Error: " + str(e), file=sys.stderr)
            return None, 500

    def update_experiment(self, experiment_id: str, sal_pos, sal_con):
        """
        Update experiment salient words.
        :param experiment_id:
        :param sal_pos:
        :param sal_con:
        :return:
        """
        try:
            body = {
                "doc": {
                    "sal_pos": sal_pos,
                    "sal_con": sal_con
                }
            }
            res = self.es.update(index='experiment', id=experiment_id, body=body)
            return res, 200

        except Exception as e:
            print("[update_experiment] Error: " + str(e), file=sys.stderr)
            return None, 500

    def update_experiment_cluster_name(self, experiment_cluster_id: str, cluster_name: str):
        """
        Update experiments cluster name.
        :param experiment_cluster_id: elastic id of cluster
        :param cluster_name: new name of cluster
        :return:
        """
        try:
            body = {
                "doc": {
                    "cluster_name": cluster_name,
                }
            }
            res = self.es.update(index='experiment_cluster', id=experiment_cluster_id, body=body)
            self.es.indices.refresh(index="experiment_cluster")
            return res, 200

        except Exception as e:
            print("[update_experiment_cluster_name] Error: " + str(e), file=sys.stderr)
            return None, 500

    def merge_experiment_cluster(self, cluster_d_from, cluster_d_to):
        """
        Merge cluster and its topics, sentences from cluster_d_from to cluster cluster_d_to
        :param cluster_d_from: cluster from which data will be moved
        :param cluster_d_to: cluster to which data will be moved
        :return:
        """
        try:
            body = {
                "script": {
                    "source": "ctx._source.cluster_number = params.cluster_id",
                    "lang": "painless",
                    "params": {
                        "cluster_id": cluster_d_to['_id']
                    }
                },
                "query": {
                    "term": {
                        "cluster_number.keyword": cluster_d_from['_id']
                    }
                }
            }
            # update cluster sentences
            res = self.es.update_by_query(index='experiment_sentence', body=body)
            self.es.indices.refresh(index="experiment_sentence")

            body = {
                "script": {
                    "source": "ctx._source.cluster_number = params.cluster_id",
                    "lang": "painless",
                    "params": {
                        "cluster_id": cluster_d_to['_id']
                    }
                },
                "query": {
                    "term": {
                        "cluster_number.keyword": cluster_d_from['_id']
                    }
                }
            }
            # update experiment topic
            res = self.es.update_by_query(index='experiment_topic', body=body)
            self.es.indices.refresh(index="experiment_topic")
            # remove old cluster
            res = self.es.delete(index="experiment_cluster", id=cluster_d_from['_id'])
            if res['result'] != 'deleted':
                raise Exception('cluster: {} :was not updated'.format(cluster_d_from['cluster_name']))

            self.es.indices.refresh(index="experiment_cluster")

            return res, 200

        except Exception as e:
            print("[merge_experiment_cluster] Error: " + str(e), file=sys.stderr)
            return None, 500

    def update_experiment_cluster_sentence(self, experiment_cluster_id: str, sentence_id: str, topic_numb, topic_id: str):
        """
        Update sentence cluster/topic origin by method arguments
        :param experiment_cluster_id:
        :param sentence_id:
        :param topic_numb:
        :param topic_id:
        :return:
        """
        try:
            body = {
                "doc": {
                    "cluster_number": experiment_cluster_id,
                    "topic_number": topic_numb,
                    "topic_id": topic_id,
                }
            }
            res = self.es.update(index='experiment_sentence', id=sentence_id, body=body)
            self.es.indices.refresh(index="experiment_sentence")
            return res, 200

        except Exception as e:
            print("[update_experiment_cluster_sentence] Error: " + str(e), file=sys.stderr)
            return None, 500

    def update_experiment_cluster_topic(self, topic_id: str, topic_name: str):
        """
        Update topic name
        :param topic_id: elastic id of topic
        :param topic_name: new nape of topic
        :return:
        """
        try:
            body = {
                "doc": {
                    "name": topic_name,
                }
            }
            res = self.es.update(index='experiment_topic', id=topic_id, body=body)
            self.es.indices.refresh(index="experiment_topic")
            return res, 200

        except Exception as e:
            print("[update_experiment_cluster_topic] Error: " + str(e), file=sys.stderr)
            return None, 500

    def update_experiment_cluster_sentences(self, topic_id_from: str,
                                            cluster_id_to: str, topic_numb_to, topic_id_to: str):
        """
        Update all sentences from cluster with new topic/cluster
        :param topic_id_from:
        :param cluster_id_to:
        :param topic_numb_to:
        :param topic_id_to:
        :return:
        """
        try:
            body = {
                "script": {
                    "source": "ctx._source.cluster_number=params.cluster_id; ctx._source.topic_number=params.topic_number; ctx._source.topic_id=params.topic_id",
                    "lang": "painless",
                    "params": {
                        "cluster_id": cluster_id_to,
                        "topic_number": topic_numb_to,
                        "topic_id": topic_id_to,
                    }
                },
                "query": {
                    "term": {
                        "topic_id.keyword": topic_id_from
                    }
                },
            }
            res = self.es.update_by_query(index='experiment_sentence', body=body)
            self.es.indices.refresh(index="experiment_sentence")

            res = self.es.delete(index="experiment_topic", id=topic_id_from)
            if res['result'] != 'deleted':
                raise Exception('topic: {} :was not deleted'.format(topic_id_from))

            self.es.indices.refresh(index="experiment_topic")
            return res, 200

        except Exception as e:
            print("[update_experiment_cluster_sentence] Error: " + str(e), file=sys.stderr)
            return None, 500

    def get_user_by_id(self, id: str):
        """
        Get user by elastic id
        :param id:
        :return:
        """
        try:
            res = self.es.get(index='users', id=id)
            source = res["_source"]
            source["_id"] = res["_id"]
            return source

        except Exception as e:
            print("[get_user_by_id] Error: " + str(e), file=sys.stderr)
            return None

    def get_review_by_id(self, id: str, domain: str):
        """
        Get review by elastic id
        :param id:
        :param domain:
        :return:
        """
        try:
            index = self.category_to_domain[domain]
            res = self.es.get(index=index, id=id)
            source = res["_source"]
            source["_id"] = res["_id"]
            return source

        except Exception as e:
            print("[get_review_by_id] Error: " + str(e), file=sys.stderr)
            return None

    def get_user_by_name(self, name: str):
        """
        Get user by name
        :param name:
        :return:
        """
        try:
            body = {
                "size": 1,
                "query": {"term": {"name.keyword": {"value": name, "boost": 1.0}}}
            }
            res = self.es.search(index='users', body=body)
            # just one
            if res["hits"]["hits"]:
                source = res["hits"]["hits"][0]["_source"]
                source["_id"] = res["hits"]["hits"][0]["_id"]
                return source

            return None

        except Exception as e:
            print("[get_user_by_name] Error: " + str(e), file=sys.stderr)
            return None

    def get_actualization_by_category(self, category_name: str):
        """
        Get actualization statistics from domain category.
        :param category_name:
        :return:
        """
        try:
            body = {
                "size": 10000,
                "query": {
                    "term": {
                        "category.keyword": {
                            "value": category_name,
                            "boost": 1.0
                        }
                    }
                },
                "sort": [
                    {
                        "_doc": {
                            "order": "asc"
                        }
                    }
                ]
            }
            res = self._get_data('actualize_statistic', None, body)
            if res:
                return res, 200
            else:
                return res, 400

        except Exception as e:
            print("[get_actualization_by_category] Error: " + str(e), file=sys.stderr)
            return None, 500
