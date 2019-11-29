from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import NotFoundError

import json, sys


class Connector:
    def __init__(self):
        # connect to localhost
        self.es = Elasticsearch()
        try:
            res = self.es.search('domain', size=20)["hits"]
        except NotFoundError:
            self.init_domains()
            res = self.es.search('domain', size=20)["hits"]
            pass

        self.domain = { hit["_source"]["name"]:hit["_source"]["domain"] for hit in res['hits']}
        self.max = 10000

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
            'Filmy, knihy, hry': 'filmy_knihy_hry',
            'Kosmetika a zdravi': 'kosmetika_a_zdravi',
            'Sport': 'sport',
            'Hobby': 'hobby',
            'Jidlo a napoje': 'jidlo_a_napoje',
            'Stavebniny': 'stavebniny',
            'Sexualni a eroticke pomucky': 'sexualni_a_eroticke_pomucky',
            "product": "product",
        }

        for k, v in indexes.items():
            d = {
                "name": k,
                "domain": v
            }
            res = self.es.index(index="domain", doc_type='doc', body=d)
            print(res['result'])

        self.es.indices.refresh(index="domain")

    def index(self, index:str, doc:dict):
        try:
            res = self.es.index(index=index, doc_type='doc', body=doc)
            return res['result']

        except Exception as e:
            print("[Connector-index] Error: " + str(e), file=sys.stderr)
            return None

    def get_count(self, category:str, subcategory=None):
        try:
            index = self.get_domain(category)
            if not subcategory:
                res = self.es.count(index=index)['count']
            else:
                subcategory_domain = self.get_domain(subcategory)
                body = {
                    "size": 0,
                    "query": {
                        "term": {
                            "domain.keyword": {
                                "value": subcategory_domain,
                                "boost": 1.0
                            }
                        }
                    },
                    "_source": False,
                    "stored_fields": "_none_",
                    "sort": [
                        {
                            "_doc": {
                                "order": "asc"
                            }
                        }
                    ],
                    "track_total_hits": 2147483647
                }
                res = self.es.search(index=index, body=body)['hits']['total']['value']

            return res
        except Exception as e:
            print("[Connector-index] Error: " + str(e), file=sys.stderr)
            return None

    def get_domain(self, category):
        try:
            return self.domain[category]
        except Exception as e:
            print("[get_domain] Error: " + str(e), file=sys.stderr)
            return None

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
                scroll='2m',
                size=self.max,
                body=query
            )

            # Get the scroll ID
            sid = data['_scroll_id']
            scroll_size = len(data['hits']['hits'])

            while scroll_size > 0:
                out += [d["_source"] for d in data["hits"]["hits"]]
                data = self.es.scroll(scroll_id=sid, scroll='2m')
                # Update the scroll ID
                sid = data['_scroll_id']
                # Get the number of results that returned in the last scroll
                scroll_size = len(data['hits']['hits'])

        except Exception as e:
            print("[Connector-__scroll] Error: " + str(e), file=sys.stderr)

        finally:
            return out

    def match_all(self, category):
        try:
            index = self.domain[category]
            body = {"query":{"match_all" : {}}}
            return self.__scroll(index, body)

        except Exception as e:
            print("[Connector-match_all] Error: " + str(e), file=sys.stderr)
            return None
        pass

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
        pass

    def get_reviews_from_subcategory(self, category, subcategory):
        try:
            index = self.domain[category]
            body = {
                "size": 1000,
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
            return self.__scroll(index, body)

        except Exception as e:
            print("[Connector-match_all] Error: " + str(e), file=sys.stderr)
            return None
        pass

    def get_newest_review(self, category, product_name):
        try:
            index = self.domain[category]
            body = {"query": {"term": {"product_name.keyword": product_name}},"sort": [{"date": {"order": "desc"}}],"size": 1}
            res = self.es.search(index, body)
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
                "size": 1000,"query": {"bool": {"must": [{
                                "bool": {"must": [
                                        {"term": {"product_name.keyword": {"value": product_name,"boost": 1.0}}},
                                        {"term": {"date_str.keyword": {"value": date_str,"boost": 1.0}}}
                                    ],"adjust_pure_negative": True,"boost": 1.0}
                            },
                            {
                                "term": {
                                    "author.keyword": {
                                        "value": author,
                                        "boost": 1.0
                                    }
                                }}],"adjust_pure_negative": True,"boost": 1.0}},
                "_source": {"includes": ["author","category","cons","cons_POS","date_str","domain","pro_POS","product_name",
                                         "pros","rating","recommends","summary","summary_POS"],"excludes": []},
                "docvalue_fields": [{"field": "date","format": "epoch_millis"}],"sort": [{"_doc": {"order": "asc"}}]
            }
            res = self.es.search(index, body)
            # just one
            if res["hits"]["hits"]:
                return res["hits"]["hits"][0]["_source"]
            else:
                return None

        except Exception as e:
            print("[match_review] Error: " + str(e), file=sys.stderr)
            return None
        pass

    def get_product_by_name(self, product_name):
        try:
            index = "product"
            body = {
                "size": 1,
                "query": {"term": {"product_name.keyword": {"value": product_name,"boost": 1.0}}},
                "_source": {
                    "includes": ["category","category_list","domain","product_name","url"],
                    "excludes": []},
                "sort": [{"_doc": {"order": "asc"}}]}
            res = self.es.search(index, body)
            # just one
            if res["hits"]["hits"]:
                return res["hits"]["hits"][0]["_source"]
            else:
                return None
        except Exception as e:
            print("[get_product_by_name] Error: " + str(e), file=sys.stderr)
            return None

    def get_category_urls(self, category_str):
        try:
            category = self.domain[category_str]
            print(category)
            body = {"size": 1000,
                    "query": {"term": {"domain.keyword": {"value": category,"boost": 1.0}}},
                    "_source": {"includes": ["url"],"excludes": []},"sort": [{"_doc": {"order": "asc"}}]}
            res = self.es.search("product", body)
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
            body = {"query": { "match": {"domain": domain}}}
            res = self.es.delete_by_query("product", body)
            return res

        except Exception as e:
            print("[delete_index] Error: " + str(e), file=sys.stderr)
            return None
        pass

    def update(self):
        pass


def main():
    con = Connector()


if __name__ == '__main__':
    main()