from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

import json, sys


class Connector:
    def __init__(self):
        # connect to localhost
        self.es = Elasticsearch()
        res = self.es.search('domain', size=20)["hits"]
        self.domain = { hit["_source"]["name"]:hit["_source"]["domain"] for hit in res['hits']}
        self.max = 10000

    def index(self, index:str, doc:dict):
        try:
            res = self.es.index(index=index, doc_type='doc', body=doc)
            return res['result']

        except Exception as e:
            print("[Connector-index] Error: " + str(e), file=sys.stderr)
            return None

    def get_count(self, index:str):
        """
        Get count of all documents from index
        example output <class 'dict'>: {'count': 24243, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}}
        :param index:
        :return: dictionary
        """
        try:
            res = self.es.count(index=index)
            return res
        except Exception as e:
            print("[Connector-index] Error: " + str(e), file=sys.stderr)
            return None

    def __scroll(self, index, query):
        """
        Execute query and return all hits by scrolling
        :param index: index for elastic
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

    def match_exact(self):
        pass

    def delete(self):
        pass

    def update(self):
        pass


def main():
    con = Connector()


if __name__ == '__main__':
    main()