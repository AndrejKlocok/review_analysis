"""
This file contains model representation of Review, Product, Aspect and AspectCategory, that can be crawled with heureka
crawler class.

Author: xkloco00@stud.fit.vutbr.cz
"""

import json
from datetime import date, timedelta

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


class Review:
    """
    Class represents review model
    """
    def __init__(self):
        self.author: str = ""
        self.date: str = ""
        self.rating: str = ""
        self.recommends: str = ""
        self.pros = []
        self.cons = []
        self.summary: str = ""

    def set_author(self, author: str):
        """
        Set author name
        :param author: author name
        :return:
        """
        self.author = author

    def set_date(self, date_str: str):
        """
        Set the date of review creation.
        :param date_str:
        :return:
        """
        if ':' in date_str:
            date_str = date_str.split(":")[1].strip()

        date_str = date_str.replace('\xa0', ' ')

        # transform date to one type
        if "včera" in date_str:
            date_str = (date.today() - timedelta(1)).strftime("%d. %B %Y").lstrip("0")
        elif "před" in date_str:
            date_str = date.today().strftime("%d. %B %Y").lstrip("0")

        m = date_str.split(" ")
        if m[1] in month_mapper:
            m[1] = month_mapper[m[1]]
            date_str = m[0] + " " + m[1] + " " + m[2]
        self.date = date_str

    def compare_review(self, review:dict) -> bool:
        """
        Compare product reviews for the date and the name of author
        :param review:
        :return:
        """
        if self.date == review["date"] and self.author == review["author"]:
            return True
        return False

    @staticmethod
    def JsonToReview(review: json):
        """
        Load review from JSON
        :param review:
        :return:
        """
        r: Review = Review()
        r.set_author(review["author"])
        r.set_date(review["date"])
        r.set_summary(review["summary"])
        r.set_recommends(review["recommends"])
        r.set_rating(review["rating"])
        r.set_cons(review["pros"])
        r.set_pros(review["cons"])

        return r

    def set_rating(self, rating: str):
        """
        Set review rating.
        :param rating:
        :return:
        """
        self.rating = rating

    def set_recommends(self, recommends: str):
        """
        Set review recommendation option.
        :param recommends:
        :return:
        """
        self.recommends = recommends

    def add_pro(self, pro: str):
        """
        Append pro sentence/text.
        :param pro:
        :return:
        """
        self.pros.append(pro)

    def add_con(self, con: str):
        """
        Append con sentence/text.
        :param con:
        :return:
        """
        self.cons.append(con)

    def set_pros(self, l: list):
        """
        Set pros section as list
        :param l:
        :return:
        """
        self.pros = l

    def set_cons(self, l: list):
        """
        Set cons section as list
        :param l:
        :return:
        """
        self.cons = l

    def set_summary(self, summary: str):
        """
        Set summary text.
        :param summary:
        :return:
        """
        self.summary = summary


class Product:
    """
    Class represents Product model.
    """
    def __init__(self, url: str):
        self.name: str = ""
        self.category: str = ""
        self.url: str = url
        self.reviews: list = []

    def set_name(self, name: str):
        """
        Set the name of product
        :param name:
        :return:
        """
        self.name = name

    def get_name(self) -> str:
        """
        Get the name of product
        :return:
        """
        return self.name

    def set_category(self, category: str):
        """
        Set the category of product
        :param category:
        :return:
        """
        self.category = category

    def get_category(self):
        """
        Get the category of product
        :return:
        """
        return self.category

    def add_review(self, review: Review):
        """
        Append review object to products review list.
        :param review:
        :return:
        """
        self.reviews.append(review)

    def get_reviews(self) -> list:
        """
        Get products review list.
        :return:
        """
        return self.reviews

    def get_url(self) -> str:
        """
        Get product url.
        :return:
        """
        return self.url

    def merge_reviews(self, revs: list):
        self.reviews += revs

    def __str__(self):
        """
        Override toString method/
        :return:
        """
        return json.dumps({
            "name": self.name,
            "category": self.category,
            "url": self.url,
            "reviews": [r.__dict__ for r in self.reviews]
        }, ensure_ascii=False).encode('utf8').decode()


class Aspect:
    """
    Class represents heureka aspect model.
    """
    def __init__(self, name):
        self.name = name
        self.value_list = []

    def get_name(self):
        """
        Get the name of aspect.
        :return:
        """
        return  self.name

    def get_values(self):
        """
        Get the value of aspect.
        :return:
        """
        return self.value_list

    def add_value(self, s):
        """
        Append value of aspect.
        :param s:
        :return:
        """
        self.value_list.append(s)

    def __str__(self):
        """
        Override toString method.
        :return:
        """
        return json.dumps(self.__dict__, ensure_ascii=False).encode('utf8').decode()


class AspectCategory:
    """
    Class represents aspect category model.
    """
    def __init__(self, name, category, url):
        self.name = name
        self.category = category
        self.url = url
        self.aspects = []
        self.aspects_dict = {}

    def add_aspect(self, aspect):
        """
        Append aspect object to the list of aspects
        :param aspect:
        :return:
        """
        self.aspects.append(aspect)

    @staticmethod
    def dict_to_aspect_category(d):
        """
        Transform input dictionary instance to AspectCategory model.
        :param d:
        :return:
        """
        aspect_category = AspectCategory(d["name"], d["category"], d["url"])

        # we can have aspects or processed aspect dict with POS
        if "Aspects" in d:
            for a in d["Aspects"]:
                aspect = Aspect(a["name"].lower())
                for v in a["value_list"]:
                    aspect.add_value(v.lower())
                aspect_category.add_aspect(aspect)
        elif "aspect_dict" in d:
            aspect_category.aspects_dict = d["aspect_dict"]

        return aspect_category

    def pos_str(self):
        """
        ToString method with use of aspect dictionary.
        :return:
        """
        return json.dumps({
            "name": self.name,
            "category": self.category,
            "url": self.url,
            "aspect_dict": self.aspects_dict
        }, ensure_ascii=False).encode('utf8').decode()

    def __str__(self):
        """
        Override toString method.
        :return:
        """
        return json.dumps({
            "name": self.name,
            "category": self.category,
            "url": self.url,
            "Aspects": [r.__dict__ for r in self.aspects]
        }, ensure_ascii=False).encode('utf8').decode()
