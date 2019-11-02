import json
import os
import shutil
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


class Files:
    def __init__(self, category):
        self.category = category
        self.reviews = None
        self.backup = None
        self.log = None
        self.reviews_name = self.category + "_reviews.txt"
        self.backup_name = self.category + "_backup.txt"
        self.actualized_name = self.category + "_actualized.txt"
        self.log_name = self.category + "_log.txt"
        self.aspect_name = self.category + "_aspects.txt"
        self.seed_aspect_name = self.category + "_seed.txt"
        self.embedings_name = self.category + "_embeddings.txt"

    def __open(self, mode: str):
        self.reviews = open(self.reviews_name, mode)
        self.actualized = open(self.actualized_name, mode)
        self.log = open(self.log_name , mode)

    def open_write(self):
        self.__open("w")

    def check_reviews(self)-> bool:
        if os.path.isfile(self.reviews_name) and os.stat(self.reviews_name).st_size > 0:
            # Create backup file just in case
            self.backup_reviews()
            return True
        return False

    def backup_reviews(self):
        if os.path.getsize(self.reviews_name) > 0:
            shutil.copyfile(self.reviews_name, self.backup_name)

    def get_reviews(self) -> dict:
        d = {}
        with open(self.reviews_name, "r") as file:
            for line in file:
                o = json.loads(line[:-1])
                d[o["name"]] = o

        return d

    def get_aspects(self) -> dict:
        d = {}
        with open(self.aspect_name, "r") as file:
            for line in file:
                o = json.loads(line[:-1])
                d[o["name"]] = AspectCategory.dict_to_aspect_category(o)

        return d

    def save_reviews_json(self, reviews_json):
        with open(self.reviews_name, "w") as file:
            for _, product in reviews_json:
                file.write(str(product) + "\n")

    def close(self):
        self.reviews.close()
        self.log.close()
        self.actualized.close()


class Review:
    def __init__(self):
        self.author: str = ""
        self.date: str = ""
        self.rating: str = ""
        self.recommends: str = ""
        self.pros = []
        self.cons = []
        self.summary: str = ""

    def set_author(self, author: str):
        self.author = author

    def set_date(self, date_str: str):
        if ':' in date_str:
            date_str = date_str.split(":")[1].strip()

        date_str = date_str.replace('\xa0', ' ')

        # prevod data na jednotny format
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
        if self.date == review["date"] and self.author == review["author"]:
            return True
        return False

    @staticmethod
    def JsonToReview(review:json):
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
        self.rating = rating

    def set_recommends(self, recommends: str):
        self.recommends = recommends

    def add_pro(self, pro: str):
        self.pros.append(pro)

    def add_con(self, con: str):
        self.cons.append(con)

    def set_pros(self, l:list):
        self.pros = l

    def set_cons(self, l: list):
        self.cons = l

    def set_summary(self, summary: str):
        self.summary = summary


class Product:
    def __init__(self, url: str):
        self.name: str = ""
        self.category: str = ""
        self.url: str = url
        self.reviews: list = []

    def set_name(self, name: str):
        self.name = name

    def get_name(self) -> str:
        return self.name

    def set_cateogry(self, category: str):
        self.category = category

    def add_review(self, review: Review):
        self.reviews.append(review)

    def get_reviews(self) -> list:
        return self.reviews

    def get_url(self) -> str:
        return self.url

    def merege_reviews(self, revs: list):
        self.reviews += revs

    def __str__(self):
        return json.dumps({
            "name": self.name,
            "category": self.category,
            "url": self.url,
            "reviews": [r.__dict__ for r in self.reviews]
        }, ensure_ascii=False).encode('utf8').decode()


class Aspect:
    def __init__(self, name):
        self.name = name
        self.value_list = []

    def get_name(self):
        return  self.name

    def get_values(self):
        return self.value_list

    def add_value(self, s):
        self.value_list.append(s)


class AspectCategory:
    def __init__(self, name, category, url):
        self.name = name
        self.category = category
        self.url = url
        self.aspects = []
        self.aspects_dict = {}

    def add_aspect(self, aspect):
        self.aspects.append(aspect)

    @staticmethod
    def dict_to_aspect_category(d):
        aspect_category = AspectCategory(d["name"], d["category"], d["url"])
        for a in d["Aspects"]:
            aspect = Aspect(a["name"])
            for v in a["value_list"]:
                aspect.add_value(v)
            aspect_category.add_aspect(aspect)
        return aspect_category

    def __str__(self):
        return json.dumps({
            "name": self.name,
            "category": self.category,
            "url": self.url,
            "Aspects": [r.__dict__ for r in self.aspects]
        }, ensure_ascii=False).encode('utf8').decode()