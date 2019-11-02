from discussion import Files, AspectCategory, Aspect
from morpho_tagger import MorphoTagger

import json
import functools
import operator
import time


class PosReview:
    def __init__(self):
        self.review = {}
        self.pos = []
        self.aspects = []
        self.product = ""
        self.category = ""

    def set_pos(self, pos):
        self.pos = pos

    def set_review(self, review):
        self.review = review

    def append_aspect(self, aspect):
        self.aspects.append(aspect)

    def set_product(self, name):
        self.product = name

    def set_category(self, category):
        self.category = category

    def __str__(self):
        l = []
        for sentence in self.pos:
            s = [r.__dict__ for r in sentence]
            l.append(s)

        return json.dumps({
            "product": self.product,
            "category": self.category,
            "review": self.review,
            "aspects": self.aspects,
            "pos": l
        }, ensure_ascii=False).encode('utf8').decode()


def task(category):
    try:
        tagger = MorphoTagger()
        tagger.load_tagger("external/morphodita/czech-morfflex-pdt-161115-no_dia-pos_only.tagger")
        f = Files(category)
        seed_aspects = f.get_aspects()
        # POS for seed aspects
        for _, aspect_category in seed_aspects.items():

            name_pos = " ".join(aspect_category.name.split("-"))
            name_pos = tagger.pos_tagging(name_pos)
            # create general aspect
            general_aspect = Aspect("obecně")
            for wp in functools.reduce(operator.iconcat, name_pos, []):
                if wp.lemma not in aspect_category.aspects_dict:
                    aspect_category.aspects_dict[wp.lemma] = general_aspect.name

            for aspect in aspect_category.aspects:
                # name of aspect cluster is also value of its cluster
                aspect.add_value(aspect.name)
                for val in aspect.value_list:
                    v = tagger.pos_tagging(val)
                    for wp in functools.reduce(operator.iconcat, v, []):
                        if wp.lemma not in aspect_category.aspects_dict:
                            aspect_category.aspects_dict[wp.lemma] = aspect.name

        aspect_file = open(f.seed_aspect_name, "w")
        embeddings_file = open(f.embedings_name, "w")
        with open(f.reviews_name, "r") as file:
            for line in file:
                product_dict = json.loads(line[:-1])

                for review_dic in product_dict["reviews"]:
                    pos_review = PosReview()
                    # parse review
                    words = tagger.parse_review(review_dic)

                    # init POS review
                    pos_review.set_category(product_dict["category"])
                    pos_review.set_product(product_dict["name"])
                    pos_review.set_review(review_dic)
                    pos_review.set_pos(words)
                    cat_name = product_dict["name"].split("(")[-1][:-1]
                    try:
                        aspect_cat = seed_aspects[cat_name]
                        for sentence in pos_review.pos:
                            for wp in sentence:
                                if wp.lemma in aspect_cat.aspects_dict:
                                    a = aspect_cat.aspects_dict[wp.lemma]
                                    if a not in aspect_cat.aspects:
                                        pos_review.append_aspect(a)

                    except Exception as e:
                        print("[task-aspect_category] Error: " + str(e))
                    text = ""
                    #for sentence in words:
                    #    text += " ".join(wp.lemma for wp in sentence)
                    #    text += " "
                    #embeddings_file.write(text+"\n")
                    aspect_file.write(str(pos_review) + "\n")

        aspect_file.close()
        embeddings_file.close()

    except Exception as e:
        print("[task] Error: " + str(e))


def main():
    categories = [
        # 'Elektronika',
        # 'Bile zbozi',
        # 'Dum a zahrada',
        # 'Chovatelstvi',
        # 'Auto-moto',
        # 'Detske zbozi',
        # 'Obleceni a moda',
        'Filmy, knihy, hry',
        # 'Kosmetika a zdravi',
        # 'Sport',
        # 'Hobby',
        # 'Jidlo a napoje',
        # 'Stavebniny',
        # 'Sexualni a eroticke pomucky'
    ]
    for category in categories:
        start = time.time()
        task(category)
        print(time.time() - start)


if __name__ == '__main__':
    main()
