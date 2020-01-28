import argparse, time, sys, json, os
from urllib.request import urlopen
from bs4 import BeautifulSoup
from utils.elastic_connector import Connector
from collections import OrderedDict


class Files:
    def __init__(self, category):
        self.category = category
        self.url_no_rev = None
        self.url_log = None
        self.url_file = None
        self.crawled_categories = None

    def __open(self, mode: str):
        self.url_no_rev = open(self.category + "_url_no_rev.txt", mode)
        self.url_log = open(self.category + "_url_log.txt", mode)
        self.url_file = open(self.category + ".txt", mode)

    def open_write(self):
        self.__open("w")

    def open_append(self):
        self.crawled_categories = self.get_crawled_categories()
        self.__open("a")

    def close(self):
        self.url_file.close()
        self.url_no_rev.close()
        self.url_log.close()

    def get_crawled_categories(self) -> list:
        categories = []
        file_lines = {}
        with open(self.category + ".txt", "r") as file:
            for line in file:
                try:
                    c = line.strip().split(".")[0].split("//")[1]
                    if c not in categories:
                        categories.append(c)
                        file_lines[c] = []
                    file_lines[c].append(line)
                except:
                    pass
        file_lines.pop(categories[-1])
        categories = categories[:-1]

        with open("_" + self.category + ".txt", "w") as file:
            for _, line in file_lines.items():
                file.writelines(line)

        try:
            os.remove(self.category + "_backup.txt")
        except:
            pass
        os.rename(self.category + ".txt", self.category + "_backup.txt")
        os.rename("_" + self.category + ".txt", self.category + ".txt")
        return categories


class Statistics:
    def __init__(self):
        self.reviews_count = 0
        self.reviews_reachable = 0
        self.products_count = 0

    def merge(self, stats):
        self.reviews_count += stats.reviews_count
        self.reviews_reachable += stats.reviews_reachable
        self.products_count += stats.products_count

    def add(self, reviews_count=0, reviews_reachable=0, products_count=0):
        self.reviews_count += reviews_count
        self.reviews_reachable += reviews_reachable
        self.products_count += products_count

    def __str__(self):
        return json.dumps({
            "reviews count": self.reviews_count,
            "reviews_reachable": self.reviews_reachable,
            "products_count": self.products_count
        }, indent=2)


class HeurekaIndex():
    def __init__(self, connector):
        self.category_url = OrderedDict([
            ('Elektronika', 'https://elektronika.heureka.cz/'),
            ('Bile zbozi', 'https://bile-zbozi.heureka.cz/'),
            ('Dum a zahrada', 'https://dum-zahrada.heureka.cz/'),
            ('Chovatelstvi', 'https://chovatelstvi.heureka.cz/'),
            ('Auto-moto', 'https://auto-moto.heureka.cz/'),
            ('Detske zbozi', 'https://detske-zbozi.heureka.cz/'),
            ('Obleceni a moda', 'https://moda.heureka.cz/')
            ('Filmy, knihy, hry', 'https://filmy-hudba-knihy.heureka.cz/'),
            ('Kosmetika a zdravi', 'https://kosmetika-zdravi.heureka.cz/'),
            ('Sport', 'https://sport.heureka.cz/'),
            ('Hobby', 'https://hobby.heureka.cz/'),
            ('Jidlo a napoje', 'https://jidlo-a-napoje.heureka.cz/'),
            ('Stavebniny', 'https://stavebniny.heureka.cz/'),
            ('Sexualni a eroticke pomucky', 'https://sex-erotika.heureka.cz/')
        ])

        self.connector = connector
        self.stats = Statistics()

    def parse_products(self, product, files: Files, stats: Statistics):
        prod: BeautifulSoup = product.find(class_="review-count")

        if not prod:
            files.url_no_rev.write(product.find("a").get("href") + "\n")

        else:
            files.url_file.write(prod.find("a").get("href") + "\n")

            rev_count = int(prod.get_text().split()[0])

            if rev_count > 500:
                stats.add(reviews_count=rev_count, reviews_reachable=500, products_count=1)
            else:
                stats.add(reviews_count=rev_count, reviews_reachable=rev_count, products_count=1)

    def parse_fashion_products(self, product, files: Files, stats: Statistics):
        href: str = product.find(class_="image").find("a").get("href")
        if href[-1] != "/":
            href += "/"

        href += "recenze/"

        if href.find("https") == -1:
            href = "https:" + href
        prod: BeautifulSoup = BeautifulSoup(urlopen(href), "lxml")
        rev: BeautifulSoup = prod.find(class_="review-count delimiter-blank")

        if not rev:
            files.url_no_rev.write(href + "\n")

        else:
            files.url_file.write(href + "\n")

            rev_count = int(rev.find("span").get_text())
            if rev_count > 500:
                stats.add(reviews_count=rev_count, reviews_reachable=500, products_count=1)
            else:
                stats.add(reviews_count=rev_count, reviews_reachable=rev_count, products_count=1)

    def parse_domain(self, catlist: BeautifulSoup, files: Files, main_category: str, stats: Statistics):
        def _parse_category():
            if main_category == "Obleceni a moda":
                products = infile.find_all(class_="p")
                for product in products:
                    try:
                        self.parse_fashion_products(product, files, item_stats)
                    except:
                        print(
                            "[parse_domain] Error in product " + product.find(class_="image").find(
                                "a").get("href"),
                            file=sys.stderr)
                        pass
            else:
                products = infile.find_all(class_="rw")
                for product in products:
                    try:
                        self.parse_products(product, files, item_stats)
                    except:
                        print("[parse_domain] Error in product " + product.find("a").get("href"),
                              file=sys.stderr)
                        pass

        for cat in catlist:
            li = cat.find_all("li")
            # crawl categories
            for item in li:
                # element strong indicates number of reviews per category
                if not item.find("strong"):
                    continue

                item_stats: Statistics = Statistics()
                category = item.find("a").get("href")

                # already analyzed category
                if files.crawled_categories:
                    tmp = category.split("//")[1].split(".")[0]
                    if tmp in files.crawled_categories:
                        print("Skipping " + tmp)
                        continue

                next = " "

                while next:
                    try:
                        infile = BeautifulSoup(urlopen(category + next), "lxml")
                        _parse_category()
                        next = infile.find(class_="butt")
                        if next:
                            next = next.find("a", "next").get("href")
                        else:
                            # Fasion has already final subcategories
                            if category != "Obleceni a moda":
                                catlist = infile.find_all(class_="catlist")
                                self.parse_domain(catlist, files, main_category, item_stats)
                    except:
                        print("[parse_domain] Cant open " + category + next, file=sys.stderr)
                        break

                stats.merge(item_stats)

    def task(self, category, url):
        stats = Statistics()

        try:
            f = Files(category)
            f.open_write()

        except IOError:
            print("Cant open files for category: " + category, file=sys.stderr)
            return

        print(category, url)

        try:
            infile = BeautifulSoup(urlopen(url), "lxml")

            if category == "Obleceni a moda":
                category_list = infile.find_all(class_="cat-list")
            else:
                category_list = infile.find_all(class_="catlist")

            self.parse_domain(category_list, f, category, stats)

            self.stats.merge(stats)

        except Exception as e:
            print(e)
            print("[TASK] Cant open URL: " + url, file=sys.stderr)

        finally:
            f.close()


def main():
    parser = argparse.ArgumentParser(description="Crawl Heureka product urls")
    args = vars(parser.parse_args())

    # Elastic
    con = Connector()

    # Crawler
    heureka_index = HeurekaIndex(con)

    for category, url in heureka_index.category_url.items():
        heureka_index.task(category, url)

    print(heureka_index.stats)


if __name__ == '__main__':
    start = time.time()
    main()
    print(time.time() - start)
