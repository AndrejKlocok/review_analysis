# KNOT - discussions_download
import argparse
import time
import sys
import json
import os
from urllib.request import urlopen
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from crawlers.config import category_url


class Files:
    def __init__(self, category):
        self.category = category
        self.url_rest_out = None
        self.control_outfile = None
        self.outfile = None
        self.crawled_categories = None

    def __open(self, mode:str):
        self.url_rest_out = open(self.category + "_url_rest_out.txt", mode)
        self.control_outfile = open(self.category + "_control_url_out.txt", mode)
        self.outfile = open(self.category + ".txt", mode)

    def open_write(self):
        self.__open("w")

    def open_append(self):
        self.crawled_categories = self.get_crawled_categories()
        self.__open("a")

    def close(self):
        self.outfile.close()
        self.url_rest_out.close()
        self.control_outfile.close()

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
        # posledna kategoria bude nekompletna, tak ju zmazem
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


def parse_products(product, files: Files, stats: Statistics):
    prod: BeautifulSoup = product.find(class_="review-count")
    # pokud produkt neobsahuje recenze.. zapisem aspon jeho adresu do specialniho souboru
    if not prod:
        files.url_rest_out.write(product.find("a").get("href") + "\n")
    # jinak zpracuj dal
    else:
        files.outfile.write(prod.find("a").get("href") + "\n")
        # ziska pocet recenzi
        rev_count = int(prod.get_text().split()[0])
        # Maximum zobrazenych recenzi je 500
        if rev_count > 500:
            stats.add(reviews_count=rev_count, reviews_reachable=500, products_count=1)
        else:
            stats.add(reviews_count=rev_count, reviews_reachable=rev_count, products_count=1)


def parse_fashion_products(product, files: Files, stats: Statistics):
    href: str = product.find(class_="image").find("a").get("href")
    if href[-1] != "/":
        href += "/"

    href += "recenze/"

    if href.find("https") == -1:
        href = "https:" + href
    prod: BeautifulSoup = BeautifulSoup(urlopen(href), "lxml")
    rev: BeautifulSoup = prod.find(class_="review-count delimiter-blank")

    # recenze nenalezeny
    if not rev:
        files.url_rest_out.write(href + "\n")
    # jinak zpracuj dal
    else:
        files.outfile.write(href + "\n")
        # Maximum zobrazenych recenzi je 500
        rev_count = int(rev.find("span").get_text())
        if rev_count > 500:
            stats.add(reviews_count=rev_count, reviews_reachable=500, products_count=1)
        else:
            stats.add(reviews_count=rev_count, reviews_reachable=rev_count, products_count=1)


def get_recursively_urls(catlist: BeautifulSoup, files: Files, main_category: str, stats: Statistics):
    '''Funkce pro ziskani url adres, rekurzivne vola sama sebe pokud se otevrenim odkazu neotevre stranka primo s produkty,
       ale pouze s odkazy na dalsi stranky.
       Jako vstup pozaduje seznam kategorii - catlist, ziskany z hlavnich stranek.
       Dale outfile - soubor/stdout kam zapisovat
       A kontrolni outfile - soubor kam zapisovat kontrolni vypisy'''

    for cat in catlist:
        li = cat.find_all("li")
        # prochazi jednotlive class (odkazy) kategorii
        for item in li:
            # pokud class neobsahuje element strong - ve kterem je udaj o poctu recenzi, znamena to ze kategorie je odkazovana z jine kategorie, dostane se k ni az pak, proto se preskakuje
            if not item.find("strong"):
                continue

            item_stats:Statistics = Statistics()
            a = item.find("a")
            category = a.get("href")

            # categorie uz byla analyzovana
            if files.crawled_categories:
                tmp = category.split("//")[1].split(".")[0]
                if tmp in files.crawled_categories:
                    print("Skipping "+tmp)
                    continue

            next = " "
            # bude prochazet jednotlive stranky produktu dane kategorie a vytahovat z nich odkaz a pocet recenzi
            while next:
                try:
                    infile = BeautifulSoup(urlopen(category + next), "lxml")
                    # print(category)
                    if main_category == "Obleceni a moda":
                        products = infile.find_all(class_="p")
                        for product in products:
                            try:
                                parse_fashion_products(product, files, item_stats)
                            except:
                                print("[get_recursively_urls] Error in product " + product.find(class_="image").find("a").get("href"),
                                      file=sys.stderr)
                                pass
                    else:
                        products = infile.find_all(class_="rw")
                        # projde vsecky produkty na dane strance
                        for product in products:
                            try:
                                parse_products(product, files, item_stats)
                            except:
                                print("[get_recursively_urls] Error in product " + product.find("a").get("href"),
                                      file=sys.stderr)
                                pass

                    # najde odkaz na dalsi stranku
                    next = infile.find(class_="butt")
                    if next:
                        next = next.find("a", "next")
                        if next:
                            next = next.get("href")
                        # pokud nenasel odkaz, doslo se na posledni stranku -> konec cyklu
                        else:
                            next = None
                    # pokud tam odkaz neni, znamena to, ze se neotevrela stranka s produkty, ale pouze stranka s dalsimi kategoriemi, je tedy nutne se zanorit o dalsi uroven -> rekurze
                    else:
                        # Obleceni a moda ma uz priamo koncove kategorie
                        if category != "Obleceni a moda":
                            catlist = infile.find_all(class_="catlist")
                            get_recursively_urls(catlist, files, main_category, item_stats)
                except:
                    print("[get_recursively_urls] Cant open " + category + next, file=sys.stderr)
                    break
            print(a.get_text() + str(item_stats))
            files.control_outfile.write(a.get_text() + str(item_stats) + "\n")
            stats.merge(item_stats)


def task(category, url, args):
    stats = Statistics()

    try:
        f = Files(category)
        if args.c:
            f.open_write()
        elif args.r:
            f.open_append()

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

        get_recursively_urls(category_list, f, category, stats)

    except Exception as e:
        print(e)
        print("[TASK] Cant open URL: " + url, file=sys.stderr)
    finally:
        f.close()
    return stats


def main():
    parser = argparse.ArgumentParser(description="Crawl Heureka urls as defined in config.py")
    parser.add_argument("-c", action="store_true", help="Crawl main sections")
    parser.add_argument("-r", action="store_true", help="Start from last downloaded review [Repair]")

    args = parser.parse_args()

    stats = Statistics()
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_to_stuff = [executor.submit(task, category, url_add, args) for category, url_add in category_url.items()]
        for future in as_completed(future_to_stuff):
            cat_stats = future.result()
            stats.merge(cat_stats)

    print(stats)


if __name__ == '__main__':
    start = time.time()
    main()
    print(time.time() - start)
