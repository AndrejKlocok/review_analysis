# KNOT - discussions_download
import json
import time
import sys
import argparse
from urllib.request import urlopen
from bs4 import BeautifulSoup
from datetime import date
from models.discussion import Files, Review, Product, Aspect, AspectCategory
import re


def parse_page(infile, product: Product, product_latest_actualization):
    actualization = False
    latest_review = None
    # pokud se aktualne zpracovavany produkt jiz nachazi ve vystupnim souboru
    # bude se provadet pouze aktualizace
    if product.get_name() in product_latest_actualization:
        actualization = True
        # posledni datum stazeni, jmeno autora posledniho prispevku
        latest_review = product_latest_actualization[product.get_name()]

    try:
        # najde div s celou diskuzi
        review_list = infile.find(class_="product-review-list")

        # volani funkce pro zpracovani stranky.. vraci tuple ve formatu (obj_reviews, True/False)
        # True - funkce skoncila stahovani nekde uprostred aktualni stranky - byly tedy stazeny pouze nejnovejsi prispevky
        # False - funkce stahla vsechny prispevky ze stranky - aktualizace se na tehle strance tedy neresi
        if format_text(review_list, product, actualization, latest_review):
            return

        # najde sekci s odkazem na dalsi stranku
        references = review_list.find(class_="pag bot")
        references = references.find("a", "next")
        # nacte odkaz na dalsi stranku
        if references:
            ref = references.get('href')
        else:
            ref = None

        # projde v cyklu vsechny stranky
        while ref:
            # otevreni stranky
            try:
                infile = BeautifulSoup(urlopen(product.get_url() + ref), "lxml")
            except IOError:
                print("[get_source_code] Cant open " + product.get_url() + ref, file=sys.stderr)

            # najde div se vsemi prispevky
            review_list = infile.find(class_="product-review-list")
            # volani formatovaci funkce
            if format_text(review_list, product, actualization, latest_review):
                return

            # najde sekci s odkazem na dalsi stranku
            references = review_list.find(class_="pag bot").find("a", "next")

            # pokud nasel ..nacti odkaz.. pujdem na dalsi stranku
            if references:
                ref = references.get('href')
            # jinak zadna dalsi stranka neexistuje.. proto bude konec
            else:
                ref = None

    except Exception as e:
        print("[get_source_code] Exception for " + product.get_url())


def parse_review(rev)->Review:
    review = Review()

    author = rev.find("strong").get_text().strip()
    review.set_author(author)
    # najde informace o autorovi prispevku

    # najde obsah prispevku
    review_text = rev.find(class_="revtext")
    # datum pridani prispevku
    review.set_date(rev.find(class_="date").get_text())

    # hodnoceni produktu
    rating = review_text.find(class_="hidden")
    if rating:
        review.set_rating(rating.get_text().replace("Hodnocení produktu: ", ""))

    # doporuceni produktu
    if review_text.find(class_="recommend-yes"):
        review.set_recommends("YES")
    elif review_text.find(class_="recommend-no"):
        review.set_recommends("NO")

    # klady
    pros = review_text.find(class_="plus")
    if pros:
        for pro in pros.find_all("li"):
            review.add_pro(pro.get_text())

    # zapory
    cons = review_text.find(class_="minus")
    if cons:
        for con in cons.find_all("li"):
            review.add_con(con.get_text())

    # shrnuti
    if review_text.p:
        review.set_summary(review_text.p.get_text())
    
    return review


def format_text(review_list, product: Product, actualization=False, latest_review=None):
    '''Zformatuje diskuzi a vysledek vypise do souboru/prikazove radky.
       V cylku prochazi jednotlive prispevky, ktere zpracovava
       Jako parametry vyzaduje:
          -review_list - div s celou diskuzi dane stranky
          -obj_reviews - seznam pro vsechny prispevky aktualniho produktu
          -actualization - True - provadi se pouze aktualizace
                         - False - stahuje se cela diskuze
          -latest_review - datum nejnovejsiho prispevku od posledniho stahovani jmeno autora nejnovejsiho prispevku od posledniho stahovani
    '''
    try:
        reviews = review_list.find_all(class_="review")
        # projde kazdy prispevek
        for rev in reviews:
            review = parse_review(rev)
            # pokud provadim aktualizaci, je nutno kontrolovat kazdy prispevek, zda-li je jeho datum a autor jiny nez ten od posledniho stazeni
            if actualization and review.compare_review(latest_review):
                # pokud se tedy narazi na shodu, vraci se vse co se doposud stahlo
                # vraci se taky True, protoze stahovani bylo zastaveno
                return True

            product.add_review(review)
    except Exception as e:
        print("[formate_text] Error: " + product.get_name() + " " + str(e))
        pass

    return False


def get_urls(json_data, string_to_append=""):
    categories_urls = []
    for _, product in json_data.items():
        try:
            url = product["url"].split(".")[0] + ".heureka.cz/" + string_to_append
            if url not in categories_urls:
                categories_urls.append(url)

        except Exception as e:
            print("[actualize_reviews] Error " + str(e))

    return categories_urls


def actualize_reviews(json_data, obj_product_dict, fast:bool):
    '''Aktualizuje soubor stazenych recenzi z heureky.. Staci skript spustit s parametrem aktualize="nazev_souboru"'''

    categories_urls = get_urls(json_data, "top-recenze")

    for category_url in categories_urls:
        print(category_url)
        next_ref = " "
        # list pro nazvy produktu ktere jiz jsou aktualni
        products_visited = []

        while next_ref:
            try:
                infile = BeautifulSoup(urlopen(category_url + next_ref), "lxml")
            except Exception as e:
                # pokud se nepodari adresu otevrit, skript bude pokracovat s jejim vynechanim
                print("[actualize_reviews] Error: "+str(e), file=sys.stderr)
                break

            review_list = infile.find_all(class_="review")

            # projde kazdy prispevek
            for rev in review_list:
                try:
                    product = rev.find("h4")
                    product_name = product.get_text()
                    url = product.find("a").get('href')
                    category = url.split(".")[0].split("//")[1]
                    product_name = product_name + " (" +category+")"

                    # pokud je nazev produktu v listu products_visited -> recenze produktu uz jsou aktualni ->
                    # netreba stahovat
                    if product_name in products_visited:
                        continue

                    url += "/recenze/"
                    product_obj = Product(url)
                    product_obj.set_name(product_name)
                    p = BeautifulSoup(urlopen(url), "lxml")

                    product_category = ""

                    # posledny je nazov produktu
                    for a in p.find(id="breadcrumbs").find_all("a")[:-1]:
                        product_category += a.get_text() + " | "
                    product_obj.set_cateogry(product_category[:-2])

                    review = parse_review(rev)
                    product_obj.add_review(review)

                    # najde nejnovejsi recenzi produktu v puvodnim souboru a vlozi ji do listu recenzi
                    # poslednich aktualizaci
                    if product_name in json_data:
                        product = json_data[product_name]
                        if product["reviews"]:
                            latest_review = product["reviews"][0]
                            if latest_review["author"] == review.author and latest_review["date"] == review.date:
                                products_visited.append(product_name)
                                # rychla metoda -> recenze existuje, starsi zaznamy existuju, nastavime next ref na None
                                if fast:
                                    next_ref = None
                                    break
                                continue
                    # pokud uz je produkt vytvoren v aktualizovacim slovniku -> pouze pripojime
                    # novejsi recenzi k te starsi
                    if product_name in obj_product_dict:
                        obj_product_dict[product_name].add_review(review)
                    # jinak musime vytvorit novy zaznam
                    else:
                        obj_product_dict[product_name] = product_obj

                except Exception as e:
                    print("[actualize_reviews] Error: " + str(e), file=sys.stderr)
                    pass

            # ak je fast metoda tak pri najdeni existujucej recenze chceme odist z cyklu
            if next_ref is None:
                break

            # najde sekci s odkazem na dalsi stranku
            references = infile.find(class_="pag bot").find("a", "next")

            # pokud nasel ..nacti odkaz.. pujdem na dalsi stranku
            if references:
                next_ref = references.get('href')
            # jinak zadna dalsi stranka neexistuje.. proto bude konec
            else:
                next_ref = None
                break


def task_seed_aspect_extraction(category:str, path:str):
    try:
        f = Files(category)
        json_data = f.get_reviews()
        categories_urls = get_urls(json_data)
        with open(path + "/" + category+"_aspects.txt", "w") as aspect_file:
            for url in categories_urls:
                try:
                    #print(url)
                    page = BeautifulSoup(urlopen(url), "lxml")
                    breadcrumbs = ""
                    max_occurence = 0
                    # posledny je nazov produktu
                    for a in page.find(id="breadcrumbs").find_all("a"):
                        breadcrumbs += a.get_text() + " | "
                    breadcrumbs = breadcrumbs[:-2]
                    name = url.split(".")[0].split("//")[1]
                    aspect_cat = AspectCategory(name, breadcrumbs, url)
                    filters = page.find(id="param-container").find_all(class_="filtr")

                    # parse aspect and its values
                    for filter_element in filters:
                        aspect_name = filter_element.find("h3").get_text()
                        aspect = Aspect(aspect_name)
                        # nonscript is text, no idea why but heureka
                        non_script = filter_element.find("noscript")
                        xml_li = filter_element.find_all("li")

                        if non_script:
                            #xml = BeautifulSoup(non_script, "lxml")
                            xml_li = non_script.find_all("li")
                        val_list = []
                        for value in xml_li:
                            v = value.find("a").get_text()
                            occurence = 0
                            try:
                                oc_str = value.find("span").get_text()
                                oc_str = re.sub('[^0-9]','', oc_str)
                                occurence = int(oc_str)

                            except Exception as e:
                                print(e)
                                pass
                            if occurence > max_occurence:
                                max_occurence = occurence
                            val_list.append( (v, occurence) )

                        # we want just 1% of max value occurence in out aspect list
                        delta = int (max_occurence * 0.01)
                        for val, occurence in val_list:
                            if occurence > delta:
                                aspect.add_value(val)

                        aspect_cat.add_aspect(aspect)
                    aspect_file.write(str(aspect_cat)+"\n")
                    print(url+" " + str(len(aspect_cat.aspects)))

                except Exception as e:
                    print("[task_seed_aspect_extraction] Error: " + str(e))


    except Exception as e:
        print("[task_seed_aspect_extraction] Error " + str(e), file=sys.stderr)


def task_actualize(category:str, fast:bool, path):
    try:
        f = Files(path + "/" + category)
        json_data = f.get_reviews()
        # vrati slovnik aktualizovanych produktu
        # { "product_name":Product}
        # slovnik pro vysledne stazene vsechny recenze vsech novych produktu
        actualized_dict_of_products = {}
        count = 0

        actualize_reviews(json_data, actualized_dict_of_products, fast)
        f.backup_reviews()
        f.open_write()
        for name, product in json_data.items():
            if name in actualized_dict_of_products:
                f.actualized.write(str(actualized_dict_of_products[name]) +"\n")
                l = [r.__dict__ for r in actualized_dict_of_products[name].get_reviews()]
                count += len(l)
                product["reviews"] = l + product["reviews"]
                del actualized_dict_of_products[name]

            f.reviews.write(json.dumps(product, ensure_ascii=False).encode('utf8').decode() + "\n")

        for _, product in actualized_dict_of_products.items():
            f.actualized.write(str(product)+"\n")

        f.close()

        if actualized_dict_of_products:
            with open(f.reviews_name, "a") as file:
                for _, product in actualized_dict_of_products.items():
                    file.write(str(product)+"\n")
                    count += len(product.get_reviews())
        print("Category: " + category + " has: " + str(count) + " reviews")

    except Exception as e:
        print("[actualize] " + str(e), file=sys.stderr)


def task(category: str, args):
    # Bude sa aktualizovat
    if args.actualize:
        # always fast for now
        task_actualize(category, True, args.path) #args.fast)
        return
    elif args.aspect:
        task_seed_aspect_extraction(category, args.path)
        return

    # dictionary pro mozny vyskyt produktu a jejich posledni aktualizace
    product_latest_actualization = {}

    # slovnik pro data_vysavace nactena ze souboru
    json_data = {}

    f = Files(args.path  + "/" +  category)

    # pokud soubor existuje a je v nem neco
    if f.check_reviews():
        # otevre soubor a nacte z nej veskera data_vysavace
        json_data = f.get_reviews()
        # pruchod slovniky a seznamy za cilem z nich vytahnout nazev produktu a nejnovejsi datum posledniho prispevku a jeho autora
        for product_name, product_json in json_data.items():
            product_latest_actualization[product_name] = product_json["reviews"][0]

    # seznam vsech produktu a recenzi k nim
    product_reviews = []

    # otevreni suboru
    try:
        f.open_write()
        with open("allCategories" + "/" + category + ".txt", 'r') as infile:
            for line in infile:
                try:
                    url = line.strip()
                    if url.find("https") == -1:
                        url = "https:" + url

                    infile = BeautifulSoup(urlopen(url), "lxml")
                    product = Product(url)

                    product_name = infile.find(class_="item")
                    if product_name:
                        product.set_name(
                            product_name.get_text().strip() + ' (' + url.split(".")[0].split("//")[1] + ')')
                    # pokud nenajde nazev produktu, tak produkt nema zadne recenze, nema cenu o nem zakladat zaznam
                    else:
                        continue

                    '''kolize, kdy heureka sloucila dva drive ruzne odkazy do jednoho na jeden produkt.. nutno predejit kolizi a
                       nezpracovavat produkt jehoz "prvni" url adresa uz byla zpracovana pred "druhou".
                       pokud uz tedy byl produkt zpracovan -> nachazi se ve final_obj_products -> netreba zpracovavat znovu -> hazelo by to
                       errory'''

                    if product.get_name() in product_reviews:
                        continue

                    product_category = ""

                    # posledny je nazov produktu
                    for a in infile.find(id="breadcrumbs").find_all("a")[:-1]:
                        product_category += a.get_text() + " | "
                    product.set_cateogry(product_category[:-2])

                    actualization = False
                    # pokud je nazev aktualne zpracovavaneho produktu ve slovniku => bude se provadet aktualizace
                    if product.get_name() in product_latest_actualization:
                        actualization = True

                    # volani funkce pro zpracovani stranek.. vraci seznam vsech recenzi
                    parse_page(infile, product, product_latest_actualization)

                    # pokud se provadi aktualizace
                    if actualization and product.get_name() in json_data:
                        l = []
                        for review in json_data[product.get_name()]["reviews"]:
                            r = Review.JsonToReview(review)
                            l.append(r)

                        f.actualized.write(str(product) + "\n")
                        product.merege_reviews(l)
                        del json_data[product.get_name()]

                    # kontrolni vypis - produkt - pocet recenzi
                    f.log.write(product.get_name() + "\t\t" + str(len(product.reviews)) + '\n')

                    # pridani do seznamu vsech produktu
                    product_reviews.append(product.get_name())
                    f.reviews.write(str(product) + "\n")
                    break

                except IOError:
                    # pokud se nepodari adresu otevrit, skript bude pokracovat s jejim vynechanim
                    print("[task] Error: Cant open URL: ", url, file=sys.stderr)
                except Exception as e:
                    print("[task] Error "+ str(e))

        # pokud neco zbylo v puvodnim souboru co nebylo potreba aktualizovat, dojde k pripojeni k novym produktum
        if json_data:
            for _, product in json_data.items():
                product_reviews.append(product["name"])
                f.reviews.write(json.dumps(product, ensure_ascii=False).encode('utf8').decode() + "\n")

    except Exception as e:
        print("[Task] " + e, file=sys.stderr)

    finally:
        f.close()


def main():
    parser = argparse.ArgumentParser(
        description="Crawl Heureka reviews as defined in config.py, Expects existance of URLS file for every category")
    parser.add_argument("-actualize", action="store_true", help="Actualize reviews")
    # parser.add_argument("-fast", action="store_true", help="Actualize reviews, when review exists breaks searching for category")
    parser.add_argument("-aspect", action="store_true", help="Get aspects from category specification")
    parser.add_argument("-path", help="Path to the dataset folder", required=True)

    args = parser.parse_args()
    categories = [
        'Elektronika',
        'Bile zbozi',
        'Dum a zahrada',
        'Chovatelstvi',
        'Auto-moto',
        'Detske zbozi',
        'Obleceni a moda',
        'Filmy, knihy, hry',
        'Kosmetika a zdravi',
        'Sport',
        'Hobby',
        'Jidlo a napoje',
        'Stavebniny',
        'Sexualni a eroticke pomucky'
    ]
    #with ThreadPoolExecutor(max_workers=1) as executor:
    #    future_to_stuff = [executor.submit(task, category, args) for category in categories]
    #    for future in as_completed(future_to_stuff):
    #        future.result()
    for category in categories:
        start = time.time()
        task(category, args)
        print(time.time() - start)

    if args.actualize:
        # zapis datumu aktualizace
        with open("actualization_dates", "a") as act_dates:
            act_dates.write(date.today().strftime("%d. %B %Y").lstrip("0") + "\n")


if __name__ == '__main__':
    start = time.time()
    main()
    print(time.time() - start)
