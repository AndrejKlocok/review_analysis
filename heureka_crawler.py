# KNOT - discussions_download
import json, time, sys, argparse, re
from datetime import datetime
from urllib.request import urlopen
from bs4 import BeautifulSoup
from datetime import date
from utils.elastic_connector import Connector
from utils.discussion import Files, Review, Product, Aspect, AspectCategory
from utils.morpho_tagger import MorphoTagger


class HeurekaCrawler:
    def __init__(self, connector, tagger):
        self.categories = [
            #'Elektronika',
            'Bile zbozi',
            #'Dum a zahrada',
            #'Chovatelstvi',
            #'Auto-moto',
            #'Detske zbozi',
            #'Obleceni a moda',
            #'Filmy, knihy, hry',
            #'Kosmetika a zdravi',
            #'Sport',
            #'Hobby',
            #'Jidlo a napoje',
            #'Stavebniny',
            #'Sexualni a eroticke pomucky'
        ]
        self.connector = connector
        self.tagger = tagger

        pass

    def parse_product_page(self, infile, product: Product, category_domain):
        '''
        Parse review page of product, build up product object by appending new reviews.

        :param infile:
        :param product:
        :param category_domain:
        :return:
        '''

        try:
            # najde div s celou diskuzi
            review_list = infile.find(class_="product-review-list")

            # if True then we parsed all product reviews, else we need to go on
            if self.parse_product_revs(review_list, product, category_domain):
                return

            # find section with new page
            references = review_list.find(class_="pag bot")
            references = references.find("a", "next")
            if references:
                ref = references.get('href')
            else:
                ref = None

            # loop over footer with references to next pages
            while ref:
                try:
                    infile = BeautifulSoup(urlopen(product.get_url() + ref), "lxml")
                except IOError:
                    print("[get_source_code] Cant open " + product.get_url() + ref, file=sys.stderr)

                review_list = infile.find(class_="product-review-list")
                # parse revies on page
                if self.parse_product_revs(review_list, product, category_domain):
                    return

                # reference to next page
                references = review_list.find(class_="pag bot").find("a", "next")

                if references:
                    ref = references.get('href')
                # if there is no next page break while
                else:
                    ref = None

        except Exception as e:
            print("[get_source_code] Exception for " + product.get_url())

    def parse_review(self, rev) -> Review:
        """
        Create review object from xml representation
        :param rev:
        :return:
        """
        review = Review()
        # author name
        author = rev.find("strong").get_text().strip()
        review.set_author(author)

        review_text = rev.find(class_="revtext")
        # set date
        review.set_date(rev.find(class_="date").get_text())
        # set rating
        rating = review_text.find(class_="hidden")
        if rating:
            review.set_rating(rating.get_text().replace("Hodnocení produktu: ", ""))
        # set recommendation
        if review_text.find(class_="recommend-yes"):
            review.set_recommends("YES")
        elif review_text.find(class_="recommend-no"):
            review.set_recommends("NO")
        # set pros
        pros = review_text.find(class_="plus")
        if pros:
            for pro in pros.find_all("li"):
                review.add_pro(pro.get_text())
        # set cons
        cons = review_text.find(class_="minus")
        if cons:
            for con in cons.find_all("li"):
                review.add_con(con.get_text())
        # set summary
        if review_text.p:
            review.set_summary(review_text.p.get_text())

        return review

    def parse_product_revs(self, review_list, product: Product, category_domain):
        """
        Parse product reviews from :param review list and save them to product.
        :param review_list: xml of page
        :param product: object that holds its reviews
        :return: True if the same review was found id DB, else False
        """

        try:
            reviews = review_list.find_all(class_="review")
            l = product.get_name().split("(")
            product_name = l[0].strip()

            # for each review
            for rev in reviews:
                review = self.parse_review(rev)

                review_elastic = self.connector.get_review_by_product_author_timestr(
                    category_domain, product_name, review.author, review.date)
                # review was found in elastic, which indicates that we have all reviews
                if review_elastic:
                    return True
                # else append review to product
                product.add_review(review)
        except Exception as e:
            print("[formate_text] Error: " + product.get_name() + " " + str(e))
            pass

        return False

    def get_urls(self, category, string_to_append=""):
        categories_urls = []
        urls = self.connector.get_category_urls(category)
        for url_dic in urls:
            try:
                url = url_dic['url'].split(".")[0] + ".heureka.cz/" + string_to_append
                if url not in categories_urls:
                    categories_urls.append(url)

            except Exception as e:
                print("[actualize_reviews] Error " + str(e))

        return categories_urls

    def add_to_elastic(self, product, category, product_new_count, review_new_count_new):
        def get_str_pos(l):
            s = []
            for sentence in l:
                s.append([str(wb) for wb in sentence])
            return s

        l = product.get_name().split("(")
        sub_cat_name = l[-1][:-1]
        product_name = l[0].strip()
        domain = self.connector.get_domain(category)

        if not self.connector.get_product_by_name(product_name):
            product_new_count += 1
            review_new_count_new += len(product.get_reviews())
            product_elastic = {
                "product_name": product_name,
                "category": sub_cat_name,
                "domain": domain,
                "category_list": product.get_cateogry(),
                "url": product.get_url()
            }

            # if not self.connector.index("product", product_elastic):
            #    print("Product of " + product_name + " " + " not created")

        # Save review elastic
        for rev in product.get_reviews():
            rev_dic = {'author': rev.author, 'rating': rev.rating, 'recommends': rev.recommends,
                       'pros': rev.pros, 'cons': rev.cons, 'summary': rev.summary, 'date_str': rev.date,
                       'category': sub_cat_name, 'product_name': product_name, 'domain': domain}
            datetime_object = datetime.strptime(rev_dic["date_str"], '%d. %B %Y')
            rev_dic["date"] = datetime_object.strftime('%Y-%m-%d')
            pro_pos = []
            cons_pos = []
            summary_pos = []

            for pos in rev_dic["pros"]:
                pro_pos.append(get_str_pos(self.tagger.pos_tagging(pos)))

            for con in rev_dic["cons"]:
                cons_pos.append(get_str_pos(self.tagger.pos_tagging(con)))

            summary_pos = get_str_pos(self.tagger.pos_tagging(rev_dic["summary"]))

            rev_dic["pro_POS"] = pro_pos
            rev_dic["cons_POS"] = cons_pos
            rev_dic["summary_POS"] = summary_pos
            # if not self.connector.index(domain, rev_dic):
            #    print("Review of " + product_name + " " + " not created")

    def actualize_reviews(self, obj_product_dict, category_domain, fast: bool):
        categories_urls = self.get_urls(category_domain, "top-recenze")

        for category_url in categories_urls:
            next_ref = " "

            while next_ref:
                try:
                    infile = BeautifulSoup(urlopen(category_url + next_ref), "lxml")
                except Exception as e:
                    print("[actualize_reviews] Error: " + str(e), file=sys.stderr)
                    break

                review_list = infile.find_all(class_="review")
                for rev in review_list:
                    try:
                        product = rev.find("h4")
                        # Create product
                        product_name_raw = product.get_text()
                        url = product.find("a").get('href')
                        category = url.split(".")[0].split("//")[1]
                        product_name = product_name_raw + " (" + category + ")"

                        url += "/recenze/"
                        product_obj = Product(url)
                        product_obj.set_name(product_name)
                        p = BeautifulSoup(urlopen(url), "lxml")

                        product_category = ""

                        # posledny je nazov produktu
                        for a in p.find(id="breadcrumbs").find_all("a")[:-1]:
                            product_category += a.get_text() + " | "
                        product_obj.set_cateogry(product_category[:-2])

                        review = self.parse_review(rev)
                        product_obj.add_review(review)

                        review_elastic = self.connector.get_review_by_product_author_timestr(category_domain, product_name_raw, review.author, review.date)
                        if review_elastic:
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
            break

    def task_seed_aspect_extraction(self, category: str, path: str):
        try:

            categories_urls = self.get_urls(category)
            with open(path + "/" + category + "_aspects.txt", "w") as aspect_file:
                for url in categories_urls:
                    try:
                        # print(url)
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
                                # xml = BeautifulSoup(non_script, "lxml")
                                xml_li = non_script.find_all("li")
                            val_list = []
                            for value in xml_li:
                                v = value.find("a").get_text()
                                occurence = 0
                                try:
                                    oc_str = value.find("span").get_text()
                                    oc_str = re.sub('[^0-9]', '', oc_str)
                                    occurence = int(oc_str)

                                except Exception as e:
                                    print(e)
                                    pass
                                if occurence > max_occurence:
                                    max_occurence = occurence
                                val_list.append((v, occurence))

                            # we want just 1% of max value occurence in out aspect list
                            delta = int(max_occurence * 0.01)
                            for val, occurence in val_list:
                                if occurence > delta:
                                    aspect.add_value(val)

                            aspect_cat.add_aspect(aspect)
                        aspect_file.write(str(aspect_cat) + "\n")
                        print(url + " " + str(len(aspect_cat.aspects)))

                    except Exception as e:
                        print("[task_seed_aspect_extraction] Error: " + str(e))

        except Exception as e:
            print("[task_seed_aspect_extraction] Error " + str(e), file=sys.stderr)

    def task_actualize(self, category: str, fast: bool):
        try:
            print("Category: " + str(category))
            actualized_dict_of_products = {}

            review_count = 0
            products_count = 0
            product_new_count = 0
            review_new_count_new = 0

            f_actualized = open(category+"_actualized.txt", "w")
            self.actualize_reviews(actualized_dict_of_products, category, fast)
            for _, product in actualized_dict_of_products.items():
                # Statistics
                products_count += 1
                review_count += len(product.get_reviews())
                # Save actualization file
                f_actualized.write(str(product) + "\n")

                # Save product elastic
                self.add_to_elastic(product, category, product_new_count, review_new_count_new)

            f_actualized.close()
            print(category + " has: " + str(review_count) + " reviews, affected products: " + str(
                products_count) + ", new products: " + str(product_new_count) + ", new product`s reviews: " + str(review_new_count_new))

        except Exception as e:
            print("[actualize] " + str(e), file=sys.stderr)

    def task(self, category: str, args):
        # product list
        product_reviews = []
        log_f = open(args['path'] + "/" + category, "w")

        # statistics
        review_count = 0
        products_count = 0
        try:
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
                        # no product name no reviews for that product
                        else:
                            continue

                        # Handle collision
                        if product.get_name() in product_reviews:
                            continue

                        product_category = ""

                        # category list -> category_domain | sub_cat1 | subcat2...
                        for a in infile.find(id="breadcrumbs").find_all("a")[:-1]:
                            product_category += a.get_text() + " | "
                        product.set_cateogry(product_category[:-2])

                        # get reviews
                        self.parse_product_page(infile, product, category)

                        # write some logs
                        log_f.write(product.get_name() + "\t\t" + str(len(product.reviews)) + '\n')

                        # add product to parsed products
                        product_reviews.append(product.get_name())

                        # add to elastic
                        self.add_to_elastic(product, category, products_count, review_count)

                    except IOError:
                        print("[task] Error: Cant open URL: ", url, file=sys.stderr)
                    except Exception as e:
                        print("[task] Error " + str(e))

        except Exception as e:
            print("[Task] " + e, file=sys.stderr)

        finally:
            log_f.close()


def main():
    parser = argparse.ArgumentParser(
        description="Crawl Heureka reviews as defined in config.py, Expects existance of URLS file for every category")
    parser.add_argument("-actualize", "--actualize", action="store_true", help="Actualize reviews")
    # parser.add_argument("-fast", action="store_true", help="Actualize reviews, when review exists breaks searching for category")
    parser.add_argument("-aspect", "--aspect", action="store_true", help="Get aspects from category specification")
    parser.add_argument("-crawl", "--crawl",  action="store_true", help="Crawl heureka reviews with url dataset")
    parser.add_argument("-path", "-path",  help="Path to the dataset folder")

    args = vars(parser.parse_args())
    # create tagger
    tagger = MorphoTagger()
    tagger.load_tagger("external/morphodita/czech-morfflex-pdt-161115-no_dia-pos_only.tagger")

    # Elastic
    con = Connector()
    # Crawler
    crawler = HeurekaCrawler(con, tagger)

    for category in crawler.categories:
        start = time.time()

        if args['actualize']:
            # actualize reviews
            # always fast for now
            crawler.task_actualize(category, True)  # args.fast)
        elif args['aspect']:
            # aspect extraction
            crawler.task_seed_aspect_extraction(category, args['path'])
        elif args['crawl']:
            # product reviews extraction
            crawler.task(category, args)

        print(time.time() - start)

    if args['actualize']:
        # zapis datumu aktualizace
        with open("actualization_dates", "a") as act_dates:
            act_dates.write(date.today().strftime("%d. %B %Y").lstrip("0") + "\n")


if __name__ == '__main__':
    start = time.time()
    main()
    print(time.time() - start)
