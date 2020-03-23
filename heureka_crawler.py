import time, sys, argparse, re
from datetime import datetime
from urllib.request import urlopen
from bs4 import BeautifulSoup
from datetime import date
from utils.elastic_connector import Connector
from utils.discussion import Review, Product, Aspect, AspectCategory
from utils.morpho_tagger import MorphoTagger
from heureka_filter import HeurekaFilter


class HeurekaCrawler:
    def __init__(self, connector, tagger, filter_model):
        self.categories = [
            'Elektronika',
            'Bile zbozi',
            'Dum a zahrada',
            'Chovatelstvi',
            'Auto-moto',
            'Detske zbozi',
            'Obleceni a moda',
            'Filmy knihy hry',
            'Kosmetika a zdravi',
            'Sport',
            'Hobby',
            'Jidlo a napoje',
            'Stavebniny',
            'Sexualni a eroticke pomucky'
        ]
        self.connector = connector
        self.tagger = tagger
        self.bert_filter = filter_model

        self.total_review_count = 0
        self.total_products_count = 0
        self.total_product_new_count = 0
        self.total_review_new_count_new = 0
        self.total_empty_reviews = 0
        self.irrelevant_sentences_count = 0

        pass

    def parse_product_page(self, infile, product: Product, category_domain):
        '''
        Parse review page of product, build up product object by appending new reviews.

        :param infile:
        :param product:
        :param category_domain:
        :return:
        '''

        def _task_product_page(product_page_xml):
            review_list = product_page_xml.find(class_="product-review-list")
            # if True then we parsed all product reviews, else we need to go on
            if self.parse_product_revs(review_list, product, category_domain):
                return

            # find section with new page
            references = review_list.find(class_="pag bot").find("a", "next").references.get('href')
            if references:
                return references.get('href')
            else:
                return None

        try:
            ref = _task_product_page(infile)

            # loop over footer with references to next pages
            while ref:
                try:
                    infile = BeautifulSoup(urlopen(product.get_url() + ref), "lxml")
                except IOError:
                    print("[parse_product_page] Cant open " + product.get_url() + ref, file=sys.stderr)

                ref = _task_product_page(infile)

        except Exception as e:
            print("[parse_product_page] Exception: " + product.get_url())

    def parse_review(self, rev) -> Review:
        """
        Create review object from xml representation
        :param rev:
        :return:
        """

        def _pro_cons(xml):
            l = []
            if xml:
                for li in xml.find_all("li"):
                    text = li.get_text()
                    if self.bert_filter.is_irrelevant(text):
                        self.irrelevant_sentences_count += 1
                        continue
                    review.add_pro(text)
            return l

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
        review.set_pros(_pro_cons(review_text.find(class_="plus")))
        # set cons
        review.set_cons(_pro_cons(review_text.find(class_="minus")))
        # set summary
        if review_text.p:
            text = review_text.p.get_text()
            if not self.bert_filter.is_irrelevant(text):
                review.set_summary(text)
            else:
                self.irrelevant_sentences_count += 1

        return review

    def parse_product_revs(self, review_list, product: Product, category_domain):
        """
        Parse product reviews from :param review list and save them to product.
        :param category_domain: domain of product category
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
            print("[parse_product_revs] Error: " + product.get_name() + " " + str(e))
            pass

        return False

    def parse_shop_revs(self, review_list, shop_name):
        """
        Parse shop reviews from :param review_list
        :param review_list: xml of page
        :param shop_name:
        :return:
        """

        def _get_str_pos(l):
            s = []
            for sentence in l:
                s.append([str(wb) for wb in sentence])
            return s

        def _cons_pros(xml):
            l_ = []
            l_pos = []
            if xml:
                for pro in xml.find_all('li'):
                    val = pro.get_text().strip()
                    if self.bert_filter.is_irrelevant(val):
                        self.irrelevant_sentences_count+=1
                        continue
                    l_.append(val)
                    l_pos.append(_get_str_pos(self.tagger.pos_tagging(val)))
            return l_, l_pos

        def _summary(xml):
            summary_text = xml.get_text().strip() if xml else ''
            if summary_text and self.bert_filter.is_irrelevant(summary_text):
                return [], []
            return summary_text, _get_str_pos(self.tagger.pos_tagging(summary_text))

        r_d = {}
        for review in review_list:
            try:
                sum_xml = review.find(class_='c-post__summary')
                author_xml = review.find(class_='c-post__author')
                rating_xml = review.find(class_='c-rating-widget__value')
                pros, pros_pos = _cons_pros(review.find(class_='c-attributes-list--pros'))
                cons, cons_pos = _cons_pros(review.find(class_='c-attributes-list--cons'))
                summary, summary_pos = _summary(sum_xml)
                delivery_time_xml = review.find(class_='c-post__delivery-time')
                date_obj = datetime.strptime(review.find(class_='c-post__publish-time').get('datetime'),
                                             '%Y-%m-%d %H:%M:%S')
                r_d = {
                    'author': author_xml.get_text() if author_xml else 'author',
                    'date': date_obj.isoformat(),
                    'date_str': date_obj.strftime('%d. %B %Y'),
                    'recommends': 'YES' if review.find(class_='u-color-success') else 'NO',
                    'delivery_time': delivery_time_xml.get_text() if delivery_time_xml else str(0),
                    'rating': rating_xml.get_text().split()[0] + '%' if rating_xml else '',
                    'summary': summary, 'summary_pos': summary_pos,
                    'pros': pros, 'pros_pos': pros_pos,
                    'cons': cons, 'cons_pos': cons_pos,
                    'domain': 'shop_review',
                    'shop_name': shop_name,
                    'aspect': [],
                }
                # check if review is not empty
                if not r_d['pros'] and not r_d['cons'] and not r_d['summary']:
                    self.total_empty_reviews += 1
                    continue

                if not self.connector.get_review_by_shop_author_timestr(
                        r_d['shop_name'], r_d['author'], r_d['date']):
                    self.total_review_count += 1
                    if not self.connector.index("shop_review", r_d):
                        print("Review of " + shop_name + " " + " not created", file=sys.stderr)
                else:
                    return True

            except Exception as e:
                print("[parse_shop_revs] Error: " + shop_name + " " + str(r_d) + " " + str(e), file=sys.stderr)
                pass

        return False


    def parse_shop_page(self, shop_list):
        """
        Parse page with shop links
        :param shop_list: xml
        :return:
        """

        def _task_shop_parse(shop_url, shop_name):
            shop_xml = BeautifulSoup(urlopen(shop_url), "lxml")
            review_list = shop_xml.find_all(class_='c-post')
            # found existing review signal to end crawl
            if self.parse_shop_revs(review_list, shop_name):
                return None

            # find section with new page
            references = shop_xml.find(class_='c-pagination').find(class_='c-pagination__button')

            return references.get('href') if references else None

        for shop in shop_list:
            try:
                reviews = self.total_review_count
                shop_url = 'https://obchody.heureka.cz' + shop.find(class_='c-shops-table__cell--rating').find('a').get(
                    'href')
                shop_name = shop.find(class_='c-shops-table__cell--name').find('a').get_text().strip()
                shop_exit_url_xml = shop.find(class_='c-shops-table__cell--name').find('a')
                shop_exit_url = shop_exit_url_xml.get('href') if shop_exit_url_xml else ''
                shop_info_xml = shop.find(class_='c-shops-table__cell--info').find('p')
                shop_info = shop_info_xml.get_text() if shop_info_xml else ''
                shop_ref = None

                if not self.connector.get_shop_by_name(shop_name):
                    shop_d = {
                        'name': shop_name,
                        'url_review': shop_url,
                        'url_shop': shop_exit_url,
                        'info': shop_info,
                        'domain': 'shop',
                    }
                    self.total_products_count += 1
                    if not self.connector.index("shop", shop_d):
                        print("Shop of " + shop_name + " " + " not created")
                else:
                    print('Shop in db already ' + str(shop_name), file=sys.stderr)

                shop_ref = _task_shop_parse(shop_url, shop_name)

                # loop over footer with references to next pages
                while shop_ref:
                    shop_ref = _task_shop_parse('https://obchody.heureka.cz' + shop_ref, shop_name)

                print(shop_name + ' ' + str(self.total_review_count - reviews))
            except Exception as e:
                print("[parse_shop_page] Error: " + shop_name + " " + str(e), file=sys.stderr)
                pass

    def get_urls(self, category, string_to_append=""):
        """
        Get all product subcategories from elastic under :param category
        :param category:
        :param string_to_append:
        :return:
        """
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

    def add_to_elastic(self, product, category):
        """
        Index product, category object to elastic.
        :param product:
        :param category:
        :return:
        """

        def get_str_pos(l):
            s = []
            for sentence in l:
                s.append([str(wb) for wb in sentence])
            return s

        product_new_count = 0
        review_new_count_new = 0
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

            if not self.connector.index("product", product_elastic):
                print("Product of " + product_name + " " + " not created")

        # Save review elastic
        for rev in product.get_reviews():
            rev_dic = {'author': rev.author, 'rating': rev.rating, 'recommends': rev.recommends,
                       'pros': rev.pros, 'cons': rev.cons, 'summary': rev.summary, 'date_str': rev.date,
                       'category': sub_cat_name, 'product_name': product_name, 'domain': domain}
            datetime_object = datetime.strptime(rev_dic["date_str"], '%d. %B %Y')
            rev_dic["date"] = datetime_object.strftime('%Y-%m-%d')
            pro_pos = []
            cons_pos = []

            for pos in rev_dic["pros"]:
                pro_pos.append(get_str_pos(self.tagger.pos_tagging(pos)))

            for con in rev_dic["cons"]:
                cons_pos.append(get_str_pos(self.tagger.pos_tagging(con)))

            summary_pos = get_str_pos(self.tagger.pos_tagging(rev_dic["summary"]))

            rev_dic["pro_POS"] = pro_pos
            rev_dic["cons_POS"] = cons_pos
            rev_dic["summary_POS"] = summary_pos
            if not self.connector.index(domain, rev_dic):
                print("Review of " + product_name + " " + " not created")

        return product_new_count, review_new_count_new

    def actualize_reviews(self, obj_product_dict, category_domain, fast: bool):
        """
        Method actualize every subcategory of main category domain
        :param obj_product_dict: actualized objects of products
        :param category_domain:
        :param fast:
        :return:
        """
        categories_urls = self.get_urls(category_domain, "top-recenze/")

        for category_url in categories_urls:
            next_ref = " "
            while next_ref:
                try:
                    infile = BeautifulSoup(urlopen(category_url + next_ref), "lxml")
                except Exception as e:
                    print("[actualize_reviews] Error: " + str(e), file=sys.stderr)
                    print(category_url, file=sys.stderr)
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

                        for a in p.find(id="breadcrumbs").find_all("a")[:-1]:
                            product_category += a.get_text() + " | "
                        product_obj.set_cateogry(product_category[:-2])

                        review = self.parse_review(rev)
                        product_obj.add_review(review)

                        if self.connector.get_review_by_product_author_timestr(
                                category_domain, product_name_raw, review.author, review.date):
                            if fast:
                                next_ref = None
                                break
                            continue

                        if product_name in obj_product_dict:
                            obj_product_dict[product_name].add_review(review)
                        else:
                            obj_product_dict[product_name] = product_obj

                    except Exception as e:
                        print("[actualize_reviews] Error: " + str(e), file=sys.stderr)
                        print(category_url, file=sys.stderr)
                        pass

                if next_ref is None:
                    break

                references = infile.find(class_="pag bot").find("a", "next")

                if references:
                    next_ref = references.get('href')
                else:
                    next_ref = None
                    break

    def task_seed_aspect_extraction(self, category: str, path: str):
        """
        Task extracts aspects from heureka category panel and save them to the dir.
        :param category:
        :param path:
        :return:
        """
        try:

            categories_urls = self.get_urls(category)
            with open(path + "/" + category + "_aspects.txt", "w") as aspect_file:
                for url in categories_urls:
                    try:
                        # print(url)
                        page = BeautifulSoup(urlopen(url), "lxml")
                        breadcrumbs = ""
                        max_occurence = 0

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
        """
        Task actualizes product reviews from main domain category.
        :param category: domain category
        :param fast:
        :return:
        """
        try:
            actualized_dict_of_products = {}

            review_count = 0
            products_count = 0
            product_new_count = 0
            review_new_count_new = 0

            self.actualize_reviews(actualized_dict_of_products, category, fast)
            for _, product in actualized_dict_of_products.items():
                # Statistics
                products_count += 1
                review_count += len(product.get_reviews())

                # Save product elastic
                p_n_c, r_n_c_n = self.add_to_elastic(product, category)
                product_new_count += p_n_c
                review_new_count_new += r_n_c_n

            category = category.replace(',', '')
            print(category + "," + str(review_count) + "," +
                  str(products_count) + "," +
                  str(product_new_count) + "," +
                  str(review_new_count_new) + "," +
                  date.today().strftime("%d. %B %Y").lstrip("0"))

            self.total_review_count += review_count
            self.total_products_count += products_count
            self.total_review_new_count_new += review_new_count_new
            self.total_product_new_count += product_new_count

        except Exception as e:
            print("[actualize] " + str(e), file=sys.stderr)

    def task_shop(self, args):
        """
        Task crawl shop reviews
        :param args:
        :return:
        """
        d = {
            'Elektronika': 'https://obchody.heureka.cz/elektronika/',
            #'Bile zbozi': 'https://obchody.heureka.cz/bile-zbozi/',
            #'Dum a zahrada': 'https://obchody.heureka.cz/dum-zahrada/',
            #'Auto-moto': 'https://obchody.heureka.cz/auto-moto/',
            #'Detske zbozi': 'https://obchody.heureka.cz/detske-zbozi/',
            #'Obleceni a moda': 'https://obchody.heureka.cz/moda/',
            #'Filmy knihy hry': 'https://obchody.heureka.cz/filmy-hudba-knihy/',
            #'Kosmetika a zdravi': 'https://obchody.heureka.cz/kosmetika-zdravi/',
            #'Sport': 'https://obchody.heureka.cz/sport/',
            #'Hobby': 'https://obchody.heureka.cz/hobby/',
            #'Jidlo a napoje': 'https://obchody.heureka.cz/jidlo-a-napoje/',
            #'Stavebniny': 'https://obchody.heureka.cz/stavebniny/',
            #'Sexualni a eroticke pomucky': 'https://obchody.heureka.cz/sex-erotika/'
        }

        def _task_shop_page(url):
            shop_page = BeautifulSoup(urlopen(url), "lxml")

            shop_list = shop_page.find_all(class_="c-shops-table__row")
            self.parse_shop_page(shop_list)

            # find section with new page
            references = shop_page.find(class_='c-pagination').find(class_='c-pagination__button')

            return references.get('href') if references else None

        for category, url in d.items():
            try:
                page_ref = _task_shop_page(url)

                while page_ref:
                    page_ref = _task_shop_page('https://obchody.heureka.cz' + page_ref)

            except Exception as e:
                print("[task_shop] Exception for " + url + " " + str(e), file=sys.stderr)

    def task(self, category: str):
        """
        Tast for product reviews crawling
        :param category:
        :return:
        """
        # product list
        product_reviews = []

        # statistics
        review_count = 0
        products_count = 0
        product_new_count = 0
        review_new_count_new = 0
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

                        # add product to parsed products
                        product_reviews.append(product.get_name())

                        # add to elastic
                        p_n_c, r_n_c_n = self.add_to_elastic(product, category)
                        product_new_count += p_n_c
                        review_new_count_new += r_n_c_n
                        self.total_review_count += review_count
                        self.total_products_count += products_count
                        self.total_review_new_count_new += review_new_count_new
                        self.total_product_new_count += product_new_count

                    except IOError:
                        print("[task] Error: Cant open URL: ", url, file=sys.stderr)
                    except Exception as e:
                        print("[task] Error " + str(e))

        except Exception as e:
            print("[Task] " + e, file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description="Crawl Heureka reviews as defined in config.py, Expects existance of URLS file for every category")
    parser.add_argument("-actualize", "--actualize", action="store_true", help="Actualize reviews")
    parser.add_argument("-aspect", "--aspect", action="store_true", help="Get aspects from category specification")
    parser.add_argument("-crawl", "--crawl", action="store_true", help="Crawl heureka reviews with url dataset")
    parser.add_argument("-path", "-path", help="Path to the dataset folder")
    parser.add_argument("-shop", "-shop", help="Crawl shop reviews", action="store_true")
    parser.add_argument("-filter", "-filter", help="Use model to filter irrelevant sentences", action="store_true")

    args = vars(parser.parse_args())
    # create tagger
    tagger = MorphoTagger()
    tagger.load_tagger("external/morphodita/czech-morfflex-pdt-161115-no_dia-pos_only.tagger")

    # Elastic
    con = Connector()

    use_model = True if args['filter'] else False
    # Bert filter model
    heureka_filter = HeurekaFilter(use_model)

    # Crawler
    crawler = HeurekaCrawler(con, tagger, heureka_filter)

    for category in crawler.categories:
        start = time.time()

        if args['actualize']:
            # actualize reviews
            # always fast for now
            crawler.task_actualize(category, True)

        elif args['aspect']:
            # aspect extraction
            crawler.task_seed_aspect_extraction(category, args['path'])
        elif args['crawl']:
            # product reviews extraction
            crawler.task(category, args)
        else:
            break

    if args['shop']:
        # crawl shop reviews
        crawler.task_shop(args)

    print(time.time() - start)

    if args['actualize'] or args['shop']:
        # Logs
        print("Total," + str(crawler.total_review_count) + "," +
              str(crawler.total_products_count) + "," +
              str(crawler.total_product_new_count) + "," +
              str(crawler.total_review_new_count_new) + "," +
              date.today().strftime("%d. %B %Y").lstrip("0"))
        print('Empty revs : '+ str(crawler.total_empty_reviews))


if __name__ == '__main__':
    start = time.time()
    main()
    print(time.time() - start)
