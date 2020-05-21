"""
This file contains implementation of the HeurekaCrawler class. Class works deeply with elasticsearch, to which it indexes
reviews. Class implements heureka reviews crawl from txts of crawled products, actualization of reviews by crawling all
new reviews from subcategories, shop reviews crawling and repair of products reviews with minimal review count. Class
can use models for irrelevant review filtering (HeurekaFilter) and text rating model (HeurekaRating).

Author: xkloco00@stud.fit.vutbr.cz
"""

import time, sys, argparse, re
from datetime import datetime
from urllib.request import urlopen
from bs4 import BeautifulSoup
from datetime import date
from utils.elastic_connector import Connector
from utils.discussion import Review, Product, Aspect, AspectCategory
from utils.morpho_tagger import MorphoTagger
from heureka_models.heureka_filter import HeurekaFilter
from heureka_models.heureka_rating import HeurekaRating


class HeurekaCrawler:
    """
    Class implements crawler, which crawls heureka with given list of starter URLS, handles indexing of products, shops
    and reviews. Class is strongly connected to elastic search client to which it indexes new reviews.
    """
    def __init__(self, connector: Connector, tagger: MorphoTagger, filter_model: HeurekaFilter,
                 rating_model: HeurekaRating):
        """
        Constructor initializes domain categories with all available models for classification and pos tagging, sets
        statistics counter.
        :param connector: Elastic search connector with API methods
        :param tagger: POS tagging model
        :param filter_model: SVM filtering model
        :param rating_model: Bert regression model
        """
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
        self.filter_model = filter_model
        self.rating_model = rating_model

        self.total_review_count = 0
        self.total_products_count = 0
        self.total_product_new_count = 0
        self.total_review_new_count = 0
        self.total_empty_reviews = 0
        self.irrelevant_sentences_count = 0
        self.sentences_count = 0

    def parse_product_page(self, infile, product: Product, category_domain):
        """
        Parse review page of product, build up product object by appending new reviews.

        :param infile:
        :param product:
        :param category_domain:
        :return:
        """

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
                    self.sentences_count += 1
                    if self.filter_model.is_irrelevant(text):
                        self.irrelevant_sentences_count += 1
                        continue
                    l.append(text)
            return l

        review = Review()
        # author name
        try:
            author = rev.find("strong").get_text().strip()
            review.set_author(author)
        except Exception as e:
            review.set_author('Anonym')

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
            self.sentences_count += 1
            if not self.filter_model.is_irrelevant(text):
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

                # check if review is not empty
                if not review.pros and not review.cons and not review.summary:
                    self.total_empty_reviews += 1
                    continue

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

    def parse_shop_revs(self, review_list, shop_name: str):
        """
        Parse shop reviews from review_list
        :param review_list: xml of page
        :param shop_name: string
        :return:
        """

        def _get_str_pos(l: list):
            """
            Inner function for dumping WordPos object to dictionary as list
            :param l: list of WordPos objects
            :return: list of strings
            """
            s = []
            for sentence in l:
                s.append([str(wb) for wb in sentence])
            return s

        def _cons_pros(xml):
            """
            Parse xml for positive/negative section of ul elements
            :param xml: beautiful soup instance
            :return: list of sentences, list of processed sentences: Tuple[list, List[List[List[str]]]]
            """
            l_ = []
            l_pos = []
            if xml:
                # loop over all list elements
                for pro in xml.find_all('li'):
                    # increase count of sentences and evaluate sentence with irrelevant model
                    val = pro.get_text().strip()
                    self.sentences_count += 1
                    if self.filter_model.is_irrelevant(val):
                        self.irrelevant_sentences_count += 1
                        continue
                    l_.append(val)
                    l_pos.append(_get_str_pos(self.tagger.pos_tagging(val)))
            return l_, l_pos

        def _summary(xml):
            """
            Parse bs4 instance of summary and evaluate it with irrelevant model
            :param xml:
            :return: sentence, list of processed sentences
            """
            summary_text = xml.get_text().strip() if xml else ''
            if summary_text and self.filter_model.is_irrelevant(summary_text):
                return '', []
            return summary_text, _get_str_pos(self.tagger.pos_tagging(summary_text))

        r_d = {}
        # parse each review in review list
        for review in review_list:
            try:
                # parse xmls
                sum_xml = review.find(class_='c-post__summary')
                author_xml = review.find(class_='c-post__author')
                rating_xml = review.find(class_='c-rating-widget__value')
                pros, pros_pos = _cons_pros(review.find(class_='c-attributes-list--pros'))
                cons, cons_pos = _cons_pros(review.find(class_='c-attributes-list--cons'))
                summary, summary_pos = _summary(sum_xml)
                delivery_time_xml = review.find(class_='c-post__delivery-time')
                date_obj = datetime.strptime(review.find(class_='c-post__publish-time').get('datetime'),
                                             '%Y-%m-%d %H:%M:%S')
                # cerate review dict (json)
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
                review_text = self.rating_model.merge_review_text(
                    r_d['pros'], r_d['cons'], r_d['summary'])

                # perform evaluation with models
                if review_text:
                    rating_model = self.rating_model.eval_sentence(review_text)
                    if rating_model:
                        r_d['rating_model'] = rating_model

                if self.filter_model.model:
                    r_d['filter_model'] = True
                else:
                    r_d['filter_model'] = False

                # check if review is not empty
                if not r_d['pros'] and not r_d['cons'] and not r_d['summary']:
                    self.total_empty_reviews += 1
                    continue
                # if the review already exists return true else index it
                if not self.connector.get_review_by_shop_author_timestr(
                        r_d['shop_name'], r_d['author'], r_d['date']):
                    self.total_review_count += 1
                    if not self.connector.index("shop_review", r_d):
                        print("Review of " + shop_name + " " + " not created", file=sys.stderr)
                else:
                    return True

            except Exception as e:
                print("[parse_shop_revs] Error: " + shop_name + " " + str(r_d) + " " + str(e), file=sys.stderr)

        return False

    def parse_shop_page(self, shop_list):
        """
        Parse page with shop links
        :param shop_list: xml
        :return:
        """

        def _task_shop_parse(shop_url: str, shop_name: str):
            """
            Function performs parsing shop reviews on heureka.
            :param shop_url:
            :param shop_name:
            :return:
            """
            shop_xml = BeautifulSoup(urlopen(shop_url), "lxml")
            review_list = shop_xml.find_all(class_='c-post')
            # found existing review signal to end crawl
            if self.parse_shop_revs(review_list, shop_name):
                return None

            # find section with new page
            references = shop_xml.find(class_='c-pagination').find(class_='c-pagination__button')

            return references.get('href') if references else None
        # loop over all shop instances
        for shop in shop_list:
            try:
                reviews = self.total_review_count
                # parse shop meta data files
                shop_url = 'https://obchody.heureka.cz' + shop.find(class_='c-shops-table__cell--rating').find('a').get(
                    'href')
                shop_name = shop.find(class_='c-shops-table__cell--name').find('a').get_text().strip()
                shop_exit_url_xml = shop.find(class_='c-shops-table__cell--name').find('a')
                shop_exit_url = shop_exit_url_xml.get('href') if shop_exit_url_xml else ''
                shop_info_xml = shop.find(class_='c-shops-table__cell--info').find('p')
                shop_info = shop_info_xml.get_text() if shop_info_xml else ''
                shop_ref = None
                # index shop if it is not already in elastic
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

                # parse shop reviews
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
        Get all product subcategories from elastic under category
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

        def get_str_pos(l: list):
            """
            Inner function for dumping WordPos object to dictionary as list
            :param l: list of WordPos objects
            :return: list of strings
            """
            s = []
            for sentence in l:
                s.append([str(wb) for wb in sentence])
            return s

        review_count = 0
        product_new_count = 0
        review_new_count_new = 0
        l = product.get_name().split("(")
        sub_cat_name = l[-1][:-1]
        product_name = l[0].strip()
        domain = self.connector.get_domain(category)

        # index product if it is no already in elastic
        if not self.connector.get_product_by_name(product_name):
            product_elastic = {
                "product_name": product_name,
                "category": sub_cat_name,
                "domain": domain,
                "category_list": product.get_category(),
                "url": product.get_url()
            }

            if not self.connector.index("product", product_elastic):
                print("Product of " + product_name + " " + " not created", sys.stderr)
            else:
                # increase statistics
                product_new_count += 1
                review_new_count_new += len(product.get_reviews())

        # loop over product reviews
        for rev in product.get_reviews():
            try:
                # create review representation as dictionary
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
                review_text = self.rating_model.merge_review_text(
                    rev_dic['pros'], rev_dic['cons'], rev_dic['summary'])
                # evaluate text with given models
                if review_text:
                    # rating model
                    rating_model = self.rating_model.eval_sentence(review_text)
                    if rating_model:
                        rev_dic['rating_model'] = rating_model
                # filter model
                if self.filter_model.model:
                    rev_dic['filter_model'] = True
                else:
                    rev_dic['filter_model'] = False

                if not self.connector.index(domain, rev_dic):
                    print("Review of " + product_name + " " + " not created", sys.stderr)
                else:
                    review_count += 1
            except Exception as e:
                print("[add_to_elastic] Error: " + str(e) + ' ' + str(rev_dic), file=sys.stderr)

        return product_new_count, review_new_count_new, review_count

    def actualize_reviews(self, obj_product_dict, category_domain, fast: bool):
        """
        Method actualize every subcategory of main category domain
        :param obj_product_dict: actualized objects of products
        :param category_domain:
        :param fast:
        :return:
        """
        categories_urls = self.get_urls(category_domain, "top-recenze/")
        # loop over product urls
        for category_url in categories_urls:
            try:
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
                            product_obj.set_category(product_category[:-2])

                            # parse review
                            review = self.parse_review(rev)

                            # check if review is not empty
                            if not review.pros and not review.cons and not review.summary:
                                self.total_empty_reviews += 1
                                continue

                            product_obj.add_review(review)
                            # if there is a review with the same date, author and product
                            if self.connector.get_review_by_product_author_timestr(
                                    category_domain, product_name_raw, review.author, review.date):
                                # fast method does not count all reviews, so after first match it ends
                                if fast:
                                    next_ref = None
                                    break
                                continue
                            # append product to dictionary of actualized products
                            if product_name in obj_product_dict:
                                obj_product_dict[product_name].add_review(review)
                            else:
                                obj_product_dict[product_name] = product_obj

                        except Exception as e:
                            print("[actualize_reviews] Error: " + str(e), file=sys.stderr)
                            print(category_url, file=sys.stderr)
                            pass
                    # go to the next page
                    if next_ref is None:
                        break

                    references = infile.find(class_="pag bot").find("a", "next")

                    if references:
                        next_ref = references.get('href')
                    else:
                        next_ref = None
                        break
            except Exception as e:
                print("[actualize_reviews] Error references: " + str(e), file=sys.stderr)
                print(category_url, file=sys.stderr)

    def task_seed_aspect_extraction(self, category: str, path: str):
        """
        Task extracts aspects from heureka category panel and save them to the dir.
        :param category:
        :param path:
        :return:
        """
        try:

            categories_urls = self.get_urls(category)
            # get hereka parameters of product categories, that might resemblance aspects
            # and save them to file
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
            # dict of actualized products
            actualized_dict_of_products = {}
            # statistics
            review_count = 0
            products_count = 0
            product_new_count = 0
            review_new_count_new = 0
            # actualize products reviews for concrete domain category
            self.actualize_reviews(actualized_dict_of_products, category, fast)
            # loop over actualized products, get statistics and save reviews with products to elastic
            for _, product in actualized_dict_of_products.items():
                try:
                    # Statistics
                    products_count += 1

                    # Save product elastic and get statistics
                    p_n_c, r_n_c_n, rev_cnt = self.add_to_elastic(product, category)
                    review_count += rev_cnt
                    product_new_count += p_n_c
                    review_new_count_new += r_n_c_n
                except Exception as e:
                    print("[actualize-statistics] " + str(e), file=sys.stderr)

            self.submit_statistic(category, review_count, products_count,
                                  product_new_count, review_new_count_new)

            self.total_review_count += review_count
            self.total_products_count += products_count
            self.total_review_new_count += review_new_count_new
            self.total_product_new_count += product_new_count

        except Exception as e:
            print("[actualize] " + str(e), file=sys.stderr)

    def task_shop(self):
        """
        Task crawl shop reviews.
        :return:
        """
        # dictionary of domain to shop-url
        d = {
            'Elektronika': 'https://obchody.heureka.cz/elektronika/',
            'Bile zbozi': 'https://obchody.heureka.cz/bile-zbozi/',
            'Dum a zahrada': 'https://obchody.heureka.cz/dum-zahrada/',
            'Auto-moto': 'https://obchody.heureka.cz/auto-moto/',
            'Detske zbozi': 'https://obchody.heureka.cz/detske-zbozi/',
            'Obleceni a moda': 'https://obchody.heureka.cz/moda/',
            'Filmy knihy hry': 'https://obchody.heureka.cz/filmy-hudba-knihy/',
            'Kosmetika a zdravi': 'https://obchody.heureka.cz/kosmetika-zdravi/',
            'Sport': 'https://obchody.heureka.cz/sport/',
            'Hobby': 'https://obchody.heureka.cz/hobby/',
            'Jidlo a napoje': 'https://obchody.heureka.cz/jidlo-a-napoje/',
            'Stavebniny': 'https://obchody.heureka.cz/stavebniny/',
            'Sexualni a eroticke pomucky': 'https://obchody.heureka.cz/sex-erotika/'
        }

        def _task_shop_page(url: str):
            """
            Function parse domain category for shop instances with its reviews
            :param url:
            :return:
            """
            shop_page = BeautifulSoup(urlopen(url), "lxml")

            shop_list = shop_page.find_all(class_="c-shops-table__row")
            self.parse_shop_page(shop_list)

            # find section with new page
            references = shop_page.find(class_='c-pagination').find(class_='c-pagination__button')

            return references.get('href') if references else None

        # loop over urls
        for category, url in d.items():
            try:
                page_ref = _task_shop_page(url)

                while page_ref:
                    page_ref = _task_shop_page('https://obchody.heureka.cz' + page_ref)

            except Exception as e:
                print("[task_shop] Exception for " + url + " " + str(e), file=sys.stderr)

    def task_repair(self, min_rec_count: int):
        """
        Crawl products, that have less then min_rec_count count of reviews
        :param min_rec_count:
        :return:
        """
        def get_str_pos(l: list):
            """
            Inner function for dumping WordPos object to dictionary as list
            :param l: list of WordPos objects
            :return: list of strings
            """
            s = []
            for sentence in l:
                s.append([str(wb) for wb in sentence])
            return s

        repaired_products = 0
        repaired_reviews = 0
        # get all products from elastic
        products = self.connector.match_all('product')

        progress = 0
        products_len = len(products)
        # loop over products
        for product in products:
            # progress
            progress += 1
            if progress % 10000 == 0:
                print('{} products of {}'.format(str(progress), str(products_len)))

            # get count of reviews for given product
            revs = self.connector.get_product_rev_cnt(product['product_name'])
            # if the count is less then minimum crawl all reviews of that product
            if revs < min_rec_count:
                review_cnt = 0
                next_ref = " "
                while next_ref:
                    try:
                        infile = BeautifulSoup(urlopen(product['url'] + next_ref), "lxml")
                    except Exception as e:
                        print("[task_repair] Error: " + str(e), file=sys.stderr)
                        print(product['url'], file=sys.stderr)
                        break

                    review_list = infile.find_all(class_="review")
                    # no reviews
                    if not review_list:
                        break
                    # loop over all product reviews
                    for rev in review_list:
                        try:
                            review = self.parse_review(rev)
                            # if the review already exists continue
                            if self.connector.get_review_by_product_author_timestr(
                                    product['domain'], product['product_name'], review.author, review.date):
                                continue
                            # review dictionary
                            rev_dic = {'author': review.author, 'rating': review.rating,
                                       'recommends': review.recommends, 'pros': review.pros, 'cons': review.cons,
                                       'summary': review.summary, 'date_str': review.date,
                                       'category': product['category'], 'product_name': product['product_name'],
                                       'domain': product['domain']}

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
                            # evaluate review text with available models
                            review_text = self.rating_model.merge_review_text(
                                rev_dic['pros'], rev_dic['cons'], rev_dic['summary'])
                            # rating model
                            rating_model = self.rating_model.eval_sentence(review_text)
                            if rating_model:
                                rev_dic['rating_model'] = rating_model
                            # filter model
                            if self.filter_model.model:
                                rev_dic['filter_model'] = True
                            else:
                                rev_dic['filter_model'] = False
                            # index review to elastic
                            if not self.connector.index(product['domain'], rev_dic):
                                print("Review of " + product['product_name'] + " " + " not created", sys.stderr)
                            else:
                                repaired_reviews += 1
                                review_cnt += 1

                        except Exception as e:
                            print("[task_repair] Error: " + str(e), file=sys.stderr)
                            print(product['product_name'], file=sys.stderr)

                    # find next page reference or break
                    if next_ref is None:
                        break
                    try:
                        references = infile.find(class_="pag bot").find("a", "next")

                        if references:
                            next_ref = references.get('href')
                        else:
                            next_ref = None
                            break
                    except Exception as e:
                        print("[task_repair] references Error: " + str(e), file=sys.stderr)
                        print(product['product_name'], file=sys.stderr)
                        next_ref = None
                        pass
                # statistics
                if review_cnt > 0:
                    repaired_products += 1

        print('Products repaired: {}'.format(str(repaired_products)))
        print('Reviews pushed: {}'.format(str(repaired_reviews)))

    def task(self, category: str, path: str):
        """
        Task for product reviews crawling
        :param path: path to url file
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
            with open(path + category + ".txt", 'r') as infile:
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
                        product.set_category(product_category[:-2])

                        # get reviews
                        self.parse_product_page(infile, product, category)

                        # add product to parsed products
                        product_reviews.append(product.get_name())

                        # add to elastic
                        p_n_c, r_n_c_n, rev_cnt = self.add_to_elastic(product, category)
                        review_count += rev_cnt
                        product_new_count += p_n_c
                        review_new_count_new += r_n_c_n

                        self.total_review_count += review_count
                        self.total_products_count += products_count
                        self.total_review_new_count += review_new_count_new
                        self.total_product_new_count += product_new_count

                    except IOError:
                        print("[task] Error: Cant open URL: ", url, file=sys.stderr)
                    except Exception as e:
                        print("[task] Error " + str(e))

        except Exception as e:
            print("[Task] " + str(e), file=sys.stderr)

    def submit_statistic(self, category: str, review_count: int, products_count: int, product_new_count: int, review_new_count: int):
        """
        Index statistics about actualization to the elastic search.
        :param category:
        :param review_count:
        :param products_count:
        :param product_new_count:
        :param review_new_count:
        :return:
        """
        try:
            document = {
                'category': category,
                'review_count': review_count,
                'affected_products': products_count,
                'new_products': product_new_count,
                'new_product_reviews': review_new_count,
                'date': date.today().strftime("%d. %B %Y").lstrip("0")
            }
            print(document)
            res = self.connector.index(index='actualize_statistic', doc=document)
            if res['result'] != 'created':
                print('Statistic for {} was not created'.format(category), file=sys.stderr)
                print(str(document), file=sys.stderr)
        except Exception as e:
            print("[submit_statistic] Error: Cant open URL: " + str(e), file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description="Crawl Heureka reviews as defined in config.py, Expects existence of URLS file for every category")
    parser.add_argument("-actualize", "--actualize", action="store_true", help="Actualize reviews")
    parser.add_argument("-aspect", "--aspect", action="store_true", help="Get aspects from category specification")
    parser.add_argument("-crawl", "--crawl", action="store_true", help="Crawl heureka reviews with url dataset")
    parser.add_argument("-path", "-path", help="Path to the dataset folder (ends with /)")
    parser.add_argument("-shop", "-shop", help="Crawl shop reviews", action="store_true")
    parser.add_argument("-filter", "-filter", help="Use model to filter irrelevant sentences", action="store_true")
    parser.add_argument("-rating", "-rating", help="Use model to predict rating of sentences", action="store_true")
    parser.add_argument("-repair", "-repair", help="Repair corrupted product reviews", type=int)

    args = vars(parser.parse_args())

    # create tagger
    tagger = MorphoTagger()
    tagger.load_tagger("../model/czech-morfflex-pdt-161115-no_dia-pos_only.tagger")

    # Elastic
    con = Connector()

    # Filter model
    heureka_filter = HeurekaFilter(args['filter'] )

    # Rating model
    heureka_rating = HeurekaRating(args['rating'])

    # Crawler
    crawler = HeurekaCrawler(con, tagger, heureka_filter, heureka_rating)

    for category in crawler.categories:
        if args['actualize']:
            # actualize reviews
            # always fast for now
            crawler.task_actualize(category, True)

        elif args['aspect']:
            # aspect extraction
            crawler.task_seed_aspect_extraction(category, args['path'])
        elif args['crawl']:
            # product reviews extraction
            crawler.task(category, args['path'])
        else:
            break

    if args['shop']:
        # crawl shop reviews
        crawler.task_shop()

    if args['repair']:
        # repair product reviews
        crawler.task_repair(args['repair'])

    if args['actualize'] or args['shop']:
        # Logs
        crawler.submit_statistic('All product domains', crawler.total_review_count, crawler.total_products_count,
                                 crawler.total_product_new_count, crawler.total_review_new_count)

        print('Empty revs : ' + str(crawler.total_empty_reviews))
        print('Irrelevant sentences : ' + str(crawler.irrelevant_sentences_count))
        print('Total sentences : ' + str(crawler.sentences_count))


if __name__ == '__main__':
    start = time.time()
    main()
    print(time.time() - start)
