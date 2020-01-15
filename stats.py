import json, time
import statistics
import argparse
from datetime import datetime
from dateutil import parser
from matplotlib import pyplot as plt
from matplotlib import style
import sys
import pandas as pd

from utils.elastic_connector import Connector


class TimeData:
    def __init__(self, sentiment: float):
        self.count = 1
        self.sentiments = [sentiment]
        self.mean_sentiment = 0.0

    def add_data(self, sentiment: float):
        self.sentiments.append(sentiment)
        self.count += 1

    def inc_count(self):
        self.count += 1

    def merge_data(self, data):
        self.sentiments += data.sentiments
        self.count += data.count

    def compute_mean_sentiment(self):
        try:
            self.mean_sentiment = statistics.mean(self.sentiments)
        except:
            pass


class CategoryStatistic:
    def __init__(self):
        self.count = 0
        self.sentiments = []
        self.sentiment_max = 0.0
        self.sentiment_min = 0.0
        # {date: TimeData}
        self.stat_review_month: {datetime: TimeData} = {}
        self.recommends = 0
        self.pros_len = []
        self.cons_len = []
        self.summary_len = []


def serialize(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, TimeData):
        serial = obj.count
        return str(serial)
    return obj.__dict__


class Statistic:
    def __init__(self):
        # categories -> { category : ( avg sentiment, lowest, max, review counts, dict_stat}
        self.prod_count = 0
        self.reviews_count = 0

        self.category_map = {}
        self.categories = {}
        self.month_mapper = {
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

    def add_category(self, category: str):
        if category not in self.categories:
            self.categories[category] = CategoryStatistic()

    def submit_review(self, review: dict):
        category = review['category']
        category_stat: CategoryStatistic = self.categories[category]
        self.reviews_count += 1
        category_stat.count += 1
        try:
            sentiment = float(review["rating"][:-1]) / 100

            date_str = review["date_str"].replace('\xa0', ' ')
            month = self.map_month_to_english(date_str.split(".")[1][1:])

            category_stat.sentiments.append(sentiment)
            category_stat.recommends += 1 if review["recommends"] == "YES" else 0

            if month not in category_stat.stat_review_month:
                category_stat.stat_review_month[month] = TimeData(sentiment)
            else:
                category_stat.stat_review_month[month].add_data(sentiment)

            summary_len_words = len(review["summary"].split(" "))
            summary_len_words += len(review["pros"]) + len(review["cons"])

            if summary_len_words > 0:
                category_stat.summary_len.append(summary_len_words)

            category_stat.pros_len.append(len(review["pros"]))
            category_stat.cons_len.append(len(review["cons"]))
        except Exception as e:
            print("[submit_review]" + str(e))
            return

    def map_month_to_english(self, month: str) -> str:
        m = month.split(" ")
        if m[0] in self.month_mapper:
            m[0] = self.month_mapper[m[0]]

        return m[0] + " " + m[1]

    def sort_month_dic_to_list(self, dic: dict) -> list:
        l = []
        for month, time_data in dic.items():
            time_data.compute_mean_sentiment()
            l.append((parser.parse(month), (time_data.count, round(time_data.mean_sentiment, 2))))
        l.sort(key=lambda x: x[0], reverse=True)
        return l

    def plot_dates(self, date_list, category_name):
        with open("log_dates.txt", "w") as dates_file:
            for d, time_data in date_list:
                dates_file.write(d.strftime("%D_%Y") + " " + str(time_data[0]) + " " + str(time_data[1]) + "\n")
        x, y = zip(*date_list)
        y1, y2 = zip(*y)

        # reviews
        style.use('ggplot')
        plt.title('[' + category_name + '] Review count in time')
        plt.ylabel('Review count')
        plt.xlabel('Date')
        plt.plot(x, y1)
        # plt.show()
        plt.savefig(category_name + '_reviews.png')

        plt.clf()

        # sentiments
        plt.title('[' + category_name + ']Average sentiment in time')
        plt.ylabel('Sentiment [%]')
        plt.xlabel('Date')
        plt.plot(x, y2)
        # plt.show()
        plt.savefig(category_name + '_sentiment.png')

        pass

    def category_tree(self, category_string: str):
        d = self.category_map
        cats = category_string.split("|")
        for cat in cats[1:-1]:
            if cat not in d:
                d[cat] = {}
            d = d[cat]

        if cats[-1] not in d:
            d[cats[-1]] = 1
        else:
            d[cats[-1]] += 1

    def category_add(self, d: dict, s: str):
        if str not in d:
            d[str] = {}

    def log(self, category_name: str):
        d = {}
        sentiments = []
        min_s = 0.0
        max_s = 0.0
        mean_s = 0.0
        summaries = []
        min_summary = 0.0
        max_summary = 0.0
        mean_summary = 0.0
        pros = []
        min_pros = 0.0
        max_pros = 0.0
        mean_pros = 0.0
        cons = []
        min_cons = 0.0
        max_cons = 0.0
        mean_cons = 0.0

        dates_all = {}

        for name, stat in self.categories.items():
            dates = json.dumps(stat.stat_review_month, default=serialize)
            for key, value in stat.stat_review_month.items():
                if key not in dates_all:
                    dates_all[key] = value
                else:
                    dates_all[key].merge_data(value)
            try:
                meanS = statistics.mean(stat.sentiments)
                meanSum = statistics.mean(stat.summary_len)
                proS = statistics.mean(stat.pros_len)
                conS = statistics.mean(stat.cons_len)

                sentiments.append(meanS)
                pros.append(proS)
                cons.append(conS)
                summaries.append(meanSum)

                cat = {
                    "name": name,
                    "reviews": str(stat.count),
                    "recommends": str(stat.recommends),
                    "mean_sentiment": str(meanS),
                    "max_sentiment": str(max(stat.sentiments)),
                    "min_sentiment": str(min(stat.sentiments)),
                    "mean_pros_len": str(proS),
                    "max_pros_len": str(max(stat.pros_len)),
                    "min_pros_len": str(min(stat.pros_len)),
                    "mean_con_len": str(conS),
                    "max_cons_len": str(max(stat.cons_len)),
                    "min_cons_len": str(min(stat.cons_len)),
                    "mean_summary_len": str(meanSum),
                    "max_summary_len": str(max(stat.cons_len)),
                    "min_summary_len": str(min(stat.cons_len))  # ,
                    # "dates": dates
                }
            except Exception as e:
                print("[" + name + "]" + str(e))
            d[name] = cat
        # self.pros_len = []
        # self.cons_len = []
        # self.summary_len = []

        min_s = min(sentiments)
        max_s = max(sentiments)
        mean_s = statistics.mean(sentiments)

        min_pros = min(pros)
        max_pros = max(pros)
        mean_pros = statistics.mean(pros)

        min_cons = min(cons)
        max_cons = max(cons)
        mean_cons = statistics.mean(cons)

        min_sum = min(summaries)
        max_sum = max(summaries)
        mean_sum = statistics.mean(summaries)

        dates_list = self.sort_month_dic_to_list(dates_all)

        print("Product counts: " + str(self.prod_count))
        print("Review counts: " + str(self.reviews_count))
        print("Categories: " + str(len(d.items())))
        print("Mean sentiment: " + str(round(mean_s, 2)))
        print("Max sentiment: " + str(round(max_s, 2)))
        print("Min sentiment: " + str(round(min_s, 2)))
        print("Mean pros per review: " + str(round(mean_pros, 2)))
        print("Mean cons per review: " + str(round(mean_cons, 2)))
        print("Mean review length: " + str(round(mean_sum, 2)))

        with open("log.txt", "w") as log_file:
            log_file.write(json.dumps(d))

        self.plot_dates(dates_list, category_name)


def parse_old_data(file: str):
    stats = Statistic()
    with open("all_in_one_out", "r") as json_file:
        data = json.load(json_file)
        for product in data["REVIEWS"]:
            try:
                stats.prod_count += 1
                # just one ...
                for key, review in product.items():
                    name = key.split("(")[0]
                    category = key.split("(")[1][:-1]
                    # print(name + " " + category)
                    stats.add_category(category)

                    for rev in review:
                        stats.reviews_count += 1
                        stats.submit_review(category, rev["review"])

            except Exception as e:
                print(e)
                pass

    stats.log(file)


def parse(category_domain, con: Connector):
    stats = Statistic()
    i = 0
    stats.prod_count = con.get_count('product', category_domain)
    data = con.match_all(category_domain)
    for d in data:
        try:
            stats.add_category(d['category'])
            stats.submit_review(d)
        except Exception as e:
            print(e)

    stats.log(category_domain)


def parse_actualized_data(path, categories):
    count_all = 0
    product_all = 0
    product_new_all = 0
    count_new_all = 0

    for category in categories:
        count = 0
        products = 0
        product_new = 0
        count_new = 0
        product_list = []
        f = Files(category)
        with open(path + "/" + f.backup_name, "r") as backup_file:
            for line in backup_file:
                o = json.loads(line[:-1])
                product_list.append(o["name"])

        with open(path + "/" + f.actualized_name, "r") as actualize_file:
            for line in actualize_file:
                o = json.loads(line[:-1])
                count += len(o["reviews"])
                products += 1
                if o["name"] not in product_list:
                    product_new += 1
                    count_new += len(o["reviews"])

        print(category + " has: " + str(count) + " reviews, affected products: " + str(
            products) + ", new products: " + str(product_new) + ", new product`s reviews: " + str(count_new))
        count_all += count
        product_all += products
        product_new_all += product_new
        count_new_all += count_new

    print(
        "Total new reviews: " + str(count_all) + ", affected products: " + str(product_all) + ", new products: " + str(
            product_new_all) + ", new product`s reviews: " + str(count_new_all))


def histogram(category: str, con: Connector, numb_cat: int):
    try:
        print(category)
        data = con.get_subcategories_count(category)

        print("category count " + str(len(data)))
        print("TOP 10 categories")
        # print top 10
        for i in range(0, 10):
            cat, count = data[i]
            print("* " + cat + ": " + str(count))
        print("\n")

        # just top 100
        data = data[:numb_cat]
        plt.title('[' + category + '] Histogram of reviews between categories')
        plt.ylabel('review counts')
        plt.xlabel('categories')
        plt.bar([i for i, _ in enumerate(data)], [value for _, value in data], color='g')
        #plt.show()
        plt.savefig("../statistics/" + category + '_hist.png')
        plt.clf()

    except Exception as e:
        print("[histogram-" + category + "] Error: " + str(e))


def parse_csv(file):
    d = {
        'reviews': {'Elektronika':[], 'Bile zbozi': [], 'Dum a zahrada': [], 'Chovatelstvi': [], 'Auto-moto': [],
                  'Detske zbozi': [], 'Obleceni a moda': [], 'Filmy knihy hry': [], 'Kosmetika a zdravi':[],
                  'Sport':[], 'Hobby':[], 'Jidlo a napoje': [], 'Stavebniny': [],
                  'Sexualni a eroticke pomucky': [], 'Total': []},
        'affected_products': {'Elektronika':[], 'Bile zbozi': [], 'Dum a zahrada': [], 'Chovatelstvi': [], 'Auto-moto': [],
                  'Detske zbozi': [], 'Obleceni a moda': [], 'Filmy knihy hry': [], 'Kosmetika a zdravi':[],
                  'Sport':[], 'Hobby':[], 'Jidlo a napoje': [], 'Stavebniny': [],
                  'Sexualni a eroticke pomucky': [], 'Total': []},
        'new_products': {'Elektronika':[], 'Bile zbozi': [], 'Dum a zahrada': [], 'Chovatelstvi': [], 'Auto-moto': [],
                  'Detske zbozi': [], 'Obleceni a moda': [], 'Filmy knihy hry': [], 'Kosmetika a zdravi':[],
                  'Sport':[], 'Hobby':[], 'Jidlo a napoje': [], 'Stavebniny': [],
                  'Sexualni a eroticke pomucky': [], 'Total': []},
        'new_product_reviews': {'Elektronika':[], 'Bile zbozi': [], 'Dum a zahrada': [], 'Chovatelstvi': [], 'Auto-moto': [],
                  'Detske zbozi': [], 'Obleceni a moda': [], 'Filmy knihy hry': [], 'Kosmetika a zdravi':[],
                  'Sport':[], 'Hobby':[], 'Jidlo a napoje': [], 'Stavebniny': [],
                  'Sexualni a eroticke pomucky': [], 'Total': []},
    }

    dates = []

    df = pd.read_csv(file)
    for _, row in df.iterrows():
        d['reviews'][row['category']].append(row['reviews'])
        d['affected_products'][row['category']].append(row['affected_products'])
        d['new_products'][row['category']].append(row['new_products'])
        d['new_product_reviews'][row['category']].append(row['new_product_reviews'])
        date_str = " ".join(row['date'].split()[:-1])
        if date_str not in dates:
            dates.append(date_str)

    if False:
        for name_plot, plot_data in d.items():
            style.use('ggplot')
            plt.title(name_plot+' in time')
            plt.ylabel('Count')
            plt.xlabel('Date')

            for category, values in plot_data.items():
                if category == 'Total':
                    continue
                plt.plot(dates, values, label=category, marker='.')
            # plt.show()
            # plt.savefig(category_name + '_reviews.png')
            plt.legend()
            plt.show()
            plt.clf()

    # print Total revs, products
    style.use('ggplot')
    plt.title('Priebeh aktualizácie')
    plt.ylabel('Počet')
    plt.xlabel('Dátum')

    plt.plot(dates, d['reviews']['Total'], label='nové_recenzie', marker='.')
    plt.plot(dates, d['new_products']['Total'], label='nové_produkty', marker='.')
    # plt.show()
    # plt.savefig(category_name + '_reviews.png')
    plt.legend()
    plt.show()
    plt.clf()


def main():
    domain = {
        'el': 'Elektronika',
        'bz': 'Bile zbozi',
        'daz': 'Dum a zahrada',
        'chv': 'Chovatelstvi',
        'am': 'Auto-moto',
        'dz': 'Detske zbozi',
        'om': 'Obleceni a moda',
        'fkh': 'Filmy, knihy, hry',
        'kaz': 'Kosmetika a zdravi',
        'sp': 'Sport',
        'hob': 'Hobby',
        'jan': 'Jidlo a napoje',
        'svb': 'Stavebniny',
        'sex': 'Sexualni a eroticke pomucky',
    }

    parser_arg = argparse.ArgumentParser(description="Compute statistics")
    parser_arg.add_argument('-hi', "--histogram", default=0, type=int,
                            help="Create histogram of count of rev for each category")
    parser_arg.add_argument('-s', "--statistic", help="Statistics from elastic data "
                            , action='store_true')

    parser_arg.add_argument('-d', '--domain', help='Generate dataset from domain in.\n'
                                                   + str(domain))
    parser_arg.add_argument('-a', "--actualize", help="Statistics from ACTUALZE data ")
    parser_arg.add_argument('-csv', "--csv", help="Statistics from CSV data ")

    args = vars(parser_arg.parse_args())

    if not args['csv'] and args['domain'] not in domain:
        print(args['domain'] + ' is not a valid option, see -h', file=sys.stderr)
        sys.exit(1)

    if not args['csv']:
        category = domain[args['domain']]

    # Elastic
    con = Connector()

    if args['actualize']:
        parse_actualized_data(args['actualize'], category)
    elif args['histogram']:
        histogram(category, con, args['histogram'])
    elif args['statistic']:
        parse(category, con)
    elif args['csv']:
        parse_csv(args['csv'])


if __name__ == '__main__':
    start = time.time()
    main()
    print(time.time() - start)
