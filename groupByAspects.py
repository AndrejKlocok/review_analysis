import argparse

from discussion import Files
import json, time


def task(category, args):
    try:
        f = Files(category)

        if args.sentence:
            # generate sentences and their aspects that are reffering to
            unknown = "nezaradeno"
            aspect_sentence_d = {unknown: []}

            with open(f.seed_aspect_name, "r") as file:
                for line in file:
                    product_dict = json.loads(line[:-1])
                    try:
                        for sentence_d in product_dict["pos"]:
                            a_str = " ".join(sentence_d["aspects"])
                            cat_name = product_dict["product"].split("(")[-1][:-1]
                            sentence_str = " ".join([wb["lemma"] for wb in sentence_d["sentence"]])
                            sentence_str += " | " + cat_name
                            if a_str:
                                if a_str not in aspect_sentence_d:
                                    aspect_sentence_d[a_str] = [sentence_str]
                                else:
                                    aspect_sentence_d[a_str].append(sentence_str)
                            else:
                                aspect_sentence_d["nezaradeno"].append(sentence_str)

                    except Exception as e:
                        print("[task-sentence] Error: " + str(e))

            with open(category + "_group_senteces.txt", "w") as log_file:
                for aspect_name, sentece_list in aspect_sentence_d.items():
                    s = aspect_name + " " + str(len(sentece_list))
                    print(s)
                    log_file.write(s + "\n")
                    if aspect_name == unknown:
                        continue

                    for sentence in sentece_list:
                        log_file.write("\t" + sentence + " | " + aspect_name + " " + "\n")
                    log_file.write("\n")

        elif args.freq:
            aspect_category_d = f.get_aspects(f.aspect_pos_name)
            freq_d = {}
            # find frequent nouns that are not categorized as aspects
            with open(f.seed_aspect_name, "r") as file:
                for line in file:
                    try:
                        product_dict = json.loads(line[:-1])
                        cat_name = product_dict["product"].split("(")[-1][:-1]
                        for sentence_d in product_dict["pos"]:
                            for wb in sentence_d["sentence"]:
                                # if lemma is noun and if it is not already in aspect cat dict
                                aspect_d = aspect_category_d[cat_name].aspects_dict
                                phrase = wb["lemma"] + "_" + cat_name
                                if wb["tag"][0] == "N" and wb["lemma"] not in aspect_d:
                                    if phrase not in freq_d:
                                        freq_d[phrase] = 1
                                    else:
                                        freq_d[phrase] += 1

                    except Exception as e:
                        print("[task-sentence] Error: " + str(e))

            freq_sort = [(k, freq_d[k]) for k in sorted(freq_d, key=freq_d.get, reverse=True)]
            with open(f.category + "_freq_aspects.txt", "w") as logfile:
                for key, val in freq_sort:
                    logfile.write(key + " " + str(val) + "\n")

    except Exception as e:
        print("[task] Error: " + str(e))


def main():
    parser = argparse.ArgumentParser(
        description="Script groups information from processed reviews from POS")
    parser.add_argument("-sentence", action="store_true", help="Generate setences to aspect grouping")
    parser.add_argument("-freq", action="store_true", help="Generate freq list of aspects")

    args = parser.parse_args()

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
        task(category, args)
        print(time.time() - start)


if __name__ == '__main__':
    main()
