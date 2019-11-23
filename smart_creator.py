import argparse

from morpho_tagger import MorphoTagger


def main():
    parser = argparse.ArgumentParser(
        description="Script groups revievs by finding key stems in them")
    # parser.add_argument('-w', '--words', help='List of words \"word word\"')
    # parser.add_argument('-out', '--output', help='Output file')
    args = vars(parser.parse_args())
    tagger = MorphoTagger()
    tagger.load_tagger("external/morphodita/czech-morfflex-pdt-161115-no_dia-pos_only.tagger")

    aspect_categories = {
        "cena": "ceny hodnota financne finančně drahý korun czk lacný cenové peněz",
        "vykon": "výkon vykon výkonný vykony sací vyluxovaný příkon",
        "zvuk": "tichy tichy hlučný hlucny tlumičů ramus rámus slyšet mluvit syčí klepe",
        "manuipulace": "šňůru těžší těžšký krátkou dlhou námahy cm vytáhnout nepoužitelný manipulace prostor",
        "zivotnost": "poškození deformace čas plastů šetří lacině provedení záruce rok vydržel křehkým zničí životnost"
    }

    # stems = [wb.lemma for wb in tagger.pos_tagging(args["words"])[0]]
    stemmed_aspects = {}

    for key, value in aspect_categories.items():
        for stem in [wb.lemma for wb in tagger.pos_tagging(value)[0]]:
            stemmed_aspects[stem] = key

    sentences = {
        "cena": [],
        "vykon": [],
        "zvuk": [],
        "manuipulace": [],
        "zivotnost": [],
    }
    other = []
    try:
        f_train = open("dataset_test_parsed.txt", "w")
        with open("dataset_test.txt", "r") as file:
            for line in file:
                line = line[:-1]
                category = []
                sentences_pos = tagger.pos_tagging(line)

                for sentence in sentences_pos:
                    for wp in sentence:
                        if wp.lemma in stemmed_aspects:
                            category.append(stemmed_aspects[wp.lemma])
                            # break
                    # else:
                        # Continue if the inner loop wasn't broken.
                    #    continue
                    # Inner loop was broken, break the outer.
                    # break
                category = list(set(category))
                if len(category) == 1:
                    sentences[category[0]].append(line)
                elif len(category) > 1:
                    other.append((line, category))
                else:
                    f_train.write(line + "\n")

        f_train.close()

        for key, value in sentences.items():
            print("["+key + "] Found : " + str(len(value)) + " senteces")
            with open("bert_service_ex_" + key, "w") as file:
                for s in value:
                    file.write(s + "\n")

        print("[Other] Found : " + str(len(other)) + " senteces")
        with open("dataset_other.txt", "w") as file:
            for sentence, categories in other:
                cats = " ".join(categories)
                file.write(sentence + " | " + cats + "\n")

    except Exception as e:
        print("[smart_creator] Exception: " + str(e))


if __name__ == '__main__':
    main()
