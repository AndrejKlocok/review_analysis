from utils.elastic_connector import Connector
import sys
sys.path.append('../')
from clasification.bert_model import Bert_model
from heureka_filter import HeurekaFilter
from utils.morpho_tagger import MorphoTagger
import re, time


def round_percentage(number):
    return round(round(number * 100.0, -1))


def merge_review_text(pos: list, con: list, summary: str):
    text = []
    text += [clear_sentence(s) for s in pos]
    text += [clear_sentence(s) for s in con]
    text += [summary]
    return ' '.join(text)


def clear_sentence(sentence: str) -> str:
    sentence = sentence.strip().capitalize()
    sentence = re.sub(r'\.{2,}', "", sentence)
    sentence = re.sub(r'\t+', ' ', sentence)
    try:
        if sentence[-1] != '.':
            sentence += '.'
    except Exception as e:
        pass

    return sentence


def eval_sentence(model: Bert_model, sentence: str, useLabels=True):
    sentence = clear_sentence(sentence)
    if sentence:
        return sentence, model.eval_example('a', sentence, useLabels)
    else:
        return None, None


def get_str_pos(l):
    s = []
    for sentence in l:
        s.append([str(wb) for wb in sentence])
    return s


def load_models(path, labels):
    d = {
    }
    indexes = [
        'elektronika',
        'bile_zbozi',
        'dum_a_zahrada',
        'chovatelstvi',
        'auto-moto',
        'detske_zbozi',
        'obleceni_a_moda',
        'filmy_knihy_hry',
        'kosmetika_a_zdravi',
        'sport',
        'hobby',
        'jidlo_a_napoje',
        'stavebniny',
        'sexualni_a_eroticke_pomucky',
    ]
    for value in indexes:
        d[value] = Bert_model(path + 'bert_bipolar_domain/' + value, labels)
        d[value].do_eval()

    return d


def main():
    connnection = Connector()
    indexes = {
        'Auto-moto': 'auto-moto',
    }
    irrelevant_count = 0
    #path = '/home/andrej/Documents/school/Diplomka/model/'
    path = '/mnt/data/xkloco00_a18/model/'
    pos_con_labels = ['+', '-']
    start = time.time()
    filter_model = HeurekaFilter(useCls=True)
    tagger = MorphoTagger()
    tagger.load_tagger("external/morphodita/czech-morfflex-pdt-161115-no_dia-pos_only.tagger")
    pos_con_model = Bert_model(path + 'bert_bipolar', pos_con_labels)
    pos_con_model.do_eval()
    #regression_model = Bert_model(path + 'bert_regression', [])
    #regression_model.do_eval()
    model_d = load_models(path, pos_con_labels)

    print('Models loaded in {} seconds'.format(time.time() - start))
    for key, value in indexes.items():

        res_reviews = connnection.match_all(key)
        i = 0
        for review in res_reviews:
            if i % 1000 == 0:
                print('{}:{}x review'.format(key, str(i)))
            #review_text = merge_review_text(review['pros'], review['cons'], review['summary'])
            #if review_text:
            #    _, rating = eval_sentence(regression_model, review_text, useLabels=False)
            #    if rating:
            #        rating = '{}%'.format(round_percentage(rating))
            #    else:
            #        rating = '0%'
            #else:
            #    rating = '0%'

            pos_model = []
            con_model = []
            pros = []
            pros_POS = []
            pros_ir = False

            cons = []
            cons_POS = []
            cons_ir = False

            summary_ir = False

            for pos in review['pros']:
                try:
                    s, label = eval_sentence(pos_con_model, pos)
                    if filter_model.is_irrelevant(s):
                        pros_ir = True
                        irrelevant_count += 1
                    else:
                        pros.append(s)
                        pros_POS.append(get_str_pos(tagger.pos_tagging(s, stem=False)))

                    pos_model.append([s, label, 'general_model'])

                    for category, model in model_d.items():
                        s, label = eval_sentence(model, pos)
                        pos_model.append(([s, label, category+'_model']))

                except Exception as e:
                    print('[pos] sentence: {} is not classified'.format(pos), file=sys.stderr)
                    pass

            for con in review['cons']:
                try:
                    s, label = eval_sentence(pos_con_model, con)
                    if filter_model.is_irrelevant(s):
                        cons_ir = True
                        irrelevant_count += 1
                    else:
                        cons.append(s)
                        cons_POS.append(get_str_pos(tagger.pos_tagging(s, stem=False)))
                    con_model.append([s, label, 'general_model'])

                    for category, model in model_d.items():
                        s, label = eval_sentence(model, con)
                        pos_model.append(([s, label, category+'_model']))

                except Exception as e:
                    print('[con] sentence: {} is not classified'.format(con), file=sys.stderr)

            if review['summary'] and filter_model.is_irrelevant(review['summary']):
                summary_ir = True
                irrelevant_count += 1

            body = {
                'doc':{
                    #'rating_model': rating,
                    'pos_model': pos_model,
                    'con_model': con_model
                }
            }
            if pros_ir:
                body['doc']['pros'] = pros
                body['doc']['pros_pos'] = pros_POS

            if cons_ir:
                body['doc']['cons'] = cons
                body['doc']['cons_pos'] = cons_POS

            if summary_ir:
                body['doc']['summary'] = ''
                body['doc']['summary_pos'] = []

            res = connnection.es.update(index=value, id=review['_id'], body=body)
            connnection.es.indices.refresh(index=value)
            if res['result'] != 'updated':
                print('review: {} was not updated'.format(review['_id']))
            i += 1
            print(body)
            print(review['_id'])
            break

        print('Category {} reindexed in {} seconds'.format(key,time.time() - start))
        connnection.es.indices.refresh(index=value)


if __name__ == '__main__':
    main()
