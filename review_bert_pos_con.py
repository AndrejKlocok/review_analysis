from utils.elastic_connector import Connector
import sys, itertools

sys.path.append('../')
import re, time
from os import walk


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


def generate_dataset(connection, indexes):
    i = 0
    l = []
    for key, value in indexes.items():
        res_reviews = connection.match_all(key)
        for review in res_reviews:
            for pos in review['pros']:
                l.append('{}\t{}\t{}\t{}\n'.format(str(i), str(0), 'a', pos))
                i += 1

            for pos in review['cons']:
                l.append('{}\t{}\t{}\t{}\n'.format(str(i), str(1), 'a', pos))
                i += 1

    with open('tmp/dev.tsv', 'w') as file:
        for line in l:
            file.write(line)


def check_sentences(connection, key, value):
    start = time.time()
    sentence_to_id = {}
    review_id = {}
    res_reviews = connection.match_all(key)
    print(len(res_reviews))
    f_failed = open('failed.txt', 'w')
    for review in res_reviews:
        review['pos_model'] = []
        review['con_model'] = []

        for index, s in enumerate(review['pros']):
            review['pos_model'].append([])
            if s not in sentence_to_id:
                sentence_to_id[s] = []
            sentence_to_id[s].append([review['_id'], 'pros', index])

        for index, s in enumerate(review['cons']):
            if s not in sentence_to_id:
                sentence_to_id[s] = []
            sentence_to_id[s].append([review['_id'], 'con', index])
            review['con_model'].append([])

        review_id[review['_id']] = review

    evals = []
    path = 'tmp/tsvs/'
    for (dirpath, dirnames, filenames) in walk(path):
        evals.extend(filenames)
        break
    i = 0

    for eval in evals:
        sentences_processed = []
        name = eval.split('.')[0]
        print(name)
        with open(path + eval, 'r') as file:
            for line in file:
                line = line[:-1].split('\t')
                try:
                    if line[0] == 'cena':
                        print(line[0])
                    if line[0] in sentences_processed:
                        continue
                    sentences_processed.append(line[0])

                    positions = sentence_to_id[line[0]]
                    for position in positions:
                        review = review_id[position[0]]
                        if position[1] == 'pros':
                            review['pos_model'][position[2]].append(
                                [line[2], name + '_model']
                            )
                        else:
                            review['con_model'][position[2]].append(
                                [line[2], name + '_model']
                            )
                except KeyError as e:
                    pass
                except Exception as e:
                    print(e, file=sys.stderr)
    updated = 0
    failed = 0
    total = 0

    for id, review in review_id.items():
        wrong = False
        total += 1
        if total % 1000 == 0:
            print('Updated: {} Failed: {}'.format(str(updated), str(failed)))

        # empty do not update
        if not review['pos_model'] or not review['con_model']:
            continue

        # safe checks
        for pos in review['pos_model']:
            if len(pos) > 15:
                wrong = True
                break
        # safe checks
        for pos in review['con_model']:
            if len(pos) > 15:
                wrong = True
                break
        # safe checks
        if wrong:
            continue

        body = {
            'doc': {
                'pos_model': review['pos_model'],
                'con_model': review['con_model']
            }
        }
        res = connection.es.update(index=value, id=review['_id'], body=body)
        connection.es.indices.refresh(index=value)
        if res['result'] != 'updated':
            #print('review: {} was not updated'.format(review['_id']))
            f_failed.write('{}\t{}\t{}\n'.format(review['_id'], str(body), str(res)))
            failed += 1
        else:
            updated += 1


    print('Created in {} seconds'.format(time.time() - start))


def main():
    connection = Connector()
    indexes = {
        'Auto-moto': 'auto-moto',
    }

    # generate_dataset(connection, indexes)
    for key, value in indexes.items():
        check_sentences(connection, key, value)



if __name__ == '__main__':
    main()
