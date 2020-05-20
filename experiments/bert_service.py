"""
This file is used for experimenting with Bert embeddings in similarity clustering task with kmeans algorithm.
As for bert embeddings it uses bert-as-service framework and bert_serving client. There needs to be server running
with Tensorflow.

"""
import argparse, random, nltk, time, operator, os
from bert_serving.client import BertClient
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
from termcolor import colored
from nltk.cluster import KMeansClusterer


def get_visual_embs(embV):
    """
    Project BERT embedding of text to 2D subspace where [CLS] is (1,0) and [SEP] is (0,1).
    :param embV:
    :return:
    """
    W = np.array(embV)

    B = np.array([embV[0], embV[8]])
    Bi = np.linalg.pinv(B.T)
    Wp = np.matmul(Bi, W.T)

    return Wp


def load_sentences(path: str, file: str, isTuple=True):
    """
    Load sentences from path described by arguments path and file with option of loading tuple.
    :param path: path to directory of dataset
    :param file: the name of file
    :param isTuple: dataset contain (sentece, category) tuple
    :return:
    """
    category = file.split("_")[-1]
    sentences = []
    with open(path + file, "r", encoding='utf-8') as file:
        for line in file:
            line = line[:-1]
            if line:
                sentences.append((line, category)) if isTuple else sentences.append(line)
    return sentences


def load_sentences_data_train_set(path: str):
    """
    Load all training datasets for similarity clustering task.
    :param path: path to dataset folder
    :return: list of sentences tuples
    """
    return load_sentences(path, "bert_service_ex_zvuk") + load_sentences(path,
                                                                         "bert_service_ex_vykon") + load_sentences(
        path, "bert_service_ex_zivotnost") + load_sentences(path, "bert_service_ex_manipulace") + load_sentences(
        path, "bert_service_ex_cena")


def load_sentences_data_test_set(path: str):
    """
    Load all testing datasets for embedding related task.
    :param path:
    :return:
    """
    return load_sentences(path + "bert_service_test_zvuk") + load_sentences(
        path + "bert_service_test_vykon") + load_sentences(
        path + "bert_service_test_zivotnost") + load_sentences(path + "bert_service_test_manipulace") + load_sentences(
        path + "bert_service_test_cena")


def visualize(data):
    """
    Visualize bert embeddings on 2D plot.
    :param data:
    :return:
    """
    colors = ['blue', 'red', 'green', 'black', 'purple']

    for n, (tokens, tensor) in enumerate(data):
        Wp = get_visual_embs(tensor)
        plt.scatter(Wp[0, :], Wp[1, :], color=colors[n], marker='x', label=str(tokens))
        rX = max(Wp[0, :]) - min(Wp[0, :])
        rY = max(Wp[1, :]) - min(Wp[1, :])
        rM = max(rX, rY)
        eps = 0.005
        eps2 = 0.005
        for i, txt in enumerate(tokens):
            if txt in ['[CLS]', '[SEP]']:
                plt.annotate(txt, (Wp[0, i] + rX * eps, Wp[1, i] + rX * eps))
            if i > 0:
                plt.arrow(Wp[0, i - 1], Wp[1, i - 1], Wp[0, i] - Wp[0, i - 1], Wp[1, i] - Wp[1, i - 1], color=colors[n],
                          head_length=0, head_width=0, length_includes_head=False)
        break
    plt.legend()
    plt.title("Embeddings")
    plt.savefig("embeddings" + ".png")
    plt.show()


def cluster(bc, path: str):
    """
    Perform similarity clustering  on bert embeddings from given dataset file and save results
    :param bc:
    :param path:
    :return:
    """
    # load sentences
    sentences = load_sentences(path, "dataset_emb.txt", isTuple=False)
    start = time.time()
    # get tensors from Bert model
    tensors = bc.encode(sentences, show_tokens=False)
    print("Clustering " + str(len(sentences)) + " sentences")
    print(time.time() - start)

    # perform clustering of tensors to 10 clusters with kmeans algorithm
    num_clusters = 10
    rng = random.Random(datetime.now())
    print("[cluster] Clusters: " + str(num_clusters))
    kclusterer = KMeansClusterer(num_clusters, distance=nltk.cluster.util.cosine_distance, repeats=60,
                                 avoid_empty_clusters=True, rng=rng)
    # assign labels to sentences
    assigned_clusters = kclusterer.cluster(tensors, assign_clusters=True)
    output = {}
    for k in range(0, num_clusters):
        output[k] = []

    for j, word in enumerate(sentences):
        output[assigned_clusters[j]].append(word)

    result = output

    # write out results
    dir = "clusters" + str(num_clusters)
    if not os.path.exists(dir):
        os.makedirs(dir)

    for key, value in result.items():
        with open(dir + "/" + str(key) + ".txt", "w", encoding='utf-8') as file:
            print("cluster: " + str(key) + " sentences: " + str(len(value)))
            for val in value:
                file.write(val + "\n")


def bert_service_dialog(bc, path: str):
    """
    Simple dialog from bert-as-service demo, that finds topk most similar sentences to given sentence in interactive
    commandline dialog
    :param bc: BertClient instance
    :param path: path to training dataset
    :return:
    """
    topk = 10
    try:
        q = load_sentences_data_train_set(path)
        doc_vecs = bc.encode([sentence for sentence, _ in q], show_tokens=False)

        while True:
            query = input(colored('your question: ', 'green'))
            query_vec = bc.encode([query])[0]
            # compute normalized dot product as score
            score = np.sum(query_vec * doc_vecs, axis=1) / np.linalg.norm(doc_vecs, axis=1)
            topk_idx = np.argsort(score)[::-1][:topk]
            print('top %d questions similar to "%s"' % (topk, colored(query, 'green')))
            category_predicted = {"zvuk": 0, "vykon": 0, "zivotnost": 0, "manipulace": 0, "cena": 0}

            for idx in topk_idx:
                name, category = q[idx]
                category_predicted[category] += score[idx]
                print('> %s\t%s' % (colored('%.1f' % score[idx], 'cyan'), colored(name, 'yellow')))

            l = max(category_predicted.items(), key=operator.itemgetter(1))

            print('> Predicted category: %s\t with score %s' % (colored(l[0], 'red'), colored(l[1], 'cyan')))

    except Exception as e:
        print('[bert_service_dialog] Exception: ' + str(e))


def test_embedding(bc, topk, q, doc_vecs):
    """
    Test trained context embeddings with different value of topk
    :param bc:  bert-as-service client
    :param topk: count of the most similar embedings
    :param q: training sentences
    :param doc_vecs: training embeddings
    :return:
    """
    # topk = 10
    results = {"zvuk": 0, "vykon": 0, "zivotnost": 0, "manipulace": 0, "cena": 0}
    try:
        f = open("ber_service_results_top" + str(topk), "w")
        q_train = load_sentences_data_test_set()

        for sentence, sentence_category in q_train:
            try:
                train_vec = bc.encode([sentence], show_tokens=False)
                category_predicted = {"zvuk": 0, "vykon": 0, "zivotnost": 0, "manipulace": 0, "cena": 0}
                score = np.sum(train_vec * doc_vecs, axis=1) / np.linalg.norm(doc_vecs, axis=1)
                topk_idx = np.argsort(score)[::-1][:topk]
                for idx in topk_idx:
                    name, category = q[idx]
                    category_predicted[category] += score[idx]
                l = max(category_predicted.items(), key=operator.itemgetter(1))

                f.write(sentence + " | " + sentence_category + " | " + str(l[0]) + "\n")

                if l[0] == sentence_category:
                    results[sentence_category] += 1

            except Exception as e:
                print("[test_embedding_" + str(sentence) + "] Exception: " + str(e))

    except Exception as e:
        print("[test_embedding] Exception: " + str(e))

    finally:
        f.close()
        print("Nearest k:" + str(topk) + str(results))


def main():
    parser = argparse.ArgumentParser(
        description="Script works with bert-as-service embedding ")
    parser.add_argument('-dia', '--dialog', help='Run active dialog', action='store_true')
    parser.add_argument('-clu', '--cluster', help='Run kmeans clustering', action='store_true')
    parser.add_argument('-test', '--test', help='Test context embedding, with n nearest sentece embeddings',
                        action='store_true')

    parser.add_argument('-v', '--visualize', help='Visualize bert embeddings with embeding projector, *.tsv files',
                        action='store_true')

    parser.add_argument('-p', '--path', help='Path for input files',
                        required=True)
    parser.add_argument('-ip', '--ip', help='IP for bert as service',
                        required=True)
    args = vars(parser.parse_args())

    # init bert-as-service client
    ipv4 = args['ip']
    bc = BertClient(ip=ipv4, timeout=60000)
    print("Connected to server")
    start = time.time()
    # switch for functionality
    if args['dialog']:
        bert_service_dialog(bc, args['path'])
    elif args['cluster']:
        cluster(bc, args['path'])
    elif args['test']:
        q = load_sentences_data_train_set(args['path'])
        doc_vecs = bc.encode([sentence for sentence, _ in q], show_tokens=False)
        for i in range(7, 20):
            print("test_embedding with topK: " + str(i))
            test_embedding(bc, i, q, doc_vecs)

    print(time.time() - start)


if __name__ == '__main__':
    main()
