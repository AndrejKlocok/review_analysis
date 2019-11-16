import argparse
from bert_serving.client import BertClient
import matplotlib.pyplot as plt
import numpy as np
from termcolor import colored
import operator
from nltk.cluster import KMeansClusterer
import nltk
import time


def get_visual_embs(embV):
    """Get BERT embedding for the sentence,
    project it to a 2D subspace where [CLS] is (1,0) and [SEP] is (0,1)."""
    W = np.array(embV)

    B = np.array([embV[0], embV[8]])
    Bi = np.linalg.pinv(B.T)
    Wp = np.matmul(Bi, W.T)

    return Wp


def load_sentences(file, tuple=True):
    category = file.split("_")[-1]
    sentences = []
    with open(file, "r") as file:
        for line in file:
            line = line[:-1]
            if line:
                sentences.append((line, category)) if tuple else sentences.append(line)
    return sentences


def load_sentences_data_train_set():
    return  load_sentences("bert_service_ex_zvuk") + load_sentences("bert_service_ex_vykon") + load_sentences(
            "bert_service_ex_zivotnost") + load_sentences("bert_service_ex_manipulace") + load_sentences(
            "bert_service_ex_cena")


def load_sentences_data_test_set():
    return load_sentences("bert_service_test_zvuk") + load_sentences("bert_service_test_vykon") + load_sentences(
        "bert_service_test_zivotnost") + load_sentences("bert_service_test_manipulace") + load_sentences(
        "bert_service_test_cena")


def visualize(data):
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


def cluster(bc ):
    sentences = load_sentences("dataset_test.txt", tuple=False)
    # just 1000 sentences
    sentences = sentences[:2000]
    start = time.time()
    tensors = bc.encode(sentences, show_tokens=False)
    print(time.time() - start)
    result = []
    num_clusters = 0

    for i in range(20, 30):
        try:
            num_clusters = i
            print("[cluster] Clusters: " + str(num_clusters))
            kclusterer = KMeansClusterer(num_clusters, distance=nltk.cluster.util.cosine_distance, repeats=25)
            assigned_clusters = kclusterer.cluster(tensors, assign_clusters=True)
            output = {}
            for k in range(0, num_clusters):
                output[k] = []

            for j, word in enumerate(sentences):
                output[assigned_clusters[j]].append(word)

            result = output

        except Exception as e:
            print(e)
            break

    f = open("clusters_"+str(num_clusters)+".txt", "w")
    for key, value in result.items():
        f.write(str(key) + " : ")
        for val in value:
            f.write(val + " | ")
        f.write("\n")
    f.close()


def bert_service_dialog(bc):
    topk = 10
    try:
        q = load_sentences_data_train_set()
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
        print(e)


def test_embedding(bc, topk, q, doc_vecs):
    """
    Test trained context embeddings with different value of topk
    :param bc:  bert-as-service client
    :param topk: count of the most similar embedings
    :param q: training sentences
    :param doc_vecs: training embeddings
    :return:
    """
    #topk = 10
    results = {"zvuk": 0, "vykon": 0, "zivotnost": 0, "manipulace": 0, "cena": 0}
    try:
        f = open("ber_service_results_top"+str(topk), "w")
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

                f.write(sentence + " | " + sentence_category +" | "+ str(l[0]) +"\n")

                if l[0] == sentence_category:
                    results[sentence_category] += 1

            except Exception as e:
                print("[test_embedding_" + str(sentence) +"] Exception: " + str(e))

    except Exception as e:
        print("[test_embedding] Exception: " + str(e))

    finally:
        f.close()
        print("Nearest k:" + str(topk) + str(results))


def main():
    parser = argparse.ArgumentParser(
        description="Script works with bert-as-service embedding")
    parser.add_argument('-dia', '--dialog', help='Run active dialog', action='store_true')
    parser.add_argument('-clu', '--cluster', help='Run kmeans clustering', action='store_true')
    parser.add_argument('-test', '--test', help='Test context embedding, with n nearest sentece embeddings',
                        action='store_true')
    args = vars(parser.parse_args())

    ipv4 = "192.168.0.123"
    bc = BertClient(ip=ipv4, timeout=60000)
    print("Connected to server")
    if args['dialog']:
        bert_service_dialog(bc)
    elif args['cluster']:
        start = time.time()
        cluster(bc)
        print(time.time() - start)
    elif args['test']:
        start = time.time()
        q = load_sentences_data_train_set()
        doc_vecs = bc.encode([sentence for sentence, _ in q], show_tokens=False)
        for i in range(7,20):
            print("test_embedding with topK: " + str(i))
            test_embedding(bc, i, q, doc_vecs)
        print(time.time() - start)


if __name__ == '__main__':
    main()
