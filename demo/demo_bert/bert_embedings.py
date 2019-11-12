import argparse

import numpy as np
import matplotlib.pyplot as plt
from cluster import load_data_sentences


def print_sentence(labels, embV, s1):
    W = np.array(embV)
    B = np.array([embV[0], embV[-1]])
    Bi = np.linalg.pinv(B.T)

    print(W.shape)
    print(Bi.shape)

    Wp = np.matmul(Bi, W.T)
    print(Wp.shape)

    plt.figure(figsize=(12, 7))
    plt.axhline(color='black')
    plt.axvline(color='black')
    plt.scatter(Wp[0, :], Wp[1, :], label=s1)
    rX = max(Wp[0, :]) - min(Wp[0, :])
    rY = max(Wp[1, :]) - min(Wp[1, :])
    rM = max(rX, rY)
    eps = 0.005
    for i, txt in enumerate(labels):
        plt.annotate(txt, (Wp[0, i] + rX * eps, Wp[1, i] + rX * eps))
        if i > 0:
            plt.arrow(Wp[0, i - 1], Wp[1, i - 1], Wp[0, i] - Wp[0, i - 1], Wp[1, i] - Wp[1, i - 1], color='lightblue',
                      head_length=rM * eps * 3, head_width=rM * eps * 2, length_includes_head=True)
    plt.legend()
    plt.show()


def internet_example():
    from bert_embedding import BertEmbedding
    bert_embedding = BertEmbedding()

    s1 = "The sky is blue today."
    bert_embedding = BertEmbedding()
    embs = bert_embedding([s1])

    labels = embs[0][0]
    embV = embs[0][1]

    print_sentence(labels, embV, s1)


def get_visual_embs(data):
    """Get BERT embedding for the sentence,
    project it to a 2D subspace where [CLS] is (1,0) and [SEP] is (0,1)."""
    tokens, embV = data
    W = np.array(embV)

    B = np.array([embV[0], embV[-1]])
    Bi = np.linalg.pinv(B.T)
    Wp = np.matmul(Bi,W.T)

    return Wp, tokens


def main():
    parser = argparse.ArgumentParser(
        description="Script visualizes bert embedings")
    requiredNamed = parser.add_argument_group('required named arguments')
    requiredNamed.add_argument('-input', help='File of sentences[str]', required=True)
    requiredNamed.add_argument('-title', help='Title of graph[str]', required=True)

    args = parser.parse_args()
    try:
        # [([sentence], [tensors])]
        sentences_text = []
        with open(args.input, "r") as file:
            for line in file:
                sentences_text.append(line[:-1])
    except Exception as e:
        print(e)
        return

    data = load_data_sentences(args.input+".json")

    colors = ['blue', 'red', 'green', 'black', 'purple']

    plt.figure(figsize=(12, 7))
    plt.axhline(color='black')
    plt.axvline(color='black')

    for n, s in enumerate(sentences_text):
        Wp, tokens = get_visual_embs(data[n])
        plt.scatter(Wp[0, :], Wp[1, :], color=colors[n], marker='x', label=str(tokens))
        rX = max(Wp[0, :]) - min(Wp[0, :])
        rY = max(Wp[1, :]) - min(Wp[1, :])
        rM = max(rX, rY)
        eps = 0.005
        eps2 = 0.005
        for i, txt in enumerate(tokens):
            if txt in ['[CLS]', '[SEP]']:
                plt.annotate(txt, (Wp[0, i] + rX * eps, Wp[1, i] + rX * eps))
            #if txt in ['sac', 'vykon']:
            #    plt.annotate(txt, (Wp[0, i] + rX * eps, Wp[1, i] + rX * eps))
            if i > 0:
                plt.arrow(Wp[0, i - 1], Wp[1, i - 1], Wp[0, i] - Wp[0, i - 1], Wp[1, i] - Wp[1, i - 1], color=colors[n],
                          head_length=0, head_width=0, length_includes_head=False)
    plt.legend()
    plt.title(args.title)
    plt.savefig(args.input + ".png")
    plt.show()




if __name__ == '__main__':
    main()