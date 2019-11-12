from polyglot.mapping import Embedding
import sys

def main():
    embeddings = Embedding.load("/home/andrej/polyglot_data/embeddings2/cs/embeddings_pkl.tar.bz2")
    print(embeddings.nearest_neighbors("knížka"))

    while True:
        line = sys.stdin.readline()
        if "." in line:
            break
        line_l = line.split()
        try:
            for val in line_l:
                print(embeddings.nearest_neighbors(val))
        except Exception as e:
            print(e)
    pass


if __name__ == '__main__':
    main()