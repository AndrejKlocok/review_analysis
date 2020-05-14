import sys

sys.path.append('../')
from clasification.SVM_model import SVM_Classifier


class HeurekaFilter:
    def __init__(self, useCls: bool):

        self.model = None
        if useCls:
            self.model = SVM_Classifier(path='../../model/')
            self.model.load_models()

        self.log_file = None
        self.index = 0
        try:
            # get last index from irrelevant tsv file
            irrelevant_file = open(self.model.irrelevant_path, "r")
            for line in irrelevant_file:
                row = line.split('\t')

            # last read row
            self.index = int(row[0])

            self.log_file = open('irrelevant_sentences.tsv', "w")

        except Exception as e:
            print('[HeurekaFilter] Exception: ' + str(e))
            self.log_file = None

    def is_irrelevant(self, sentence):
        # TODO split sentences !
        # one word sentences are irrelevant
        if len(sentence.split()) <= 1:
            return True
        # too long review sentence is kind of valid
        if len(sentence.split()) > 10:
            return False

        # evaluate sentence with trained model, if we use one
        if self.model:
            if self.model.eval_example(sentence) == 'irrelevant':
                if self.log_file:
                    self.index += 1
                    self.log_file.write('{0}\t0\ta\t{1}\n'.format(str(self.index), sentence))
                return True

        return False

    def __del__(self):
        if self.log_file:
            self.log_file.close()


def main():
    heureka_filter = HeurekaFilter()

    with open('../tmp/dataset_positive.txt', "r") as file:
        for line in file:
            line = line[:-1]
            label = heureka_filter.is_irrelevant(line)
            print(line + '\t' + label)


if __name__ == '__main__':
    main()
