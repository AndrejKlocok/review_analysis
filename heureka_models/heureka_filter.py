"""
This file contains implementation of class HeurekaFilter, which wraps up SVM classifier with FastText word embeddings
with uSIF weighting scheme. If outcome of  classification in method is_irrelevant is True, the sentence is dumped to
irrelevant.tsv file.

Author: xkloco00@stud.fit.vutbr.cz
"""
import sys
from datetime import date
sys.path.append('../')
from clasification.SVM_model import SVM_Classifier


class HeurekaFilter:
    """
    Class handles filtering of irrelevant sentences with creation of new dataset irrelevant_sentences.tsv. To this
    dataset all irrelevant sentences, that are longer than one word are written.
    """
    def __init__(self, useCls: bool):
        """
        Constructor loads irrelevant classifier model and opens file for dumping irrelevant sentences.
        :param useCls:
        """
        self.model = None
        if useCls:
            self.model = SVM_Classifier(path='../model/')
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

            self.log_file = open('irrelevant_sentences_'+ date.today().strftime('%d_%m')+'.tsv', "w")

        except Exception as e:
            print('[HeurekaFilter] Exception: ' + str(e))
            self.log_file = None

    def is_irrelevant(self, sentence: str) -> bool:
        """
        Evaluate sentence with SVM classifier.
        :param sentence:
        :return:
        """
        # one word sentences are irrelevant
        if len(sentence.split()) <= 1:
            return True

        # evaluate sentence with trained model, if we use one
        if self.model:
            if self.model.eval_example(sentence) == 'irrelevant':
                if self.log_file:
                    # dump sentence
                    self.index += 1
                    self.log_file.write('{0}\t0\ta\t{1}\n'.format(str(self.index), sentence))
                return True

        return False

    def __del__(self):
        """
        Destructor method closes opened file for dumping irrelevant sentences.
        :return:
        """
        if self.log_file:
            self.log_file.close()


def main():
    heureka_filter = HeurekaFilter(True)

    with open('../tmp/dataset_positive.txt', "r") as file:
        for line in file:
            line = line[:-1]
            label = heureka_filter.is_irrelevant(line)
            print(line + '\t' + label)


if __name__ == '__main__':
    main()
