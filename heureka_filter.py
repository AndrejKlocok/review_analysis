from clasification.bert_model import Bert_model


class HeurekaFilter:
    def __init__(self, path, irrelevant_file_path):
        labels = ['irrelevant', 'valid']
        self.bert_model_filter = None
        if path != '':
            self.bert_model_filter = Bert_model(path, labels)
            self.bert_model_filter.do_eval()

        self.log_file = None
        self.index = 0
        try:
            # get last index from irrelevant tsv file
            irrelevant_file = open(irrelevant_file_path, "r")
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
            # not necessary to archive one word reviews
            # self.log_file.write(sentence + '\n')
            return True
        # too long review sentence is kind of valid
        elif len(sentence.split()) > 15:
            return False

        # evaluate sentence with trained model, if we use one
        if self.bert_model_filter:
            if self.bert_model_filter.eval_example('a', sentence) == 'irrelevant':
                if self.log_file:
                    self.index += 1
                    self.log_file.write('{0}\t1\ta\t{1}\n'.format(str(self.index), sentence))
                return True

        return False

    def __del__(self):
        if self.log_file:
            self.log_file.close()


def main():
    heureka_filter = HeurekaFilter('../model/bert_irelevant')

    with open('tmp/dataset_positive.txt', "r") as file:
        for line in file:
            line = line[:-1]
            label = heureka_filter.is_irelevant(line)
            print(line + '\t' + label)


if __name__ == '__main__':
    main()
