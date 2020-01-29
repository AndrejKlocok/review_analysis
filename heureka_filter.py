from clasification.bert_model import Bert_model


class HeurekaFilter:
    def __init__(self, path, log = ''):
        labels = ['irrelevant', 'valid']
        self.bert_model_filter = Bert_model(path, labels)
        self.bert_model_filter.do_eval()
        self.log_file = None
        if log:
            try:
                self.log_file = open(log, "w")

            except Exception as e:
                print('[HeurekaFilter] Exception: '+str(e))
                self.log_file = None

    def is_irrelevant(self, sentence):
        # one word senteces are irrelevant
        if len(sentence.split()) <= 1:
            self.log_file.write(sentence + '\n')
            return True

        # evaluate sentence with trained model
        if self.bert_model_filter.eval_example('a', sentence)  == 'irrelevant':
            if self.log_file:
                self.log_file.write(sentence+'\n')
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
            print(line + '\t'+ label)


if __name__ == '__main__':
    main()

