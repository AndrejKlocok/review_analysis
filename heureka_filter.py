from clasification.bert_model import Bert_model


class HeurekaFilter:
    def __init__(self, path):
        labels = ['irrelevant', 'valid']
        self.bert_model_filter = Bert_model(path, labels)
        self.bert_model_filter.do_eval()

    def check_sentence(self, sentence):
        return  self.bert_model_filter.eval_example('a', sentence)



def main():
    #heureka_filter = HeurekaFilter('../model/bert_irelevant')

    with open('tmp/dataset_negative.txt', "r") as file:
        i = 0
        for line in file:
            line = line[:-1]
            print(str(i)+'\t'+'1'+'\t'+'a'+'\t'+line)
            i+=1
            #label = heureka_filter.check_sentence(line)
            #print(line + '\t'+ label)


if __name__ == '__main__':
    main()

