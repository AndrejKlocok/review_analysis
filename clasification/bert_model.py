import sys
sys.path.append('../')
from review_analysis.clasification.procesors_cls import InputExample, convert_to_features
from transformers import BertForSequenceClassification, BertTokenizer, BertConfig
import numpy as np
import torch


class Bert_model:
    def __init__(self, path, labels):
        model_class = BertForSequenceClassification
        self.model = model_class.from_pretrained(path)
        self.tokenizer = BertTokenizer.from_pretrained(path)
        self.config = BertConfig.from_pretrained(path)
        self.labels = labels

    def do_eval(self):
        self.model.eval()

    def eval_example(self, sentence_a, sentence_b, useLabels= True):
        examples = [InputExample(guid=0, text_a=sentence_a, text_b=sentence_b, label=None)]
        inputs, _, _ = convert_to_features(examples, self.tokenizer)
        ouputs = self.model(**inputs)
        logits = ouputs[:2][0]
        preds = logits.detach().cpu().numpy()
        if useLabels:
            preds = np.argmax(preds, axis=1)
            return self.labels[preds[0]]
        else:
            return preds.view().item()

    def get_embedding(self, sentence, strategy=None):
        input_ids = torch.tensor(self.tokenizer.encode(sentence)).unsqueeze(0)
        outputs = self.model(input_ids)
        last_hidden_states = outputs[0]
        return  last_hidden_states


def main():
    labels = []
    path = '/home/andrej/Documents/school/Diplomka/model/bert_regression'
    bert_model = Bert_model(path, labels)
    bert_model.do_eval()

    sentences = ['Tento výrobek nedoporučuji. loupe se z něj barva a celkově je to nepoužitelný výrobek.',
                 'Výborní cena a kvalita produktu, vážne jsem s ním spokojena',
                 'Nesnáším tenhle produkt', 'Je moc hluční, malý', 'Neplíbí se mi',
                 'Výrobek máme krátce.', 'Vse ok.']

    for sentence in sentences:
        print(" Prediction : {} \n\n".format(bert_model.eval_example('a', sentence, useLabels=False)))

    #emb = bert_model.get_embedding(sentence)
    #print(emb)


if __name__ == '__main__':
    main()
