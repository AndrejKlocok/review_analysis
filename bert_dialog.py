import torch
import argparse

from torch.utils.data import (DataLoader, RandomSampler, SequentialSampler,
                              TensorDataset)
from clasification.procesors_cls import  InputExample, convert_to_features
from pytorch_transformers import BertForSequenceClassification,BertTokenizer,BertConfig
from bertviz.bertviz.neuron_view import show
import numpy as np
from clasification.bert_model import Bert_model



def call_html():
    from IPython.display import display
    from IPython import core
    display(core.display.HTML('''
        <script src="/static/components/requirejs/require.js"></script>
        <script>
          requirejs.config({
            paths: {
              base: '/static/base',
              "d3": "https://cdnjs.cloudflare.com/ajax/libs/d3/5.7.0/d3.min",
              jquery: '//ajax.googleapis.com/ajax/libs/jquery/2.0.0/jquery.min',
            },
          });
        </script>
        '''))


class BertVizModel():
    def __init__(self, model_path):
        from bertviz.bertviz.transformers_neuron_view import BertModel, BertTokenizer

        self.model_type = 'bert'
        bert_version = 'bert-base-cased'
        do_lower_case = False
        self.model = BertModel.from_pretrained(model_path)
        self.tokenizer = BertTokenizer.from_pretrained(bert_version, do_lower_case=do_lower_case)

    def show(self, sentence_a, sentence_b):
        show(self.model, self.model_type, self.tokenizer, sentence_a, sentence_b)


def main():
    labels = ["pozitivní", "negativní"]
    path = '../model/bert_bipolar'
    #parser = argparse.ArgumentParser(
    #    description="Scrip visualizes attention")
    #parser.add_argument('-path', '--model_path', help='Path to bert model', required=True)

    #args = vars(parser.parse_args())

    bert_viz = BertVizModel(path)
    bert_infer = Bert_model(path, labels)
    sentence = 'Výborní cena'
    print(" Prediction : {} \n\n".format(bert_infer.eval_example(sentence, 'a')))
    call_html()
    bert_viz.show(sentence, 'a')



if __name__ == '__main__':
    main()