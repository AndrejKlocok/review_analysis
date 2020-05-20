"""
This file contains implementation of class Bert_model, which is wraps needed functionality of bert model for text
evaluation.

Author: xkloco00@stud.fit.vutbr.cz
"""
import sys
sys.path.append('../')
from review_analysis.clasification.procesors_cls import InputExample, convert_to_features
from transformers import BertForSequenceClassification, BertTokenizer, BertConfig
import numpy as np
import torch


class Bert_model:
    """
    Class represents wrapper for bert model, that is used in review analysis.
    """
    def __init__(self, path, labels):
        model_class = BertForSequenceClassification
        self.model = model_class.from_pretrained(path)
        self.tokenizer = BertTokenizer.from_pretrained(path)
        self.config = BertConfig.from_pretrained(path)
        self.labels = labels

    def do_eval(self):
        """
        Set model to evaluation status.
        :return:
        """
        self.model.eval()

    def eval_example(self, sentence_a, sentence_b, useLabels= True):
        """
        Evaluate sentence a and b with fine tuned bert model. In bipolar classification task sentence_a is not used
        and can be replaced with any string, f.e. 'a'. In next sentence prediction tasks sentence_a is used as first
        sentence, based on which model predicts if sentence_b could be next sentence
        :param sentence_a: sentence
        :param sentence_b: sentence
        :param useLabels: in regression task, we want models estimate.
        :return:
        """
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
        """
        Get sentence embedding from last hidden state.
        :param sentence: text
        :param strategy:
        :return:
        """
        input_ids = torch.tensor(self.tokenizer.encode(sentence)).unsqueeze(0)
        outputs = self.model(input_ids)
        last_hidden_states = outputs[0]
        return  last_hidden_states

