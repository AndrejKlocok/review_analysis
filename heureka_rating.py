import re
from clasification.bert_model import Bert_model


class HeurekaRating:
    def __init__(self, useModel: bool):
        path = '/home/andrej/Documents/school/Diplomka/model/'
        # path = '/mnt/data/xkloco00_a18/model/'
        self.regression_model = None

        if useModel:
            self.regression_model = Bert_model(path + 'bert_regression', [])
            self.regression_model.do_eval()

    def __clear_sentence(self, sentence: str) -> str:
        sentence = sentence.strip().capitalize()
        sentence = re.sub(r'\.{2,}', "", sentence)
        sentence = re.sub(r'\t+', ' ', sentence)
        if sentence[-1] != '.':
            sentence += '.'

        return sentence

    def eval_sentence(self, sentence: str):
        if self.regression_model:
            sentence = self.__clear_sentence(sentence)

            rating = self.regression_model.eval_example('a', sentence, False)
            rating = self.__round_percentage(rating)

            return '{}%'.format(rating)

        else:
            return ''

    def merge_review_text(self, pos: list, con: list, summary: str):
        text = []
        text += [self.__clear_sentence(s) for s in pos]
        text += [self.__clear_sentence(s) for s in con]
        text += [summary]
        return ' '.join(text)

    def __round_percentage(self, number):
        return round(round(number * 100.0, -1))
