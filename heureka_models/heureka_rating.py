"""
This file contains implementation of class HeurekaRating, which wraps up Bert regression model. It is used as part of
HeurekaCrawler class to predict rating of reviews.

Author: xkloco00@stud.fit.vutbr.cz
"""

import re, sys
sys.path.append('../../')
from clasification.bert_model import Bert_model


class HeurekaRating:
    """
    Class wraps up functionality of Bert regression model. Its main purpose is to evaluate rating of text. Class needs
    model directory structure as described in README.md file.
    """
    def __init__(self, useModel: bool):
        """
        Constructor loads bert model from model directory and switches model to evaluation state.
        :param useModel: option for model usage
        """
        path = '../model/'
        self.regression_model = None

        if useModel:
            self.regression_model = Bert_model(path + 'bert_regression', [])
            self.regression_model.do_eval()

    def __clear_sentence(self, sentence: str) -> str:
        """
        Clear sentence representation for Cased bert model.
        :param sentence:
        :return:
        """
        try:
            # capitalize
            sentence = sentence.strip().capitalize()
            # remove dots with count > 2
            sentence = re.sub(r'\.{2,}', "", sentence)
            # remove tabs
            sentence = re.sub(r'\t+', ' ', sentence)
            # last char is dot
            if sentence[-1] != '.':
                sentence += '.'
        except Exception as e:
            print("[__clear_sentence] Error: " + str(e), file=sys.stderr)

        return sentence

    def eval_sentence(self, sentence: str):
        """
        Perform evaluation of sentence with Bert regression model.
        :param sentence: text
        :return: string percentage representation of rating
        """
        try:
            if self.regression_model:
                sentence = self.__clear_sentence(sentence)

                rating = self.regression_model.eval_example('a', sentence, False)
                rating = self.__round_percentage(rating)

                return '{}%'.format(rating)

            else:
                return ''
        except Exception as e:
            print("[eval_sentence] Error: " + str(e), file=sys.stderr)
            return ''

    def merge_review_text(self, pos: list, con: list, summary: str):
        """
        Merge review sentences to one text string.
        :param pos: positive sentences of review
        :param con: negative sentences of review
        :param summary: review summary
        :return:
        """
        text = []
        text += [self.__clear_sentence(s) for s in pos]
        text += [self.__clear_sentence(s) for s in con]
        text += [summary]
        return ' '.join(text)

    def __round_percentage(self, number):
        """
        Round zero dot percentage to decimal percentage.
        :param number:
        :return:
        """
        return round(round(number * 100.0, -1))
