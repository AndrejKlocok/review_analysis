#!/usr/bin/env bash
git clone https://github.com/AndrejKlocok/review_analysis.git

virtualenv -p python3 review_analysis_env

source review_analysis_env/bin/activate

wget https://storage.googleapis.com/bert_models/2018_11_23/multi_cased_L-12_H-768_A-12.zip
