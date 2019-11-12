#!/usr/bin/env bash

if [ $# -ne 1 ]; then
    echo "Needs one arg as file name"
    exit
fi
file=$1
Bert_dir="./bg_cs_pl_ru_cased_L-12_H-768_A-12"
Repo="../../../bert"
python3 $Repo/extract_features.py --input_file=$file \
 --output_file=$file.json \
 --vocab_file=$Bert_dir/vocab.txt \
 --bert_config_file=$Bert_dir/bert_config.json \
 --init_checkpoint=$Bert_dir/bert_model.ckpt \
 --layers=-2\
 --max_seq_length=128 \
 --batch_size=8
