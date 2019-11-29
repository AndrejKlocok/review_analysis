#!/usr/bin/env bash
export DATA_DIR=../bert_bile_zbozi/
export TASK_NAME=bipolar

python transformers_cls.py \
  --model_type bert \
  --model_name_or_path /tmp/xkloco00/multi_cased_L-12_H-768_A-12 \
  --task_name $TASK_NAME \
  --do_train \
  --do_eval \
  --data_dir $DATA_DIR \
  --max_seq_length 128 \
  --per_gpu_train_batch_size 32 \
  --learning_rate 2e-5 \
  --save_steps 10000 \
  --num_train_epochs 6.0 \
  --output_dir $DATA_DIR/out