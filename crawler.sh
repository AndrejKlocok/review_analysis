#!/usr/bin/env bash

log_file=$(date +%F)'_logs.txt'
crawler=/mnt/data/xkloco00_pc5/review_analysis/heureka_crawler.py


python3 $crawler -actualize -rating -filter > $log_file
