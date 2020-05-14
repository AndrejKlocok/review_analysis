# Review analysis
Review analysis project consists of three repositories. This repository is the main part of them. Contains the core part of thesis - crawlers, scripts to train models, clustering implementation, elastic connections.

Repository [Review analysis-front end](https://github.com/AndrejKlocok/review_analysis-frontend) provides API for front end application,   
Repository [Review analysis-back end](https://github.com/AndrejKlocok/review_analysis-backend) provides API for back end application,   

There are more information about these projects in theirs README.md file, how to execute servers on both sides. 

# Project setup
Project structure needs to follow schema of working directory 
```
├── review_analysis
├── review analysis-backend
├── review analysis-front-end
├── model
|    ├── bert_bipolar
|    ├── bert_bipolar_domain
|    |   ├── auto-moto
|    |   ├── bile_zbozi
|    |   ├── chovatelstvi
|    |   ├── detske_zbozi
|    |   ├── dum_a_zahrada
|    |   ├── elektronika
|    |   ├── filmy_knihy_hry
|    |   ├── hobby
|    |   ├── jidlo_a_napoje
|    |   ├── kosmetika_a_zdravi
|    |   ├── obleceni_a_moda
|    |   ├── sexualni_a_eroticke_pomucky
|    |   ├── sport
|    |   └── stavebniny
|    ├── bert_regression
|    ├── fasttext
|    ├── fse
|    ├── SVM
|    ├── czech-morfflex-pdt-161115-no_dia-pos_only.tagger
|    ├── embeddings.txt
|    └── irrelevant.tsv
└── elasticsearch-backup
```
Use these commands to download review analysis repositories:

        git clone https://github.com/AndrejKlocok/review_analysis.git
        git clone https://github.com/AndrejKlocok/review_analysis-frontend.git
        git clone https://github.com/AndrejKlocok/review_analysis-backend.git

## Python dependencies
Python dependencies are located in requirements.txt file. Project Review analysis uses virtual environment. It was developed under Python Python 3.6.9.

Init virtual environment with these commands in working directory.

       
    virtualenv -p python3 review_analysis_env
    source review_analysis_env/bin/activate

Then install python dependencies with pip tool:

    pip install -r /path/to/requirements.txt

## Elastic-search
Project uses elasticsearch. Version of elastic is [Elasticsearch 7.6.2](https://www.elastic.co/downloads/past-releases/elasticsearch-7-6-2). Project uses elasti on localhost:

    localhost:9200

Last snapshot of indexes is located in elasticsearch-backup (on working directory). To run elastic simply execute

    /path/to/elastic/bin/elasticsearch

Or as a daemon with command:
    /path/to/elastic/bin/elasticsearch -d -p pid
## Models
Models are available right now only in working setup directory. Base model for bipolar classification of sentiment can be downloaded from google:

    wget https://storage.googleapis.com/bert_models/2018_11_23/multi_cased_L-12_H-768_A-12.zip

## Working setup
Complete project setup is located on:

    pcknot5.fit.vutbr.cz 

In directory:
    
    /mnt/data/xkloco00_pc5/

Also there is virtual environment with all requirements installed. It is located here:

    /mnt/data/xkloco00_pc5/review_analysis_env

Elastic search is located in directory:

    /mnt/data/xkloco00_pc5/elasticsearch-7.6.2/

# Crawlers
## Index products
To initiate heureka database, firstly there the indexing script needs to be executed. It crawls through heureka and indexes products, which have at least one review. It creates txt files with URLs of products in current working directory
    
    python3 heureka_index.py 

Output example is located in pcknot5 directory:
    
    /mnt/data/xkloco00_pc5/crawl_index_output/

## Index reviews
After product indexing is complete execute heureka crawler script with arguments:

    python3 heureka_crawler.py -crawl -path /path/to/indexed_product_directory/

Crawling and indexing will start for each domain category products.

## Index shop reviews
Indexing of shop reviews is handled with simple execution of script (optional -rating -filter). It will index shop reviews for given shop until it finds existing review in elastic, then it continues with next shop page.

    python3 heureka_crawler.py -shop

## Automatic actualization
Automatic actualization of data is handled by heureka_crawler script. There is crawler.sh script, that can be executed with cron once a week. Script executes this command:

    python3 heureka_crawler.py -actualize -rating -filter > /path/to/output/file

Actualization is based on crawling heureka product reviews, until crawler finds out existing review in elastic, then it continues to another product.

## Product review repair
Some products does not have all its reviews crawled. For this example execute with optional (-rating -filter) this command:

    python3 heureka_crawler.py -repair number_of_minimum_reviews

It will crawl all products, which have les reviews then number_of_minimum_reviews. The reviews, that are already in elastic wil lbe skipped.

# Model training
After review crawling the model training can begin. Using [Review analysis-front end](https://github.com/AndrejKlocok/review_analysis-frontend) and its generation dataset feature or directly script:

    python3 utils/generate_dataset.py -h

After generating dataset for bert model, use bash script for training bert model.

    ./clasification/run_cls.sh

Script for performance evaluation:

    ./clasification/run_cls_pred.sh

There are two variables defining model data directory (DATA_DIR with dev.tsv, train.tsv files) and the name of the task TASK_NAME (bipolar/rating_regression)

    export DATA_DIR=../bert_mall_polarity_s2/
    export TASK_NAME=bipolar

To transform bert model from tensorflow to pytorch use script:
    
    python3 clasification/tf_to_torch.py
