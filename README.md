# First Assignment - Entity Linking

Web Data Processing System Assignment 1 - 2021 - Group 26
- Zhining Bai
- Bowen lyv
- Tianshi Chen
- Yiming Xu

## Description

This is a Python program to Entity Linking  by processing WARC files. We recognize entities from web pages and link them to a Knowledge Base(Wikidata). The pipeline for this program as below:

![image](https://github.com/straightedge77/wdps_2126/blob/main/imgs/img1.jpg)

### Read WARC
- Use `pyspark` to read large-scale warc files, so the program supports parallel computing.
- Extract text information from HTML files by using `beautifulsoup`.

### Named entity recognition
- Extract entities by using `recognize_entities_bert` model from `sparknlp`.

### Disambiguation and NIL
We considered the popularity of the candidate page as well as the semantic similarity between the sentence where the entity is located and the candidate description to achieve Disambiguation.
- Popularity: Calculate popularity rankings using the `Elasticsearch` scoring algorithm and the number of properties of the mention from the knowledge graph.
- Sentence similarity: Measure the difference between text and description using the `Levenshtein distance`.

NIL: Retain results with distances < 40.

![image](https://github.com/straightedge77/wdps_2126/blob/main/imgs/wdps%20-%202.jpg)

## Prerequisites

Codes are run on the DAS cluster at `/var/scratch/wdps2106/wdps_2126`, `result1` is a conda virtual environment that has been created. Below are the packages installed to run the assignment.

 ```
# if you want to use pip(pip for python3) to install the packages, use the following command(python version 3.8)
pip install pyspark==3.1.2
pip install spark-nlp==3.3.3
pip install beautifulsoup4
pip install python-Levenshtein
pip install elasticsearch

# if you want to use conda to install the packages, use the following command(recommended)
conda create --n <env> python=3.8
conda install pyspark
conda install bs4
conda install elasticsearch
pip install python-Levenshtein
pip install sparknlp
 ```

## Run

To run the program, you can simply use the command below. Be aware that the result file will be renamed as `result.tsv`.

```
sh run.sh /path/to/warc/file.warc.gz /path/to/result/
```

If you use DAS cluster, you also need to add this command before running:
```
export OPENBLAS_NUM_THREADS=10
```

To check the score of the result file, use the command below.

```
python3 score.py /sample/annotation/file/sample.tsv /generated/result/file/result.tsv
```

## Result
We tested our entity linking code using `sample.warc.gz`. Since `sample_annotations.tsv` only contains the entities that page_id is less than 92, our test results only output entity links with page_id <= 92. The f1 score of the sample data is 0.1122.

|  Metric   | Value  |
|  :----:  | :----:  |
|  Gold | 500 |
| Predicted  | 480 |
| Correct  | 55 |
| Precision  | 0.1145 |
| Recall  | 0.11 |
| F1 Score  | 0.1122 |
