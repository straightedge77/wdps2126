from bs4 import BeautifulSoup, Comment
from elasticsearch import Elasticsearch
import requests
import json
import sys
import urllib
import math
import sparknlp
from sparknlp.pretrained import PretrainedPipeline
from pyspark.sql.functions import explode
from pyspark.sql.functions import col
import re
import Levenshtein

KEYNAME="WARC-TREC-ID"

##### HTML PROCESSING #####
def record_to_html(record):
    _, record = record

    # find html in warc file
    ishtml = False
    html = ""
    for line in record.splitlines():
        # html starts with <html
        if line.startswith("<html"):
            ishtml = True
        if ishtml:
            html += line
    if not html:
        return

    # key for the output
    key = ''
    for line in record.splitlines():
        if line.startswith(KEYNAME):
            key = line.split(': ')[1]
            break
    if not key:
        return

    yield key, html


##### Convert HTML To Text #####                                 
def html_to_text(record):
    key, html = record
    # remove useless tags
    useless_tags = ['footer', 'header', 'sidebar', 'sidebar-right', 'sidebar-left', 'sidebar-wrapper', 'wrapwidget', 'widget']
    soup = BeautifulSoup(html, "html.parser")
    [s.extract() for s in soup(['script', 'style', 'code', 'title', 'head', 'footer', 'header'])]
    [s.extract() for s in soup.find_all(id=useless_tags)]
    [s.extract() for s in soup.find_all(name='div', attrs={"class": useless_tags})]

    for element in soup(s=lambda s: isinstance(s, Comment)):
        element.extract()

    # use beautifulsoup to find the text
    paragraph = soup.find_all("p")
    text = ""
    for p in paragraph:
        if p.get_text(" ", strip=True) != '':
            text += p.get_text(" ", strip=True)+"\n"
    if text == "":
        text = soup.get_text(" ", strip=True)

    yield key, text

##### Check the entity found out ####
def words_check(record):
    key, name, label, text = record
    name = name.rstrip()
    # only use the certain type of entity
    if(label in ["LOC", "ORG", "PER", "MISC"]):
        # check whether there are entities that can not be put into elasticsearch, or cannot be considered as a qualified entity
        if len(name) > 1 and check_skip_constraints(name):
            yield key, [name, text]

##### Check whether the entity contains the special symbol that can not be put into elasticsearch
def check_skip_constraints(entity):
    entity_skip_symbols = ["\n", "<", ">", "(", ")", "/", ":", "=", "[", "]", "+", "-", "&&", "||", "!", "{", "}", "^", "\"", "~", "*", "?", "\\", ",","“", "”", "_", "&", "|", "@", "NOT", "AND", "OR", "UTF-8", "NoneType"]
    # if have, delete them
    for symbol in entity_skip_symbols:
        if symbol in entity:
            return False

    return True

##### Use the elasticsearch to get the candidate mention of the entity
def get_elasticsearch(record):
    query = record[0]
    # Use the remote server
    e = Elasticsearch("http://fs0.das5.cs.vu.nl:10010/")
    p = { "query" : { "query_string" : { "query" : query }}}
    response = e.search(index="wikidata_en", body=json.dumps(p), request_timeout=1000)
    result = {}
    if response:
        for hit in response['hits']['hits']:
            source = hit['_source']
            # get the id, label, score and description of the entity
            if 'schema_description' in source:
                description = hit['_source']['schema_description']
                freebase_id = hit['_id']
                label = hit.get('_source', {}).get('schema_name')
                score = hit.get('_score', 0)
                # record down the mention's information
                if result.get(freebase_id) is None:
                    result[freebase_id] = ({
                        'label': label,
                        'score': score,
                        'texts': description,
                        'rank': 0,
                        'facts': 0,
                        'similarity': 100000
                    })
                # get the highest score for the mention of entity from elasticsearch
                else:
                    score_1 = max(result[freebase_id]['score'], score)
                    result[freebase_id]['score'] = score_1
    if result:
        yield [record, result]

# use this sparql to find out how many properties the mention have
sparql_query = """
    SELECT DISTINCT * WHERE {
        %s ?p ?o.
    }
    """

HOST = "http://fs0.das5.cs.vu.nl:10011/sparql"

##### Find out the popularity of the mention using the knowledge graph #####
def get_kbdata(record):
    for key in record[1]:
        query = sparql_query % key
        resp = requests.get(HOST + "?" + urllib.parse.urlencode({
            "default-graph": "",
            "should-sponge": "soft",
            "query": query,
            "debug": "on",
            "timeout": "",
            "format": "application/json",
            "save": "display",
            "fname": ""
        }))
        response = json.loads(resp.content.decode("utf-8"))
        if response:
            results = response["results"]
            # n is the number of the properties the mention have
            n = len(results["bindings"])
            record[1][key]['facts'] = n
            # the popularity of the mention is determined by the log(n) * score, since people will usually use the most common meaning the entity
            if n != 0:
                record[1][key]['rank'] = math.log(n) * record[1][key]['score']
            else:
                record[1][key]['rank'] = 0
    yield record

##### Find 4 most popular mentions of the entities #####
def get_bestmatches(record):
    best_matches = {}
    entity = record[0]
    if record[1].items() is not None:
        best_matches = dict(sorted(record[1].items(), key=lambda x: (x[1]['rank']), reverse=True)[:4])
        yield [entity, best_matches]

##### Compute the similarity between the entity and mention ##### 
def compute_similarity(record):
    entity = record[0][0]
    text = record[0][1]
    for key in record[1]:
        description = record[1][key]['texts']
        label = record[1][key]['label']
        # compute Levenshtein distance between the sentence where the entity is extracted from and the description of the mention as well as the Levenshtein distance between the name of the entity and the label of the mention
        if label:
            # the similarity is determined the log of the former distance multiply by the later distance
            # usually the former distance is much larger than the later distance, use the log function can help us consider the impact of both
            record[1][key]['similarity'] = math.log(Levenshtein.distance(text, description)) * Levenshtein.distance(entity, label)
    yield [entity, record[1]]

##### Find the mention that are most similar #####
def get_linkedent(record):
    linked_ent = {}
    linked_ent = dict()
    entity = record[0]
    if record[1].items() is not None:
        # The smaller the similarity value, the more similar they are
        linked_ent = dict(sorted(record[1].items(), key=lambda x: (x[1]['similarity']))[:1])
        yield [entity, linked_ent]

##### Output the result #####
def get_output(record):
    line = ''
    if record:
        if record[1]:
            i = record[1]
            if i[0] and i[1]:
                for key in i[1]:
                    if key:
                        # perform NIL based on the similarity value, if the value is too high, it means they may not be the same thing, so don't output it
                        if i[1][key]['similarity'] < 40:
                            line += record[0] + "\t" + i[0] + "\t" + key
    if len(line) != 0:
        return line    

if __name__ == '__main__':

    import sys
    try:
        _, INPUT,KEYNAME = sys.argv
    except Exception as e:
        print('Usage: python entity_linking.py INPUT KEYNAME')
        sys.exit(0)

    # start the spark using sparknlp
    spark = sparknlp.start()

    # read the warc.gz file into rdd
    rdd = spark.sparkContext.newAPIHadoopFile(INPUT,
        "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
        "org.apache.hadoop.io.LongWritable",
        "org.apache.hadoop.io.Text",
        conf={"textinputformat.record.delimiter": "WARC/1.0"})

    # extract text
    rdd = rdd.flatMap(record_to_html)
    rdd = rdd.flatMap(html_to_text)
    deptColumns = ["id","text"]
    # convert to dataframe since sparknlp apply the model on dataframe
    df = rdd.toDF(deptColumns)
    # use the bert model to do ner
    pipeline = PretrainedPipeline('recognize_entities_bert', lang = 'en')
    # apply the model
    annotation = pipeline.transform(df)
    # the model is also capable of finding each sentence in the text
    df2 = annotation.select(annotation.id, explode(annotation.sentence).alias("sentence"))
    # note down the sentence position for each web page
    df3 = df2.select(df2.id, df2.sentence.result.alias("text"), df2.sentence.metadata["sentence"].alias("pos"))
    # get the entities the model detect
    df4 = annotation.select(annotation.id, explode(annotation.entities).alias("entity"))
    # get the entity's name, label and which sentence it is in
    df5 = df4.select(df4.id, df4.entity.result.alias("name"), df4.entity.metadata["entity"].alias("label"), df4.entity.metadata["sentence"].alias("pos"))
    df6 = df5.join(df3, ["id", "pos"]).select("id", "name", "label", "text").orderBy(col("id").asc())
    # convert back to rdd since dataframe doesn't have map function
    rdd = df6.rdd.map(tuple)
    # get qualified entities
    rdd = rdd.flatMap(words_check)
    # get candidate mention
    rdd = rdd.flatMapValues(get_elasticsearch)
    # disambiguous
    rdd = rdd.flatMapValues(get_kbdata)
    rdd = rdd.flatMapValues(get_bestmatches)
    rdd = rdd.flatMapValues(compute_similarity)
    # link the most suitable mention to entity
    rdd = rdd.flatMapValues(get_linkedent)
    # perform NIL and get output
    result = rdd.map(get_output)
    # save the result
    result = result.coalesce(1,True).saveAsTextFile("temp")
    

