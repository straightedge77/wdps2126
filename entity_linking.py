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
        if line.startswith("WARC-TREC-ID"):
            key = line.split(': ')[1]
            break
    if not key:
        return
    
    yield key, html


def html_to_text(record):
    key, html = record

    useless_tags = ['footer', 'header', 'sidebar', 'sidebar-right', 'sidebar-left', 'sidebar-wrapper', 'wrapwidget', 'widget']
    soup = BeautifulSoup(html, "html.parser")
    [s.extract() for s in soup(['script', 'style', 'code', 'title', 'head', 'footer', 'header'])]
    [s.extract() for s in soup.find_all(id=useless_tags)]
    [s.extract() for s in soup.find_all(name='div', attrs={"class": useless_tags})]

    for element in soup(s=lambda s: isinstance(s, Comment)):
        element.extract()

    paragraph = soup.find_all("p")
    text = ""
    for p in paragraph:
        if p.get_text(" ", strip=True) != '':
            text += p.get_text(" ", strip=True)+"\n"
    if text == "":
        text = soup.get_text(" ", strip=True)

    yield key, text

def words_check(record):
    key, name, label, text = record
    name = name.rstrip()
    if(label in ["LOC", "ORG", "PER", "MISC"]):
        if len(name) > 1 and check_skip_constraints(name):
            yield key, [name, text]

def check_skip_constraints(entity):
    entity_skip_symbols = ["\n", "<", ">", "(", ")", "/", ":", "=", "[", "]", "+", "-", "&&", "||", "!", "{", "}", "^", "\"", "~", "*", "?", "\\", ",","“", "”", "_", "&", "|", "@", "NOT", "AND", "OR", "UTF-8", "NoneType"]
    for symbol in entity_skip_symbols:
        if symbol in entity:
            return False

    return True

def get_elasticsearch(record):
    query = record[0]
    e = Elasticsearch("http://fs0.das5.cs.vu.nl:10010/")
    p = { "query" : { "query_string" : { "query" : query }}}
    response = e.search(index="wikidata_en", body=json.dumps(p), request_timeout=1000)
    result = {}
    if response:
        for hit in response['hits']['hits']:
            source = hit['_source']
            if 'schema_description' in source:
                description = hit['_source']['schema_description']
                freebase_id = hit['_id']
                label = hit.get('_source', {}).get('schema_name')
                score = hit.get('_score', 0)
                if result.get(freebase_id) is None:
                    result[freebase_id] = ({
                        'label': label,
                        'score': score,
                        'texts': description,
                        'rank': 0,
                        'facts': 0,
                        'similarity': 100000
                    })
                else:
                    score_1 = max(result[freebase_id]['score'], score)
                    result[freebase_id]['score'] = score_1
    if result:
        yield [record, result]

sparql_query = """                                                               SELECT DISTINCT * WHERE {                                                        %s ?p ?o.                                                                }                                                                            """

HOST = "http://fs0.das5.cs.vu.nl:10011/sparql"

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
            n = len(results["bindings"])
            record[1][key]['facts'] = n
            if n != 0:
                record[1][key]['rank'] = math.log(n) * record[1][key]['score']
            else:
                record[1][key]['rank'] = 0
    yield record

def get_bestmatches(record):
    best_matches = {}
    entity = record[0]
    if record[1].items() is not None:
        best_matches = dict(sorted(record[1].items(), key=lambda x: (x[1]['rank']), reverse=True)[:4])
        yield [entity, best_matches]


def compute_similarity(record):
    entity = record[0][0]
    text = record[0][1]
    for key in record[1]:
        description = record[1][key]['texts']
        label = record[1][key]['label']
        if label:
            record[1][key]['similarity'] = math.log(Levenshtein.distance(text, description)) * Levenshtein.distance(entity, label)
    yield [entity, record[1]]

def get_linkedent(record):
    linked_ent = {}
#    tuples = []
#    for i in record:
    linked_ent = dict()
    entity = record[0]
    if record[1].items() is not None:
        linked_ent = dict(sorted(record[1].items(), key=lambda x: (x[1]['similarity']))[:1])
#        tuples.append([entity, linked_ent])
        yield [entity, linked_ent]

def get_output(record):
    line = ''
    if record:
        if record[1]:
            i = record[1]
            if i[0] and i[1]:
                for key in i[1]:
                    if key:
                        if i[1][key]['similarity'] < 40:
                            line += record[0] + "\t" + i[0] + "\t" + key
    if len(line) != 0:
        return line    

if __name__ == '__main__':

    import sys
    try:
        _, INPUT = sys.argv
    except Exception as e:
        print('Usage: python entity_linking.py INPUT')
        sys.exit(0)

    spark = sparknlp.start()

    rdd = spark.sparkContext.newAPIHadoopFile(INPUT,
        "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
        "org.apache.hadoop.io.LongWritable",
        "org.apache.hadoop.io.Text",
        conf={"textinputformat.record.delimiter": "WARC/1.0"})

    rdd = rdd.flatMap(record_to_html)
    rdd = rdd.flatMap(html_to_text)
#    print(rdd.take(10))
    deptColumns = ["id","text"]
    df = rdd.toDF(deptColumns)
    pipeline = PretrainedPipeline('recognize_entities_bert', lang = 'en')
    annotation = pipeline.transform(df)
    df2 = annotation.select(annotation.id, explode(annotation.sentence).alias("sentence"))
    df3 = df2.select(df2.id, df2.sentence.result.alias("text"), df2.sentence.metadata["sentence"].alias("pos"))
    df4 = annotation.select(annotation.id, explode(annotation.entities).alias("entity"))
    df5 = df4.select(df4.id, df4.entity.result.alias("name"), df4.entity.metadata["entity"].alias("label"), df4.entity.metadata["sentence"].alias("pos"))
    df6 = df5.join(df3, ["id", "pos"]).select("id", "name", "label", "text").orderBy(col("id").asc())
    rdd = df6.rdd.map(tuple)
    rdd = rdd.flatMap(words_check)
    rdd = rdd.flatMapValues(get_elasticsearch)
    rdd = rdd.flatMapValues(get_kbdata)
    rdd = rdd.flatMapValues(get_bestmatches)
    rdd = rdd.flatMapValues(compute_similarity)
    rdd = rdd.flatMapValues(get_linkedent)
    result = rdd.map(get_output)
#    print(result.take(20))
    result = result.coalesce(1,True).saveAsTextFile("./temp")

    

