#!/bin/sh
rm -rf test7
echo "Reading webpages ..."
python entity_linking.py sample.warc.gz WARC-TREC-ID
echo "Generating results ..."
mv ./test7/part-00000 test.tsv
rm -rf result.tsv
python readid.py ./test7/test.tsv > result.tsv

