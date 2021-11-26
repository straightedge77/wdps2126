#!/bin/bash

rm -rf temp/

echo Processing WARC file...
python3 entity_linking.py $1
echo Generating result file.
python3 helper.py $2

echo Result file saved at $2/result.tsv
