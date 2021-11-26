#!/bin/bash

rm -rf temp/

python3 entity_linking.py $1
python3 helper.py $2

echo Result file saved at $2/result.tsv
