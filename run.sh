#!/bin/sh
rm -rf test7
echo "Reading webpages ..."
python entity_linking.py <input-file-path> <keyname>
echo "Generating results ..."
mv ./test7/part-00000 test.tsv
rm -rf <output-file-path>
python readid.py test.tsv > <output-file-path>

