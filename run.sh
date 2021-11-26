<<<<<<< HEAD
#!/bin/sh
rm -rf test7
echo "Reading webpages ..."
python entity_linking.py <input-file-path> <keyname>
echo "Generating results ..."
mv ./test7/part-00000 test.tsv
rm -rf <output-file-path>
python readid.py ./test7/test.tsv > <output-file-path>

=======
#!/bin/bash

rm -rf temp/

python3 entity_linking.py $1
python3 helper.py $2

echo Result file saved at $2/result.tsv
>>>>>>> c0ea4a7fd9d60d1432feb42bc7e6e4917ff0636a
