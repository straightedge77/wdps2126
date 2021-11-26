##### this file is used for remove the record that cannot be used to calculate the score
import re
import sys

file = sys.argv[1]

gold = {}
for line in open(file):
    result = line.strip().split('\t')
    if len(result) == 3:
        record = result[0]
        string = result[1]
        key = result[2]
        print(record + '\t' + string + '\t' + key)

