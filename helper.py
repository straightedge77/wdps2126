import sys

if __name__ == "__main__":
    output = sys.argv[1]
    folder = "temp"
    lines = []
    with open(folder + "/part-00000") as rf:
        lines = rf.readlines()
    with open("./" + output + ".tsv", "w+") as wf:
        for line in lines:
            if line == "None\n":
                continue
            else:
                wf.write(line)

