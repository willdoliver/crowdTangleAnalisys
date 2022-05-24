import re
import os
from time import sleep

def main():

    path = "/home/willdoliver/Documents/Databases/crowdtangle"
    collectionsToInsert = [f for f in os.listdir(path) if re.match(r'.*\.bson', f)]
    if (len(collectionsToInsert) == 0):
        print("No '*.bson' files found in directory!")
        exit(0)

    for item in sorted(collectionsToInsert):
        item = item.replace(".bson", "")
        # number = int(item.split("_")[1])
        # if (number < 1192):
        #     continue
        command = "mongorestore --nsInclude=crowdtangle.%s %s/%s.bson --drop --batchSize=100" % (item, path, item)
        os.system(command)
        print(command)
        sleep(2)


if __name__ == "__main__":
    main()