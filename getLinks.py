import requests
import re
import os
import pandas as pd
from pymongo import MongoClient
from random import randint
from time import sleep


def main():
    token = os.getenv("TOKEN")
    # collections with 1998 registers need to collect more data
    count = 999

    df = pd.read_csv('URLs/retweeted_urls_rph_BRA.csv',nrows=5)
    # print(df.head())
    for index,row in df.iterrows():
        print("\nCollectionName: "+ str(row['collection_name']))
        # print("URL Not formatted: "+ str(row['retweeted_url']))

        urlLink = re.match('(.*\?)', row['retweeted_url'])
        url = "https://api.crowdtangle.com/links?token=%s&count=%s&link=%s&sortBy=total_interactions" % (token,count,urlLink.group(0))
        print("URL: " + str(url))

        payload={}
        headers = {}
        linkToCollect = 1
        aux = linkToCollect
        action = "insert"

        while linkToCollect and aux <3:
            response = requests.request("GET", url, headers=headers, data=payload)

            if (response.status_code == 200):
                try:
                    response = response.json()
                    posts = response['result']['posts']
                    print("Posts Collected:"+str(len(posts)))
                    # print(posts)

                    print("Inserting Documents")
                    insertDocuments(row['collection_name'], posts, action)

                    try:
                        if (response['result']['pagination']['nextPage'] != None):
                            url = response['result']["pagination"]["nextPage"]
                            print("Url for next page: "+str(url))
                            action = "update"
                    except:
                        linkToCollect = 0
                    
                    aux+=1

                except:
                    print("ERRO NO RETORNO")
                    print(response.text)

            else:
                print("ERRO NA REQUISIÇÃO")
                print(response.status_code)
                print(response.text)
                exit(0)

            sleepTime = randint(31,40)
            print("sleep:"+str(sleepTime))
            sleep(sleepTime)


def mongodbConnection(collectionName):
    if collectionName == None:
        print("CollectionName Error")
        exit(666)

    client = MongoClient('mongodb://localhost:27017/')
    db = client.config
    return db[collectionName]

def insertDocuments(collectionName, items, action="insert"): # array of jsons [{},{}]
    collection = mongodbConnection(collectionName)
    
    # delete and re-create to avoid duplicated
    if (action == "insert"):
        collection.drop()

    collection.insert_many(items)
    
    # for item in items:
    #     post_id = collection.insert_one(item).inserted_id
    #     print(post_id)

if __name__ == "__main__":
    main()