import requests
import re
import os
import pandas as pd
import pprint as pp
from pymongo import MongoClient, collection
from random import randint
from time import sleep
from dotenv import load_dotenv
from datetime import datetime
from datetime import timedelta

def main():
    load_dotenv()
    token = os.getenv("TOKEN")
    count = 1000

    df = pd.read_csv('URLs/retweeted_urls_rph_BRA.csv',nrows=5)
    # print(df.head())

    for index,row in df.iterrows():
        print("\nCollectionName: "+ str(row['collection_name']))
        # print("URL Not formatted: "+ str(row['retweeted_url']))

        urlLink = re.match('(.*&)', row['retweeted_url'])
        if urlLink is None:
           urlLink = row['retweeted_url']
        else:
            urlLink = urlLink.group(0).replace("&","")

        url = "https://api.crowdtangle.com/links?token=%s&count=%s&link=%s" % (token,count,urlLink)
        print("URL: " + str(url))

        payload={}
        headers = {}
        action = "insert"

        limit = 0
        endDate = None
        coll_count = getTotalCollection(row['collection_name'])
        if coll_count > 0:
            endDate = findOldestDate(row['collection_name'])
        print(coll_count)
        print(endDate)

        try:
            response = requests.request("GET", url, headers=headers, data=payload)

            if (response.status_code == 200):
                response = response.json()
                posts = response['result']['posts']
                qtd_posts = len(posts)

                print("Posts Collected: "+str(qtd_posts))
                # print(posts)

                # Search by date
                if qtd_posts == 1000:
                    exit_aux = 0
                    end_date_updated = None

                    # Collect in database the data of the last register to continue search

                    while exit_aux < 3:
                        print("------------------")

                        if exit_aux == -1:
                            interval_day = int(interval_day/2)
                            exit_aux = 0
                        else:
                            interval_day = 3

                        if end_date_updated:
                            endDate = end_date_updated
                        elif endDate is not None and limit != 1:
                            try:
                                endDate = endDate.strftime('%Y-%m-%d %H:%M:%S')
                            except:
                                pass
                            startDate = endDate.split(" ")[0] + " 00:00:00"

                            endDate = datetime.strptime(endDate, '%Y-%m-%d %H:%M:%S')
                            startDate = datetime.strptime(startDate, '%Y-%m-%d %H:%M:%S')

                            # executed at least one time
                            if exit_aux != 0:
                                endDate = startDate - timedelta(seconds=1)
                                endDateString = endDate.strftime('%Y-%m-%d %H:%M:%S')
                                endDateString = endDateString.split(" ")[0] + " 00:00:00"
                                startDate = datetime.strptime(endDateString, '%Y-%m-%d %H:%M:%S')
                                startDate = startDate - timedelta(days=interval_day)
                                
                        else:
                            print("Else dos dates")
                            if limit != 1:
                                endDate = posts[0]["date"].split(" ")[0] + " 23:59:59"
                                endDate = datetime.strptime(endDate, '%Y-%m-%d %H:%M:%S')
                            startDate = endDate - timedelta(days=interval_day, hours=23, minutes=59, seconds=59)
                            

                        if not startDate:
                            startDate = endDate - timedelta(days=interval_day, hours=23, minutes=59, seconds=59)

                        print("Start date: " + str(startDate))
                        print("End date: " + str(endDate))
                        print("Days interval: " + str(interval_day))

                        url = "https://api.crowdtangle.com/links?token=%s&count=%s&link=%s&endDate=%s&startDate=%s" % (token, count, urlLink, endDate, startDate)
                        print("URL:" + str(url))
                        freeze()

                        try:
                            response = requests.request("GET", url, headers=headers, data=payload)
                            response = response.json()
                            posts_data = response['result']['posts']
                            qtd_posts = len(posts_data)
                            print("Posts Collected: "+str(qtd_posts))

                            if qtd_posts == 0:
                                exit_aux += 1
                            elif qtd_posts == 1000:
                                limit = 1
                                exit_aux = -1
                            else:
                                limit = 1
                                exit_aux = 0
                                end_date_updated = startDate - timedelta(seconds=1)

                                print("Inserting %s Documents" % (len(posts_data)))
                                insertDocuments(row['collection_name'], posts_data, "update")

                        except:
                            print("ERRO NA REQUISIÇÃO COM DATAS")
                            print(response)
                            exit(0)
                        print("------------------")
                else:
                    print("Inserting %s Documents" % (len(posts)))
                    insertDocuments(row['collection_name'], posts, action)

            else:
                print("ERRO NA REQUISIÇÃO")
                print(response.status_code)
                print(response.text)
                exit(0)

        except Exception as e:
            print("ERRO NO RETORNO")
            print(e)
            print(response.text)
            exit(0)


        freeze()

def freeze():
    # sleepTime = randint(30,34)
    print("sleep:"+str(31))
    sleep(31)


def mongodbConnection(collectionName):
    if collectionName == None:
        print("CollectionName Error")
        exit(666)

    client = MongoClient('mongodb://localhost:27017/')
    db = client.config
    return db[collectionName]


def getTotalCollection(collectionName):
    collection = mongodbConnection(collectionName)
    return collection.count_documents({})


def findOldestDate(collectionName):
    collection = mongodbConnection(collectionName)
    oldest = collection.find().sort("date",1).limit(1)

    for old in oldest:
        return old['date']


def insertDocuments(collectionName, items, action="insert"): # array of jsons [{},{}]
    collection = mongodbConnection(collectionName)
    
    # delete and re-create to avoid duplicates
    if (action == "insert"):
        collection.drop()

    collection.insert_many(items)
    
    # for item in items:
    #     post_id = collection.insert_one(item).inserted_id
    #     print(post_id)

if __name__ == "__main__":
    main()