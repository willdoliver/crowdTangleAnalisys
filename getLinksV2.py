import requests
import re
import os
import pandas as pd
import pprint as pp
import argparse
from pymongo import MongoClient, collection
from random import randint
from time import sleep
from dotenv import load_dotenv
from datetime import datetime
from datetime import timedelta

def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description='Description of the script')
    parser.add_argument('-mode','--mode', help='insert or update database', required=True)
    parser.add_argument('-collection','--collection', help='[all/collection_n]', required=True)
    args = vars(parser.parse_args())

    mode = args['mode']
    collection = args['collection']

    # Links to search
    df = pd.read_csv('URLs/retweeted_urls_rph_BRA.csv',nrows=5)
    # print(df.head())

    if collection != 'all':
        df = df[df['collection_name'] == collection]

    for index,row in df.iterrows():
        print("\nCollectionName: "+ str(row['collection_name']))
        # print("URL Not formatted: "+ str(row['retweeted_url']))

        urlLink = treatLinkToSearch(row['retweeted_url'])
        
        maxEmptyResults = 3
        
        if mode == 'update':
            endDate = findOldestDate(row['collection_name'])
            if endDate is None:
                mode = 'insert'
                pass

            startDate, endDate = calculateInterval(None, endDate)
            posts = getDatedResponse(startDate, endDate, urlLink, mode)

            qtd_posts = len(posts)
            print("Posts Collected: "+str(qtd_posts))

            if qtd_posts == 1000:
                startDate, endDate = calculateInterval(None, endDate)
                posts += getDatedResponse(startDate, endDate, urlLink, mode)


        if mode == 'insert':
            posts = getInitialRequest(urlLink)
            qtd_posts = len(posts)
            print("Posts Collected: "+str(qtd_posts))

            if qtd_posts == 1000:
                # search by date parts
                endDate = posts[0]["date"].split(" ")[0] + " 23:59:59"
                startDate = posts[0]["date"].split(" ")[0] + " 00:00:00"
                posts = getDatedResponse(startDate, endDate, urlLink, mode)

        # loop
        emptyLoop = 0
        while True:
            qtd_posts = len(posts)

            if qtd_posts == 1000:
                startDate, endDate = calculateInterval(None, endDate)
                posts = getDatedResponse(startDate, endDate, urlLink, mode)

            if qtd_posts == 0:
                emptyLoop+=1

                if emptyLoop >= maxEmptyResults:
                    break

        #insert in database
        print("Inserting %s Documents" % (len(posts)))
        insertDocuments(row['collection_name'], posts, mode)
        
        




def calculateInterval(startDate, endDate):
    if startDate is None:
        interval = timedelta(days=1)
    else:
        interval = endDate - startDate

    minInterval = timedelta(hours=1, minutes=00, seconds=00)
    if interval < minInterval:
        print("Min interval accepted reached")
        exit(0)

    startDate = startDate + (interval/2)
    print("Interval before: " + str(interval))
    print("Interval after: " + str(interval/2))
    print(startDate)
    print(endDate)

    return startDate, endDate

def getDatedResponse(startDate, endDate, link, mode):
    url = "https://api.crowdtangle.com/links?token=%s&count=%s&link=%s&endDate=%s&startDate=%s" % (os.getenv('TOKEN'), 1000, link, endDate, startDate)
    print("URL:" + str(url))

    payload={}
    headers = {}

    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        if (response.status_code == 200):
            response = response.json()
            return response['result']['posts']
        
    except Exception as e:
        print("erro na requisicao")
        print(e)


def getInitialRequest(link):
    url = "https://api.crowdtangle.com/links?token=%s&count=%s&link=%s" % (os.getenv('TOKEN'), 1000, link)
    print("URL: " + str(url))

    payload={}
    headers = {}

    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        if (response.status_code == 200):
            response = response.json()
            return response['result']['posts']
        
    except Exception as e:
        print("erro na requisicao")
        print(e)

def treatLinkToSearch(link):
    urlLink = re.match('(.*&)', link)
    if urlLink is None:
        urlLink = link
    else:
        urlLink = urlLink.group(0).replace("&","")

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
    count = collection.count_documents({})

    try:
        if count > 0:
            oldest = collection.find().sort("date",1).limit(1)
            for old in oldest:
                return datetime.strptime(old['date'], '%Y-%m-%d %H:%M:%S')
        else:
            return None
    except Exception as e:
        print("Oldest date not found for collection")
        return None


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