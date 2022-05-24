import pandas as pd
from pandas.core.frame import DataFrame
from pymongo import MongoClient
import pprint as pp
import networkx as nx
import numpy as np
import matplotlib.pyplot as plt
# from nltk import word_tokenize, sent_tokenize
# from nltk.corpus import stopwords
# from nltk.stem import PorterStemmer
# from nltk import pos_tag

def main():
    collections = checkCollections()
    createGraphNodesCollections(collections)
    # createGraphNodesAccounts(collections)

class startProj():
    def textTreatment(df):
        dfTitle = df.groupby(["title"])["title"].count()
        dfDesc = df.groupby(["description"])["description"].count()
        pp.pprint(dfTitle)
        pp.pprint(dfDesc)

        print("\ntratamento de texto...")
        # https://ichi.pro/pt/introducao-ao-nltk-para-nlp-com-python-9335755705822
        stop_words_pt = stopwords.words('portuguese')
        stop_words_pt.append("''")
        stop_words_pt.append("``")
        stop_words_en = stopwords.words('english')

        bag_of_words = []
        for title in dfTitle.index:
            for word in word_tokenize(title):
                bag_of_words.append(word)

        cleaned_token = []
        for word in bag_of_words:
            if word not in stop_words_pt and word not in stop_words_en:
                cleaned_token.append(word)

        # pp.pprint(stop_words_pt)
        # pp.pprint(stop_words_en)
        pp.pprint("This is the unclean version:")
        pp.pprint(bag_of_words)
        pp.pprint("This is the cleaned version:")
        pp.pprint(cleaned_token)

        stemmer = PorterStemmer()
        stemmed = [stemmer.stem(word) for word in cleaned_token]
        print(stemmed)

        # Type of the word (Verb, Preposition...)
        tagged = pos_tag(cleaned_token)                 
        print(tagged)


    def addNodeAttributes(self, G, collectionName, df):
        platform = dict(df.groupby(["platform"])["platform"].count())
        type = dict(df.groupby(["type"])["type"].count())

        for k, v in platform.items():
            platform[k] = float(v)

        for k, v in type.items():
            type[k] = float(v)

        # nx.set_node_attributes(G, {collectionName: {"platform": platform}})
        # nx.set_node_attributes(G, {collectionName: {"type": type}})
        nx.set_node_attributes(G, {collectionName: {"meanSubscriberCount": round(df["subscriberCount"].mean(),2)}})
        nx.set_node_attributes(G, {collectionName: {"meanScore": round(df["score"].mean(),2)}})
        nx.set_node_attributes(G, {collectionName: {"medianSubscriberCount": round(df["subscriberCount"].median(),2)}})
        nx.set_node_attributes(G, {collectionName: {"medianScore": round(df["score"].median(),2)}})

        return G


    def statisticsCalculate(df, data):
        # General data
        data['itemsCount']                     = len(df)
        data['platform']                       = dict(df.groupby(["platform"])["platform"].count())
        data['type']                           = dict(df.groupby(["type"])["type"].count())
        data['pageCategory']                   = dict(df.groupby(["account.pageCategory"])["account.pageCategory"].count())
        data['meanPostSubscriberCount']        = round(df["subscriberCount"].mean(),2)
        data['medianPostSubscriberCount']      = round(df["subscriberCount"].median(),2)
        data['meanAccountSubscriberCount']     = round(df['account.subscriberCount'].mean(),2)
        data['medianAccountSubscriberCount']   = round(df['account.subscriberCount'].median(),2)
        # Likes count
        data['meanScore']           = round(df["score"].mean(),2)
        data['medianScore']         = round(df['score'].median(),2)
        data['meanActualLike']      = round(df["statistics.actual.likeCount"].mean(),2)
        data['meanExpectedLike']    = round(df["statistics.expected.likeCount"].mean(),2)
        data['meanActualShare']     = round(df["statistics.actual.shareCount"].mean(),2)
        data['meanExpectedShare']   = round(df["statistics.expected.shareCount"].mean(),2)
        data['meanActualComment']   = round(df["statistics.actual.commentCount"].mean(),2)
        data['meanExpectedComment'] = round(df["statistics.expected.commentCount"].mean(),2)
        # Reactions
        data['meanActualLove']          = round(df["statistics.actual.loveCount"].mean(),2)
        data['meanExpectedLove']        = round(df["statistics.expected.loveCount"].mean(),2)
        data['meanActualWow']           = round(df["statistics.actual.wowCount"].mean(),2)
        data['meanExpectedWow']         = round(df["statistics.expected.wowCount"].mean(),2)
        data['meanActualHaha']          = round(df["statistics.actual.hahaCount"].mean(),2)
        data['meanExpectedHaha']        = round(df["statistics.expected.hahaCount"].mean(),2)
        data['meanActualSad']           = round(df["statistics.actual.sadCount"].mean(),2)
        data['meanExpectedSad']         = round(df["statistics.expected.sadCount"].mean(),2)
        data['meanActualAngry']         = round(df["statistics.actual.angryCount"].mean(),2)
        data['meanExpectedAngry']       = round(df["statistics.expected.angryCount"].mean(),2)
        data['meanActualThankful']      = round(df["statistics.actual.thankfulCount"].mean(),2)
        data['meanExpectedThankful']    = round(df["statistics.expected.thankfulCount"].mean(),2)
        data['meanActualCare']          = round(df["statistics.actual.careCount"].mean(),2)
        data['meanExpectedCare']        = round(df["statistics.expected.careCount"].mean(),2)

        pp.pprint("Count: %s" % (df.shape[0]))

        pp.pprint("Means:")
        pp.pprint("Score: %s" % (data['meanScore']))
        pp.pprint("Actual Like: %s" % (data['meanActualLike']))
        pp.pprint("Expected Like: %s" % (data['meanExpectedLike']))
        pp.pprint("Actual Share: %s" % (data['meanActualShare']))
        pp.pprint("Expected Share: %s" % (data['meanExpectedShare']))
        pp.pprint("Actual Comments: %s" % (data['meanActualComment']))
        pp.pprint("Expected Comments: %s" % (data['meanExpectedComment']))
        # Reactions
        pp.pprint("Actual Love: %s" % (data['meanActualLove']))
        pp.pprint("Expected Love: %s" % (data['meanExpectedLove']))
        pp.pprint("Actual Wow: %s" % (data['meanActualWow']))
        pp.pprint("Expected Wow: %s" % (data['meanExpectedWow']))
        pp.pprint("Actual Haha: %s" % (data['meanActualHaha']))
        pp.pprint("Expected Haha: %s" % (data['meanExpectedHaha']))
        pp.pprint("Actual Sad: %s" % (data['meanActualSad']))
        pp.pprint("Expected Sad: %s" % (data['meanExpectedSad']))
        pp.pprint("Actual Angry: %s" % (data['meanActualAngry']))
        pp.pprint("Expected Angry: %s" % (data['meanExpectedAngry']))
        pp.pprint("Actual Thankful: %s" % (data['meanActualThankful']))
        pp.pprint("Expected Thankful: %s" % (data['meanExpectedThankful']))
        pp.pprint("Actual Care: %s" % (data['meanActualCare']))
        pp.pprint("Expected Care: %s" % (data['meanExpectedCare']))


    def minmax_norm(df_input):
        return (df_input - df_input.min()) / ( df_input.max() - df_input.min())


    def mongodbConnection(self, collectionName):
        # sudo service mongod status
        if collectionName == None:
            print("CollectionName Error")
            exit(0)

        client = MongoClient('mongodb://localhost:27017/')
        db = client.crowdtangle
        return db[collectionName]


    def countDocuments(self, collectionName): # array of jsons [{},{}]
            # print("Counting documents from: " + collectionName)
            collection = startproj.mongodbConnection(collectionName)
            return collection.count_documents({})


    def getDocumentsAccount(self, collectionName): # array of jsons [{},{}]
        # print("Getting documents from: " + collectionName)
        collection = startproj.mongodbConnection(collectionName)

        return collection.find({},{
            'account.id':1,
            'account.name':1,
            'account.subscriberCount':1,
            'account.pageCategory':1,
            'account.url':1,
        })


    def getDocumentsCollection(self, collectionName): # array of jsons [{},{}]
        print("Getting documents from: " + collectionName)
        collection = startproj.mongodbConnection(collectionName)

        # https://pymongo.readthedocs.io/en/stable/tutorial.html
        # pp.pprint(collection.count_documents({}))
        return collection.find({},{
            'platform':1,
            'type':1,
            'subscriberCount':1,
            'score':1,
            'statistics.actual.likeCount':1,
            'statistics.actual.shareCount':1,
            'statistics.actual.commentCount':1,
            'statistics.actual.loveCount':1,
            'statistics.actual.wowCount':1,
            'statistics.actual.hahaCount':1,
            'statistics.actual.sadCount':1,
            'statistics.actual.angryCount':1,
            'statistics.actual.thankfulCount':1,
            'statistics.actual.careCount':1,
            'statistics.expected.likeCount':1,
            'statistics.expected.shareCount':1,
            'statistics.expected.commentCount':1,
            'statistics.expected.loveCount':1,
            'statistics.expected.wowCount':1,
            'statistics.expected.hahaCount':1,
            'statistics.expected.sadCount':1,
            'statistics.expected.angryCount':1,
            'statistics.expected.thankfulCount':1,
            'statistics.expected.careCount':1,
            'account.id':1,
            'account.name':1,
            'account.subscriberCount':1,
            'account.pageCategory':1,
            'account.url':1,
        })


def checkCollections():
    dfCsv = pd.read_csv('URLs/retweeted_urls_rph_BRA.csv')
    collections = {}
    outGraph = {}
    totalDocuments = 0
    collectionsFiltered = [ # not about specific news
        "collection_0014",
        "collection_0032",
        "collection_0287",
        "collection_0581",
        "collection_0623",
        "collection_0761",
        "collection_0855",
        "collection_1378",
        "collection_1989",
        "collection_2008",
        "collection_2012"
    ]

    for index,row in dfCsv.iterrows():
        if row['collection_name'] in collectionsFiltered:
            continue
        
        collectionName = row['collection_name'].replace("_","")

        qtdDocuments = startproj.countDocuments(row['collection_name'])

        if qtdDocuments <= 10 or qtdDocuments > 1000:
            if qtdDocuments > 1000:
                outGraph[collectionName] = qtdDocuments
            continue
        
        totalDocuments += qtdDocuments
        
        collections[collectionName] = qtdDocuments

    collections = {k: v for k, v in sorted(collections.items(), key=lambda item: item[1], reverse=True)}

    # print(collections)
    print("Total collections: %s" % (len(collections)))
    print("Total documents: %s" % (totalDocuments))

    myList = collections.items()
    x, y = zip(*myList)
    plt.plot(x, y)
    plt.xlabel('Collections')
    plt.ylabel('Documents Count')
    plt.savefig("collectionsHistogram.png")

    print("Collections out of graph:")
    outGraph = {k: v for k, v in sorted(outGraph.items(), key=lambda item: item[1], reverse=True)}
    for key,value in outGraph.items():
        print("\tTotal documents of %s: %s" % (key,value))
        collections[key] = value
        totalDocuments += value

    print("Total collections: %s" % (len(collections)))
    print("Total documents: %s" % (totalDocuments))
    
    return collections


def createGraphNodesCollections(collections):
    # Collection as node
    dfCsv = pd.read_csv('URLs/retweeted_urls_rph_BRA.csv')

    G = nx.Graph()
    collectionsDone = []
    collectionsFiltered = []
    accounts = {}
    edgeCutValue = 2

    # Nodes initialization and their attributes, edge using related account ids cross news shares
    for index,row in dfCsv.iterrows():
        collectionName = row['collection_name'].replace("_","")
        print("\nCollectionName: "+ str(collectionName))

        # Collections filtered above
        if collectionName not in collections:
            print("Collection %s with insufficent length" % (collectionName))
            collectionsFiltered.append(collectionName)
            continue

        documents = startproj.getDocumentsCollection(row['collection_name'])
        dfDocuments = pd.json_normalize(list(documents))

        # Add node attributes from csv
        G.add_node(collectionName)
        rph = row['RP(H)']
        nx.set_node_attributes(G, {collectionName: {"retweetedUrl": row['retweeted_url']}})
        nx.set_node_attributes(G, {collectionName: {"rph": rph}})
        nx.set_node_attributes(G, {collectionName: {"retweetsCount": row['retweets_count']}})
        nx.set_node_attributes(G, {collectionName: {"documentsCount": len(dfDocuments)}})

        # Add node attributes from database
        # G = startproj.addNodeAttributes(G, collectionName, dfDocuments)

        accounts[collectionName] = dfDocuments["account.id"].unique()
        
        if rph < -0.333:
            rphText = "Esquerda"
        elif rph < 0.333:
            rphText = "Neutro"
        else:
            rphText = "Direita"
        
        nx.set_node_attributes(G, {collectionName: {"rphText": rphText}})

        reactionCountActual = None
        reactionCountExpected = None

        # Positive reactions: like, love, wow, haha, thankful, care
        # Negative reactions: sad, angry
        reactionCountActual = ((
                dfDocuments["statistics.actual.likeCount"] +
                dfDocuments["statistics.actual.loveCount"] +
                dfDocuments["statistics.actual.wowCount"] +
                dfDocuments["statistics.actual.hahaCount"] +
                dfDocuments["statistics.actual.thankfulCount"] +
                dfDocuments["statistics.actual.careCount"]) -
                (dfDocuments["statistics.actual.sadCount"] +
                dfDocuments["statistics.actual.angryCount"]
            )) / (
                dfDocuments["statistics.actual.likeCount"] + 
                dfDocuments["statistics.actual.loveCount"] +  
                dfDocuments["statistics.actual.wowCount"] +  
                dfDocuments["statistics.actual.hahaCount"] +  
                dfDocuments["statistics.actual.sadCount"] +  
                dfDocuments["statistics.actual.angryCount"] +  
                dfDocuments["statistics.actual.thankfulCount"] +  
                dfDocuments["statistics.actual.careCount"]
            )

        reactionCountExpected = ((
                dfDocuments["statistics.expected.likeCount"] +
                dfDocuments["statistics.expected.loveCount"] +
                dfDocuments["statistics.expected.wowCount"] +
                dfDocuments["statistics.expected.hahaCount"] +
                dfDocuments["statistics.expected.thankfulCount"] +
                dfDocuments["statistics.expected.careCount"]) -
                (dfDocuments["statistics.expected.sadCount"] +
                dfDocuments["statistics.expected.angryCount"]
            )) / (
                dfDocuments["statistics.expected.likeCount"] +
                dfDocuments["statistics.expected.loveCount"] +
                dfDocuments["statistics.expected.wowCount"] +
                dfDocuments["statistics.expected.hahaCount"] +
                dfDocuments["statistics.expected.sadCount"] +
                dfDocuments["statistics.expected.angryCount"] +
                dfDocuments["statistics.expected.thankfulCount"] +
                dfDocuments["statistics.expected.careCount"]
            )

        # Avoid divisions by 0
        reactionCountActual = reactionCountActual.fillna(0)
        reactionCountExpected = reactionCountExpected.fillna(0)
        # Change to float value
        reactionCountActual = round(reactionCountActual.mean(),2)
        reactionCountExpected = round(reactionCountExpected.mean(),2)
        # print("reactionCountActual: %s" % reactionCountActual)
        # print("reactionCountExpected: %s" % reactionCountExpected)

        reactionScore = reactionCountExpected/reactionCountActual

        if reactionScore > 0.5:
            reactionScoreText = "Positiva"
        elif reactionScore >= -0.5:
            reactionScoreText = "Neutra"
        else:
            reactionScoreText = "Negativa"

        nx.set_node_attributes(G, {collectionName: {"reactionCountActual": reactionCountActual}})
        nx.set_node_attributes(G, {collectionName: {"reactionCountExpected": reactionCountExpected}})
        nx.set_node_attributes(G, {collectionName: {"reactionScore": reactionScore}})
        nx.set_node_attributes(G, {collectionName: {"reactionScoreText": reactionScoreText}})

        for collection in collectionsDone:
            commonElements = list(set(accounts[collection]).intersection(accounts[collectionName]))

            edgeWeight = len(commonElements)
            if edgeWeight >= edgeCutValue:
                G.add_edge(collectionName, collection)
                nx.set_edge_attributes(G, {(collectionName, collection): {"weight": edgeWeight}})
                # nx.set_edge_attributes(G, {(collectionName, collection): {"accounts": ",".join(str(v) for v in commonElements)}})

        collectionsDone.append(collectionName)

    nx.write_gml(G, "Graphs/crowdtangleCollections.gml")
    print("Graph saved!")

    print("Used collections: %s" % (len(collections)))
    print("Collections filtered: %s" % (len(collectionsFiltered)))
    print("Filtered percentage: %s" % ( (len(collectionsFiltered) * 100) / len(collections) ))


def createGraphNodesAccounts(collections):
    # Account as node
    dfCsv = pd.read_csv('URLs/retweeted_urls_rph_BRA.csv')

    G = nx.Graph()
    accountsDone = []
    collectionsFiltered = []
    accounts = {}
    pageIds = {}

    # Create account as node and collections as edges
    for index,row in dfCsv.iterrows():
        # graphData = {}
        collectionName = row['collection_name'].replace("_","")
        print("\nCollectionName: "+ str(collectionName))

        if collectionName not in collections:
            print("Collection %s with insufficent length" % (collectionName))
            collectionsFiltered.append(collectionName)
            continue

        documents = startproj.getDocumentsAccount(row['collection_name'])
        dfDocuments = pd.json_normalize(list(documents))

        dfAccounts = dfDocuments[["account.id","account.name","account.pageCategory","account.subscriberCount"]].set_index('account.id')
        dfAccounts = dfAccounts[~dfAccounts.index.duplicated(keep='first')]
        accounts = dfAccounts.T.to_dict('list')

        # print("accountsDone: %s" % len(accountsDone))
        pageIds = list(accounts.keys())
        pageIds.sort()
        # print("pageIds: %s" % len(pageIds))

        pageToInsert = list(set(pageIds) - set(accountsDone))
        # print("pageToInsert: %s" % len(pageToInsert))
        
        # Create new nodes
        for pageId in pageToInsert:
            attr = dfAccounts.loc[pageId]

            G.add_node(pageId)
            nx.set_node_attributes(G, {pageId: {"pageName": accounts[pageId][0]}})
            try:
                # print("subscriberCount: %s" % str(attr["account.subscriberCount"]))
                nx.set_node_attributes(G, {pageId: {"subscriberCount": int(attr["account.subscriberCount"])}})
            except:
                pass
            try: # Some accounts don't have category
                # print("pageCategory: %s" % str(attr["account.pageCategory"]))
                nx.set_node_attributes(G, {pageId: {"pageCategory": str(attr["account.pageCategory"])}})
            except:
                pass
            accountsDone.append(pageId)

        # Create edges between accounts
        for pageIdOrig in pageIds:
            for pageIdDest in pageIds:
                # print(pageIdOrig, pageIdDest)
                if pageIdOrig < pageIdDest: # include (pageIdOrig == pageIdDest)
                    edgeWeight = G.get_edge_data(pageIdOrig, pageIdDest, default=0)
                    # print("edgeWeight: %s" % edgeWeight)
                    if edgeWeight:
                        nx.set_edge_attributes(G, {(pageIdOrig, pageIdDest): {"weight": edgeWeight['weight']+1}})
                    else:
                        G.add_edge(pageIdOrig, pageIdDest)
                        nx.set_edge_attributes(G, {(pageIdOrig, pageIdDest): {"weight": 1}})


    print("Used collections: %s" % (len(collections)))
    print("Collections filtered: %s" % (len(collectionsFiltered)))
    print("Filtered percentage: %s" % ( (len(collectionsFiltered) * 100) / len(collections) ))

    nx.write_gml(G, "Graphs/crowdtangleAccounts.gml")
    print("Graph saved!")

if __name__ == "__main__":
    startproj = startProj()
    main()
