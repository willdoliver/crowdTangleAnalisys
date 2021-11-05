import pandas as pd
from pandas.core.frame import DataFrame
from pymongo import MongoClient
import pprint as pp
from nltk import word_tokenize, sent_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk import pos_tag

# @TODO:
# Mount graph, using account's and news links as nodes and edges
# Clusterizar titles e descriptions
# Normalizar valores de reactions/likes de acordo com as collections
# Analisar PageCategory dos registros de circulação de notícias

def main():
    # collection_name,retweeted_url,RP(H),retweets_count
    dfCsv = pd.read_csv('URLs/retweeted_urls_test.csv',nrows=50)

    fieldsToCollect = [
        # "_id",
        "platform",
        "type",
        "subscriberCount",
        "score",
        "statistics.actual.likeCount",
        "statistics.actual.shareCount",
        "statistics.actual.commentCount",
        "statistics.actual.loveCount",
        "statistics.actual.wowCount",
        "statistics.actual.hahaCount",
        "statistics.actual.sadCount",
        "statistics.actual.angryCount",
        "statistics.actual.thankfulCount",
        "statistics.actual.careCount",
        "statistics.expected.likeCount",
        "statistics.expected.shareCount",
        "statistics.expected.commentCount",
        "statistics.expected.loveCount",
        "statistics.expected.wowCount",
        "statistics.expected.hahaCount",
        "statistics.expected.sadCount",
        "statistics.expected.angryCount",
        "statistics.expected.thankfulCount",
        "statistics.expected.careCount",
        "account.id",
        "account.name",
        "account.subscriberCount",
        "account.accountType",
    ]

    dfGraph = pd.DataFrame()

    for index,row in dfCsv.iterrows():
        graphData = {}
        print("\nCollectionName: "+ str(row['collection_name']))
        
        graphData['collectionName']   = row['collection_name']
        graphData['link']             = row['retweeted_url']
        graphData['rph']              = row['RP(H)']
        graphData['retweetsCount']    = row['retweets_count']

        news = getDocuments(row['collection_name'])
        dfDB = pd.json_normalize(list(news))
        dfDB = dfDB[fieldsToCollect]

        print("Calculating fields for: " + row['collection_name'])
        statisticsCalculate(dfDB, graphData)

        dfGraph = dfGraph.append(graphData, ignore_index = True)
    
        # textTreatment(df)
        # normalized values
        # df["minmax_norm"] = minmax_norm(df["statistics.actual.likeCount"])

    dfGraph.to_csv("collections/graph.csv", index=False)
    print(dfGraph)


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



def statisticsCalculate(df, data):
    # General data
    data['itemsCount']                     = len(df)
    data['platform']                       = dict(df.groupby(["platform"])["platform"].count())
    data['type']                           = dict(df.groupby(["type"])["type"].count())
    data['accountType']                    = dict(df.groupby(["account.accountType"])["account.accountType"].count())
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

    pp.pprint()


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


def mongodbConnection(collectionName):
    # sudo service mongod status
    if collectionName == None:
        print("CollectionName Error")
        exit(0)

    client = MongoClient('mongodb://localhost:27017/')
    db = client.crowdtangle
    return db[collectionName]


def getDocuments(collectionName): # array of jsons [{},{}]
    pp.pprint("Getting documents from: " + collectionName)
    collection = mongodbConnection(collectionName)

    # https://pymongo.readthedocs.io/en/stable/tutorial.html
    # pp.pprint(collection.count_documents({}))
    return collection.find({})


if __name__ == "__main__":
    main()