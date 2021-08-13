import pandas as pd
from pandas.core.frame import DataFrame
from pymongo import MongoClient
import pprint as pp
from nltk import word_tokenize, sent_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk import pos_tag

# @TODO:
# Clusterizar titles e descriptions
# Normalizar valores de reactions/likes de acordo com as collections
# Analisar PageCategory dos registros de circulação de notícias

def main():
    df = pd.read_csv('URLs/retweeted_urls_rph_BRA.csv',nrows=5)

    fieldsToCollect = [
        "_id",
        # "platform",
        "postUrl",
        "title",
        "description",
        "score",
        # "postUrl",
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
        "account.pageCategory"
    ]

    for index,row in df.iterrows():
        print("\nCollectionName: "+ str(row['collection_name']))
        news = getDocuments(row['collection_name'])

        df = pd.json_normalize(list(news))
        df = df[fieldsToCollect]

        # meanCalculate(df, row["retweeted_url"])
        textTreatment(df)

        # normalized values
        # df["minmax_norm"] = minmax_norm(df["statistics.actual.likeCount"])
        # pp.pprint(df)
        # df.to_csv("collections/%s_normalized.csv" % (row['collection_name']), index=False)
        exit(0)

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


def meanCalculate(df, retweeted_url):
    meanScore           = round(df["score"].mean(),2)
    meanActualLike      = round(df["statistics.actual.likeCount"].mean(),2)
    meanExpectedLike    = round(df["statistics.expected.likeCount"].mean(),2)
    meanActualShare     = round(df["statistics.actual.shareCount"].mean(),2)
    meanExpectedShare   = round(df["statistics.expected.shareCount"].mean(),2)
    meanActualComment   = round(df["statistics.actual.commentCount"].mean(),2)
    meanExpectedComment = round(df["statistics.expected.commentCount"].mean(),2)
    # Reactions
    meanActualLove          = round(df["statistics.actual.loveCount"].mean(),2)
    meanExpectedLove        = round(df["statistics.expected.loveCount"].mean(),2)
    meanActualWow           = round(df["statistics.actual.wowCount"].mean(),2)
    meanExpectedWow         = round(df["statistics.expected.wowCount"].mean(),2)
    meanActualHaha          = round(df["statistics.actual.hahaCount"].mean(),2)
    meanExpectedHaha        = round(df["statistics.expected.hahaCount"].mean(),2)
    meanActualSad           = round(df["statistics.actual.sadCount"].mean(),2)
    meanExpectedSad         = round(df["statistics.expected.sadCount"].mean(),2)
    meanActualAngry         = round(df["statistics.actual.angryCount"].mean(),2)
    meanExpectedAngry       = round(df["statistics.expected.angryCount"].mean(),2)
    meanActualThankful      = round(df["statistics.actual.thankfulCount"].mean(),2)
    meanExpectedThankful    = round(df["statistics.expected.thankfulCount"].mean(),2)
    meanActualCare          = round(df["statistics.actual.careCount"].mean(),2)
    meanExpectedCare        = round(df["statistics.expected.careCount"].mean(),2)

    pp.pprint("Count: %s" % (df.shape[0]))
    pp.pprint("Retweeted Url: %s" % (retweeted_url))
    pp.pprint("Means:")
    pp.pprint("Score: %s" % (meanScore))
    pp.pprint("Actual Like: %s" % (meanActualLike))
    pp.pprint("Expected Like: %s" % (meanExpectedLike))
    pp.pprint("Actual Share: %s" % (meanActualShare))
    pp.pprint("Expected Share: %s" % (meanExpectedShare))
    pp.pprint("Actual Comments: %s" % (meanActualComment))
    pp.pprint("Expected Comments: %s" % (meanExpectedComment))
    # Reactions
    pp.pprint("Actual Love: %s" % (meanActualLove))
    pp.pprint("Expected Love: %s" % (meanExpectedLove))
    pp.pprint("Actual Wow: %s" % (meanActualWow))
    pp.pprint("Expected Wow: %s" % (meanExpectedWow))
    pp.pprint("Actual Haha: %s" % (meanActualHaha))
    pp.pprint("Expected Haha: %s" % (meanExpectedHaha))
    pp.pprint("Actual Sad: %s" % (meanActualSad))
    pp.pprint("Expected Sad: %s" % (meanExpectedSad))
    pp.pprint("Actual Angry: %s" % (meanActualAngry))
    pp.pprint("Expected Angry: %s" % (meanExpectedAngry))
    pp.pprint("Actual Thankful: %s" % (meanActualThankful))
    pp.pprint("Expected Thankful: %s" % (meanExpectedThankful))
    pp.pprint("Actual Care: %s" % (meanActualCare))
    pp.pprint("Expected Care: %s" % (meanExpectedCare))


def minmax_norm(df_input):
    return (df_input - df_input.min()) / ( df_input.max() - df_input.min())


def mongodbConnection(collectionName):
    # sudo service mongod status
    if collectionName == None:
        print("CollectionName Error")
        exit(0)

    client = MongoClient('mongodb://localhost:27017/')
    db = client.config
    return db[collectionName]


def getDocuments(collectionName): # array of jsons [{},{}]
    collection = mongodbConnection(collectionName)

    # https://pymongo.readthedocs.io/en/stable/tutorial.html
    # pp.pprint(collection.count_documents({}))
    return collection.find({})


if __name__ == "__main__":
    main()