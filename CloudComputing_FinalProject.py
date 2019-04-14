import csv
import numpy as np

from pyspark import SparkContext

import sys
import ast
import json
import requests
from operator import add
from time import strftime
import requests_oauthlib
import string


#nltk libraries

from nltk.sentiment import SentimentAnalyzer
from nltk.sentiment.util import *
from nltk.corpus import stopwords
from nltk.classify import NaiveBayesClassifier


sc  = SparkContext('local[1]', 'TwitterSampleStream')
BLOCKSIZE = 1000  # How many tweets per update
INTERVAL = 45  # How frequently to update
ssc = StreamingContext(sc, INTERVAL)



def getrow(line):
  """
  Splits apart the rows for each line
  """
  row = line.split(',')
  sentiment = row[1]
  tweet = row[3].strip()
  tweet_lower=get_tweet_text(tweet)
  return (tweet_lower, sentiment)

def func_streamer():
  """
  Makes a GET request to the Twitter resource and returns a 
  specified number of Tweets (blocksize)
  """
  response = requests.get(filter_url, auth=auth, stream=True)
  count = 0
  for line in response.iter_lines():
    try:
      if count > BLOCKSIZE:
        break
      post = json.loads(line.decode('utf-8'))
      contents = [post['text']]
      count += 1
      yield str(contents)
    except:
      result = False

def func_classify(tweet):
  """
  Returns class that the tweet belongs to
  """
  sentence = [(tweet, '')]
  test_set = sentim_analyzer.apply_features(sentence)
  return(tweet, classifier.classify(test_set[0][0]))

def get_tweet_text(tweet):
  """
  Returns cleaned tweet text
  """

  tweet = line.strip()
  translator = str.maketrans({key: None for key in string.punctuation})
  tweet = tweet.translate(translator)
  tweet = tweet.split(' ')
  tweet_lower = []
  for word in tweet:
    tweet_lower.append(word.lower())
  return tweet_lower
 

def func_rdd_output(rdd):
  """
  Adds labels to all tweets in the global result var
  """
  global results
  couple = rdd.map(lambda x: (func_classify(get_tweet_text(x))[1],1))
  counts = couple.reduceByKey(add)
  output = []
  for count in counts.collect():
    output.append(count)
  result = [time.strftime("%I:%M:%S"), output]
  results.append(result)

def flat_map(t, rdd):
  return rdd.flatMap(lambda x: func_streamer())

#build nltk training model
#read in the text file and create a dataset in memory
file = sc.textFile("Analysis_Dataset.csv")
header = file.take(1)[0]
raw_data = file.filter(lambda line: line != header)
#call the function on each row in the dataset
#create a SentimentAnalyzer object
#get list of stopwords (with _NEG) to use as a filter
init_data = raw_data.map(lambda line: getrow(line))
sentim_analyzer = SentimentAnalyzer()
stopwords_all = []
for word in stopwords.words('english'):
  stopwords_all.append(word)
  stopwords_all.append(word + '_NEG')

#take 10,000 Tweets from this training dataset for this example and get all the words
#that are not stop words
init_data_sample = init_data.take(10000)
dictionary_of_neg = sentim_analyzer.dictionary_of([mark_negation(doc) for doc in init_data_sample])
dictionary_of_neg_nostops = [x for x in dictionary_of_neg if x not in stopwords_all]
unigram_feats = sentim_analyzer.unigram_word_feats(dictionary_of_neg_nostops, top_n=200)
sentim_analyzer.add_feat_extractor(extract_unigram_feats, unigrams=unigram_feats)
training_set = sentim_analyzer.apply_features(init_data_sample)

#train the model

trainer = NaiveBayesClassifier.train
classifier = sentim_analyzer.train(trainer, training_set)

#set up Twitter authentication
key = "**"
secret = "***"
token = "***"
token_secret = "***"

#specify the URL and a search term
search_term=sys.argv[1]

filter_url = 'https://stream.twitter.com/1.1/statuses/filter.json?track='+search_term
#’auth’ represents the authorization that will be passed to Twitter
auth = requests_oauthlib.OAuth1(key, secret, token, token_secret)
# Setup Stream
rdd = ssc.sparkContext.parallelize([0])
stream = ssc.queueStream([], default=rdd)


stream = stream.transform(flat_map)
c_stream = stream.map(lambda line: ast.literal_eval(line))

results = []


c_stream.foreachRDD(lambda t, rdd: func_rdd_output(rdd))
ssc.start()
ssc.awaitTermination()
cont = True
while cont:
  if len(results) >10:
    cont = False
ssc.stop()
#save results
reducedresults = sc.parallelize(results)
reducedresults.saveAsTextFile(csvfile)




