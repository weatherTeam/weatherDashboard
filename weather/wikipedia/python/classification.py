#!/usr/bin/env python
import sys
import os
import re
import pickle
import nltk
import string

from nltk.stem.porter import PorterStemmer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.neighbors.nearest_centroid import NearestCentroid

def extract_articles(content):
    ''' returns a list of wikipedia articles'''
    pattern = re.compile(r'<page>.*?</page>',re.DOTALL)
    articles = re.findall(pattern, content)
    return articles

def clean(articles):
    ''''removes part of text formating'''
    cleaned = []
    for article in articles:

        # remove capital letters
        article = article.lower()

        article = re.sub(r'<.*?>',"",article)           # tags
        article = re.sub(r'&lt;.*?&gt;',"",article,flags=re.DOTALL)     # references, comments
        article = re.sub(r'\{\{cite.*?\}\}',"",article,flags=re.DOTALL) # citations
        article = re.sub(r'&amp;ndash;',"-",article)    # NOT correct dash... correct is u"\u2013"
        article = re.sub(r'&amp;nbsp;'," ",article)     # non breaking spaces
        article = re.sub(r'&quot;',' ',article)
        article = re.sub(r'&.*?;',"",article)           # rest of html entities

        # remove punctuation (done by sklearn classifier)
        # no_punct = content.translate(None, string.punctuation)

        cleaned.append(article)

    return cleaned

def load_obj(path):
 	''' loads an object saved on disk '''
 	with open(path, 'rb') as f:
 		return pickle.load(f)

def create_dir(path):
    ''' if the directory deosn't exists it creats it'''
    if not os.path.exists(path):
        os.makedirs(path)

###################################################################

voc = load_obj("./save/voc.pkl")
clf = load_obj("./save/ricchio-clf.pkl")
dict_classes = load_obj("./save/dict_classes.pkl")

articles = []
# extracts the articles from files
dir = "./files"
for subdir, dirs, files in os.walk(dir):
    for file in files:
        with open(dir+"/"+file,'r') as f:
            articles.extend(extract_articles(f.read()))


# finds the tfidf
vectorizer = CountVectorizer(
    stop_words='english',min_df=10,vocabulary=voc)
tf_matrix = vectorizer.transform(articles)

tfidf = TfidfTransformer()
tfidf.fit(tf_matrix)
tfidf_matrix = tfidf.transform(tf_matrix)

res = clf.predict(tfidf_matrix.todense())

# Create files for results
res_dir="./res/"
for clazz in dict_classes.keys():
    open(res_dir+dict_classes[clazz]+".xml",'w').close()

# Write results
for i in range(0,len(res)):
    with open(res_dir+dict_classes[res[i]]+".xml",'a') as f:
        f.write(articles[i])
