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
from sklearn.metrics.pairwise import cosine_similarity

def extract_articles(content):
    ''' returns a list of cleaned wikipedia articles'''
    pattern = re.compile(r'<page>.*?</page>',re.DOTALL)
    articles = re.findall(pattern, content)
    return clean(articles)

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

def tokenize(text):
    '''extracts stemmed tokens from a set of wikipedia articles (not used)'''
    # tokenization  

    tokens = nltk.word_tokenize(text)

    # stemming
    stemmer = PorterStemmer()
    stemmed = []
    for word in tokens:
        stemmed.append(stemmer.stem(word))
    return stemmed

def create_dir(path):
    ''' if the directory deosn't exists it creats it'''
    if not os.path.exists(path):
        os.makedirs(path)

def save_readable_voc(voc, path):
    ''' takes a vocabulary list and saves it as text in a files'''
    with open(path,'w') as f:
        for elem in voc:
            f.write("%s\n" % elem.encode('utf-8'))

def save_obj(obj, path):
    '''saves an object as it is in a file'''
    with open(path, 'wb') as f:
        pickle.dump(obj,f)

###################################################################

dict_classes = {}
articles = []

# extracts the articles from files
count = 0
for i in xrange(1,len(sys.argv)):
    file_name = str(sys.argv[i])
    with open(file_name, 'r') as f:
        content = f.read()
    # add class name to list   
    class_name = os.path.splitext(os.path.basename(file_name))[0]
    dict_classes[count] = class_name
    count += 1
    articles.append(extract_articles(content)) # should make tuple with class id

flat_articles = [y for x in articles for y in x]
print len(flat_articles)

# finds the term frequencies
vectorizer = CountVectorizer(stop_words='english',min_df=2)
tf_matrix = vectorizer.fit_transform(flat_articles)
print len(vectorizer.get_feature_names())

# finds the tf-idf matrix for each article
tfidf = TfidfTransformer()
tfidf.fit(tf_matrix)
tfidf_matrix = tfidf.transform(tf_matrix)

# contruct articles classes for classifier
classes = []
for i in range(0,len(articles)):
    for article in articles[i]:
        classes.append(i)

# train the nearest centroid classifier (Ricchio classifier)
clf = NearestCentroid(metric='cosine')
clf.fit(tfidf_matrix.todense(),classes)

#print zip(classes,clf.predict(tfidf_matrix.todense()))

# save info on disk
create_dir("./save")
voc = vectorizer.get_feature_names()
save_obj(voc,"./save/voc.pkl")
save_readable_voc(voc,"./save/voc.txt")
save_obj(clf,"./save/ricchio-clf.pkl")
print dict_classes
save_obj(dict_classes,"./save/dict_classes.pkl")