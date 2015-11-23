# -*- coding: UTF-8 -*-
from pymongo import MongoClient 
from pymongo.errors import DuplicateKeyError

import nltk
from nltk.corpus import stopwords
from pattern.es import parse, split
import re

import html2text


SPACE_PATTERN = re.compile("\s+")

stop_words = stopwords.words('english')
exclusion_tokens=["", " ", "rt"]


def extract_topics_list(text):	
	words = re.split(SPACE_PATTERN, text)
	topic_words=[]
	for t in words:
		t_lower= t.lower()
		if(len(t)>0 and t_lower not in stop_words and t_lower not in exclusion_tokens):
			last_char = t[-1]
			if last_char in [',', ':', '.', '?', '!']:
				t= t[0:-1]
			topic_words.append(t)
	return topic_words

client = MongoClient('localhost', 27017)
my_db = client['Grupo07']

my_db.movies_trending_topics.remove({})

questions= my_db.movies_questions.find().sort("question_id", 1)
i=0
for q in questions:
	i = i+1
	if i%100==0:
		print i
	q_topics= []
	q_topics = q_topics + extract_topics_list(q['title'])	
	q_topics = q_topics + extract_topics_list(html2text.html2text(q['body']))
	if "answers" in q.keys():
		for a in q["answers"]:
			q_topics = q_topics + extract_topics_list(html2text.html2text(a['body']))	
	topics_dict={}
	for w in q_topics:
		if w in topics_dict:
			topics_dict[w]=topics_dict[w] + 1
		else:
			topics_dict[w]=1

	for k in topics_dict.keys():
		topicObj = my_db.movies_trending_topics.find_one({"word": k})
		if topicObj!=None:		
			topicObj['count']=topicObj['count']+topics_dict[k];			
			my_db.movies_trending_topics.update({'_id': topicObj['_id']}, {"$set": topicObj}, upsert=False)
		else:
			topicObj= {"word": k, "count": topics_dict[k]}
			my_db.movies_trending_topics.insert(topicObj)
	
