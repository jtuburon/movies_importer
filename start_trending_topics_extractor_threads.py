# -*- coding: UTF-8 -*-
from pymongo import MongoClient 
from pymongo.errors import DuplicateKeyError
from utils import *
import thread
import threading

import nltk
from nltk.corpus import stopwords
from pattern.es import parse, split
import re

import html2text

def delete_collection():
	client = MongoClient('localhost', 27017)
	my_db = client['Grupo07']
	my_db.movies_trending_topics.remove({})


class MyThread (threading.Thread):
	def __init__(self, offset, lim):
		super(MyThread, self).__init__()
		self.offset = offset
		self.lim = lim
		self.SPACE_PATTERN = re.compile("\s+")
		self.stop_words= stopwords.words('english')
		self.exclusion_tokens=["", " ", "rt"]
	
	def run(self):
		self.run_thread(self.offset, self.lim)

	def extract_topics_list(self, text):	
		words = re.split(self.SPACE_PATTERN, text)
		topic_words=[]
		for t in words:
			t_lower= t.lower()
			if(len(t)>0 and t_lower not in self.stop_words and t_lower not in self.exclusion_tokens):
				last_char = t[-1]
				if last_char in [',', ':', '.', '?', '!']:
					t= t[0:-1]
				topic_words.append(t)
		return topic_words

	def run_thread(self, start, lim):
		client = MongoClient('localhost', 27017)
		my_db = client['Grupo07']

		questions= my_db.movies_questions.find().sort("question_id", 1).skip(start).limit(lim)
		i=0
		for q in questions:
			i= i+1
			print str(start) +" : " +str(i)
			q_topics= []
			q_topics = q_topics + self.extract_topics_list(q['title'])	
			q_topics = q_topics + self.extract_topics_list(html2text.html2text(q['body']))
			if "answers" in q.keys():
				for a in q["answers"]:
					q_topics = q_topics + self.extract_topics_list(html2text.html2text(a['body']))	
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


delete_collection()
lot_size=500;
total=10000
hilos = []
for i in range(total/lot_size):
	start= i*lot_size
	hilo= MyThread(start, lot_size)
	hilos.append(hilo)
	hilo.start()
	
for h in hilos:
	h.join()