# -*- coding: UTF-8 -*-
from pymongo import MongoClient 
from pymongo.errors import DuplicateKeyError
from utils import *
import thread

def delete_collection():
	client = MongoClient('localhost', 27017)
	my_db = client['Grupo07']
	my_db.questions_spotlight_resources.remove({})


import threading

class MyThread (threading.Thread):
	def __init__(self, offset, lim):
		super(MyThread, self).__init__()
		self.offset = offset
		self.lim = lim
	
	def run(self):
		self.run_thread(self.offset, self.lim)

	def run_thread(self, start, lim):
		client = MongoClient('localhost', 27017)
		my_db = client['Grupo07']

		questions= my_db.movies_questions.find().sort("question_id", 1).skip(start).limit(lim)
		i=0
		for q in questions:
			i= i+1
			print str(start) +" : " +str(i)
			question_resource={
				"question_id": q["question_id"],
				"title": retrieve_resources(q, "title"),
				"body": retrieve_resources(q, "body"),
				"answers": retrieve_resources(q, "answers"),
			}
			my_db.questions_spotlight_resources.insert(question_resource)


delete_collection()
lot_size=500;
total=10000
for i in range(total/lot_size):
	start= i*lot_size
	hilo= MyThread(start, lot_size)
	hilo.start()

	