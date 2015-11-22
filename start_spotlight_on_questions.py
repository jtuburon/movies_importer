# -*- coding: UTF-8 -*-
import sys
from pymongo import MongoClient 
from pymongo.errors import DuplicateKeyError
from utils import *

offset= sys.argv[0]
lim= sys.argv[1]

client = MongoClient('localhost', 27017)
my_db = client['Grupo07']
my_db.questions_spotlight_resources.remove({})

questions= my_db.movies_questions.find().sort("question_id", 1).skip(offset).limit(lim)
i=0
for q in questions:
	i=i+1
	if i%10==0:
		print i
	question_resource={
		"question_id": q["question_id"],
		"title": retrieve_resources(q, "title"),
		"body": retrieve_resources(q, "body"),
		"answers": retrieve_resources(q, "answers"),
	}
	my_db.questions_spotlight_resources.insert(question_resource)
	