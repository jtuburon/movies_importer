# -*- coding: UTF-8 -*-
from pymongo import MongoClient 
from pymongo.errors import DuplicateKeyError
import json
import stackexchange

client = MongoClient('localhost', 27017)
my_db = client['Grupo07']

api_key='68df1YOXb1WWdundnCk0FQ(('

site = stackexchange.Site("movies.stackexchange", api_key)
site.include_body=True
site.impose_throttling = True
site.throttle_stop = False


from_date=946684800 # 2000-01-01
page_size=100
page_setting= my_db.settings.find_one({"id":1})
#page_index = int(page_setting["value"])
page_index = 0

keep_requesting= True

sw = True


page_index= page_index+1;
print page_index

questions= site.questions(pagesize=page_size, page=page_index, fromdate=from_date, order="asc", sort="creation")
print len(questions)
print page_index
for q in questions:		
	try:
		print q.creation_date
		my_db.movies_questions.insert(q.json)		
	except DuplicateKeyError as e:
		pass
print "Conte en la BD"
print my_db.movies_questions.count()