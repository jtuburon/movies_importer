# -*- coding: UTF-8 -*-
from pymongo import MongoClient 
from pymongo.errors import DuplicateKeyError
from utils import *

client = MongoClient('localhost', 27017)
my_db = client['Grupo07']
body= my_db.questions_spotlight_resources.find_one()['body'];

print body['@text']
for r in body['Resources']:
	print r
	print body['@text'][0:85]
