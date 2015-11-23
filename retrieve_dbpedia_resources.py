# -*- coding: UTF-8 -*-
from pymongo import MongoClient 
from pymongo.errors import DuplicateKeyError
import json
from SPARQLWrapper import SPARQLWrapper, JSON


client = MongoClient('localhost', 27017)
my_db = client['Grupo07']

my_db.dbpedia_resources.remove({})

spotlight_resources= my_db.questions_spotlight_resources.find()

sparql = SPARQLWrapper("http://dbpedia.org/sparql")
sparql.setReturnFormat(JSON)

def scan_dbpedia_resources(res, attribute):
	if attribute in res.keys():
		if attribute =="answers":		
			if res[attribute]!= None:
				for a in res[attribute]:
					for r in a['Resources']:
						dbpedia_res={
							"label": r["@surfaceForm"],
							"uri": r["@URI"],
							"types": r["@types"],
						}
						sparql.setQuery("DESCRIBE <"+ r["@URI"]+ ">")
						results = sparql.query().convert()
						dbpedia_res["tuples"]= results["results"]["bindings"]
						try:
							my_db.dbpedia_resources.insert(dbpedia_res)
						except:
							pass
		else:
			if 'Resources' in res[attribute].keys():
				for r in res[attribute]['Resources']:
					dbpedia_res={
						"label": r["@surfaceForm"],
						"uri": r["@URI"],
						"types": r["@types"],
					}
					sparql.setQuery("DESCRIBE <"+ r["@URI"]+ ">")
					results = sparql.query().convert()
					dbpedia_res["tuples"]= results["results"]["bindings"]
					try:
						my_db.dbpedia_resources.insert(dbpedia_res)
					except:
						pass

for res in spotlight_resources:
	scan_dbpedia_resources(res, "title")
	scan_dbpedia_resources(res, "body")
	scan_dbpedia_resources(res, "answers")
