from pymongo import MongoClient 
import re

client = MongoClient('localhost', 27017)
my_db = client['Grupo07']
my_db.places_in_dbpedia.remove({})

prop="http://www.georss.org/georss/point"
resources= my_db.dbpedia_resources.find({"tuples.p.value": prop}, 
	{"label": 1, "uri": 1,"tuples": {"$elemMatch": {"p.value": prop}}})
for r in resources:
	vals= r["tuples"][0]["o"]["value"].split(" ")
	place={
		"type" : "Point", 
		"coordinates" : [ float(vals[1]), float(vals[0])], 
		"uri": r["uri"], 
		"label":r["label"]}
	my_db.places_in_dbpedia.insert(place)