# -*- coding: UTF-8 -*-

from pymongo import MongoClient 
from pymongo.errors import DuplicateKeyError
import json
import requests
import re
from lxml import html, etree
from datetime import date

client = MongoClient('localhost', 27017)
my_db = client['Grupo07']

YEAR_PATTERN= "In \d{4}"

today = date.today()

imdb_rss_urls= {
	"born":"http://rss.imdb.com/daily/born/",
	"died":"http://rss.imdb.com/daily/died/",
	}
for key in imdb_rss_urls.keys():
	r= requests.get(imdb_rss_urls[key]);

	root= html.fromstring(r.content.decode('latin-1').encode("utf-8"))
	items = root.xpath("//item")
	for i in items:
		date_object ={}
		date_object["kind"]= key
		date_object["guid"]= i.xpath("./guid")[0].text
		date_object["name"]= i.xpath("./title")[0].text
		date_object["description"]= i.xpath("./description")[0].text
		matching =  re.search('(\d{4})', date_object["description"])
		if matching is not  None:
			year= matching.group(0)
			date_o= today.replace(year=int(year))
			date_object["date"]= str(date_o)
		try:
			my_db.born_or_died_dates.insert(date_object)
		except DuplicateKeyError as e:
			pass
		
		