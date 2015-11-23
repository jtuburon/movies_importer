from pymongo import MongoClient 
from pymongo.errors import DuplicateKeyError
from stop_words import get_stop_words
import re

#stop_words = get_stop_words('en')

stop_words = [">", "\*", "-", "\d+", "\(or"]

client = MongoClient('localhost', 27017)
my_db = client['Grupo07']

for w in stop_words:
	pattern = re.compile("^"+w+"$", re.IGNORECASE)
	topics= my_db.movies_trending_topics.find({"word": pattern})
	for t in topics:
		print t['word']
		my_db.movies_trending_topics.remove(t)
