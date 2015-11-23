from pymongo import MongoClient 
from pymongo.errors import DuplicateKeyError
import threading
from SPARQLWrapper import SPARQLWrapper, JSON

class MyThread (threading.Thread):
	def __init__(self, question_id):
		super(MyThread, self).__init__()
		self.question_id = question_id
		self.sparql = SPARQLWrapper("http://dbpedia.org/sparql")
		self.sparql.setReturnFormat(JSON)	
	
	def run(self):
		self.run_thread(self.question_id)

	def run_thread(self, question_id):
		client = MongoClient('localhost', 27017)
		my_db = client['Grupo07']
		spotlight_resources= my_db.questions_spotlight_resources.find({"question_id":question_id})
		for res in spotlight_resources:
			print question_id
			self.scan_dbpedia_resources(my_db, res, "title")
			self.scan_dbpedia_resources(my_db, res, "body")
			self.scan_dbpedia_resources(my_db, res, "answers")

	def scan_dbpedia_resources(self, my_db, res, attribute):
		if attribute in res.keys():
			if attribute =="answers":		
				if res[attribute]!= None:
					for a in res[attribute]:
						if "Resources" in a.keys():
							for r in a['Resources']:
								dbpedia_res={
									"label": r["@surfaceForm"],
									"uri": r["@URI"],
									"types": r["@types"],
								}
								self.sparql.setQuery("DESCRIBE <"+ r["@URI"]+ ">")
								results = self.sparql.query().convert()
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
						self.sparql.setQuery("DESCRIBE <"+ r["@URI"]+ ">")
						results = self.sparql.query().convert()
						dbpedia_res["tuples"]= results["results"]["bindings"]
						try:
							my_db.dbpedia_resources.insert(dbpedia_res)
						except:
							pass

hilo= MyThread(43637)
hilo.start()
	
		