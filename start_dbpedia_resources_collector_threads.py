from pymongo import MongoClient 
from pymongo.errors import DuplicateKeyError
import threading
from SPARQLWrapper import SPARQLWrapper, JSON

class MyThread (threading.Thread):
	def __init__(self, offset, lim):
		super(MyThread, self).__init__()
		self.offset = offset
		self.lim = lim
		self.sparql = SPARQLWrapper("http://dbpedia.org/sparql")
		self.sparql.setReturnFormat(JSON)	
	
	def run(self):
		self.run_thread(self.offset, self.lim)

	def run_thread(self, start, lim):
		client = MongoClient('localhost', 27017)
		my_db = client['Grupo07']
		spotlight_resources= my_db.questions_spotlight_resources.find().sort("question_id", 1).skip(start).limit(lim)
		i=0
		for res in spotlight_resources:
			i= i+1
			print str(start) +" : " +str(i)
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
								try:
									dbpedia_res={
										"label": r["@surfaceForm"],
										"uri": r["@URI"],
										"types": r["@types"],
									}
									self.sparql.setQuery("DESCRIBE <"+ r["@URI"]+ ">")
									results = self.sparql.query().convert()
									dbpedia_res["tuples"]= results["results"]["bindings"]
									my_db.dbpedia_resources.insert(dbpedia_res)
								except:
									pass
			else:
				if 'Resources' in res[attribute].keys():
					for r in res[attribute]['Resources']:						
						try:
							dbpedia_res={
								"label": r["@surfaceForm"],
								"uri": r["@URI"],
								"types": r["@types"],
							}
							self.sparql.setQuery("DESCRIBE <"+ r["@URI"]+ ">")
							results = self.sparql.query().convert()
							dbpedia_res["tuples"]= results["results"]["bindings"]
							my_db.dbpedia_resources.insert(dbpedia_res)
						except:
							pass

# def delete_collection():
# 	client = MongoClient('localhost', 27017)
# 	my_db = client['Grupo07']
# 	my_db.dbpedia_resources.remove({})


#delete_collection()
lot_size=500;
total=10000
hilos=[]
for i in range(total/lot_size):
	start= i*lot_size
	hilo= MyThread(start, lot_size)
	hilos.append(hilo)
	hilo.start()

for h in hilos:
	h.join()