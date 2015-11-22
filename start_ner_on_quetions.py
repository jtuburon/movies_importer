# -*- coding: UTF-8 -*-
from pymongo import MongoClient 
from pymongo.errors import DuplicateKeyError
import ner

def remove_repated_values(entities_object):
	for k in entities_object.keys():		
		entities_object[k]= sorted(set(entities_object[k]))



tagger = ner.SocketNER(host='localhost', port=8080)
client = MongoClient('localhost', 27017)
my_db = client['Grupo07']
my_db.movies_questions_entities.remove({})
questions= my_db.movies_questions.find()
for q in questions:
	question_entities_obj={"question_id": q["question_id"]}

	#Title NER 
	title_entities= tagger.get_entities(q['title'])
	remove_repated_values(title_entities)
	question_entities_obj["title_entities"]= title_entities

	#Body NER 
	body_entities= tagger.get_entities(q['body'])
	remove_repated_values(body_entities)
	question_entities_obj["body_entities"]= body_entities

	

	answers_entities=[] 
	if 'answers' in q.keys():
		for a in q['answers']:
			answer_body_entities= tagger.get_entities(a['body'])
			remove_repated_values(answer_body_entities)		
			if len(answer_body_entities)>0:
				answer_entities_obj= {"answer_id": a["answer_id"], "entities": answer_body_entities}
				answers_entities.append(answer_entities_obj)

	question_entities_obj["answers_entities"]= answers_entities
	
	my_db.movies_questions_entities.insert(question_entities_obj)