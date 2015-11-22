import requests
import json

URL="http://spotlight.sztaki.hu:2222/rest/annotate"

def make_request(text):

	headers = {'Accept': 'application/json'}
	payload = {'text': text, "confidence": "0.7"}
	r = requests.post(URL, headers= headers, data= payload, timeout=15)
	try:
		json_data = json.loads(r.text)
		return json_data
	except Exception as e:
		print ""
		return {}


def retrieve_resources(q, attribute):
	if attribute in q.keys():
		if attribute =="answers":
			answers_resources=[]
			for a in q[attribute]:
				a_r= make_request(a['body'])
				a_r["answer_id"] = a["answer_id"]
				answers_resources.append(a_r)
			return answers_resources
		else:
			return make_request(q[attribute])
