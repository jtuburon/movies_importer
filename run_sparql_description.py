from SPARQLWrapper import SPARQLWrapper, JSON

sparql = SPARQLWrapper("http://dbpedia.org/sparql")
sparql.setQuery("""
    PREFIX dbres: <http://dbpedia.org/resource/>
	DESCRIBE dbres:Shakira
""")
sparql.setReturnFormat(JSON)
results = sparql.query().convert()

print results["results"]["bindings"][0]