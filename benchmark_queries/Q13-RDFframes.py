from rdfframes.knowledge_graph import KnowledgeGraph
from rdfframes.client.http_client import HttpClientDataFormat, HttpClient
from rdfframes.client.sparql_endpoint_client import SPARQLEndpointClient
import time
__author__ = "Ghadeer"


endpoint = 'http://10.161.202.101:8890/sparql/'
port = 8890
output_format = HttpClientDataFormat.PANDAS_DF
max_rows = 1000000
timeout = 12000


client = SPARQLEndpointClient(endpoint)
graph =  KnowledgeGraph(graph_name='dbpedia')


def expand_optional():
  Films = graph.entities('dbpo:Film', entities_col_name='film')\
    .expand('film', [('dbpp:starring', 'actor'), ('dbpp:director','director', True), ('dbpp:country', 'movie_country'), 
      ('dbpp:producer', 'producer', True), ('dbpp:language', 'language'), ('dbpp:story','story') ,
      ('dbpp:runtime', 'runtime'), ('dbpp:studio' ,'studio', True), ('dbpp:title', 'title'),
      ('dbpp:genre', 'genre')])
  print("SPARQL Query = \n{}".format(Films.to_sparql())) # dbo:starring ?actor .  #,('dbpp:releaseDate', 
  df = Films.execute(client)
  print(df.shape)

#### running 
start_time = time.time()
expand_optional()
end_time = time.time()

print('Duration = {} sec'.format(end_time-start_time))














