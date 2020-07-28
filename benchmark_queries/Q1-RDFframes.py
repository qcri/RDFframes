from time import time
from rdfframes.knowledge_graph import KnowledgeGraph
from rdfframes.client.http_client import HttpClientDataFormat, HttpClient
from rdfframes.client.sparql_endpoint_client import SPARQLEndpointClient
from rdfframes.utils.constants import JoinType
__author__ = "Ghadeer"


endpoint = 'http://10.161.202.101:8890/sparql/'
port = 8890
output_format = HttpClientDataFormat.PANDAS_DF
max_rows = 1000000
timeout = 12000
"""
client = HttpClient(endpoint_url=endpoint,
	port=port,
		return_format=output_format,
		timeout=timeout,
		max_rows=max_rows
		)
"""
client = SPARQLEndpointClient(endpoint)

graph =  KnowledgeGraph(graph_name='dbpedia')

def expand_join(join_type):
  basketball_palyer = graph.entities('dbpo:BasketballPlayer', entities_col_name='player')\
		.expand('player', [('dbpp:nationality', 'nationality') ,('dbpp:birthPlace', 'place')\
            ,('dbpp:birthDate','birthDate'),('dbpp:team', 'team')])
  basketball_team = graph.entities('dbpo:BasketballTeam', entities_col_name='team')\
  .expand('team', [('dbpp:president', 'president'), ('dbpp:sponsor', 'sponsor'), ('dbpp:name', 'name')])
  basketball_palyer_team = basketball_team.join(basketball_palyer,'team', join_type=join_type)
  df = basketball_palyer_team.execute(client)

start = time()
expand_join(JoinType.RightOuterJoin) ## change the type here.
duration = time()-start
print("Duration ofRightOuter Join on expandable datasets = {} sec".format(duration))
