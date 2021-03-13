
''' Get a list of American actors available in both DBpedia and YAGO graphs. '''
from rdfframes.knowledge_graph import KnowledgeGraph
from rdfframes.utils.constants import JoinType
from rdfframes.client.http_client import HttpClientDataFormat, HttpClient
from time import time

graph1 = KnowledgeGraph(graph_name='dbpedia2020')
graph2 = KnowledgeGraph(graph_name='yago',
                        graph_uri='http://yago-knowledge.org/',
                        prefixes={
                            'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
                            'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
                            'yago': 'http://yago-knowledge.org/resource/',
                            'yagoinfo': 'http://yago-knowledge.org/resource/infobox/en/'
                        })


def join(join_type):
    dbpedia_actors = graph1.feature_domain_range('dbpp:starring', 'film1', 'actor1') \
        .expand('actor1', [('dbpp:birthPlace', 'actor_country1'), ('dbpp:name', 'name')]) \
        .filter({'actor_country1': ['regex(str(?actor_country1), "USA")']})

    yago_actors = graph2.feature_domain_range('yago:actedIn', 'actor2', 'film2') \
        .expand('actor2', [('yago:isCitizenOf', 'actor_country2'), ('yagoinfo:name', 'name')]) \
        .filter({'actor_country2': ['= yago:United_States']})

    actors = dbpedia_actors.join(yago_actors, 'name', join_type=join_type)
    endpoint = 'http://10.161.202.101:8890/sparql/'
    output_format = HttpClientDataFormat.PANDAS_DF
    client = HttpClient(endpoint_url=endpoint, return_format=output_format)
    df = actors.execute(client, return_format=output_format)
    print(df.shape)
    print(actors.to_sparql())

join(JoinType.InnerJoin)
 









