from rdfframes.knowledge_graph import KnowledgeGraph
from rdfframes.dataset.rdfpredicate import RDFPredicate
from rdfframes.utils.constants import JoinType

from rdfframes.client.http_client import HttpClientDataFormat, HttpClient


def movies_with_american_actors():
    graph = KnowledgeGraph(graph_name='dbpedia',graph_uri='http://dbpedia.org',
                           prefixes={'dcterms': 'http://purl.org/dc/terms/',
                                     'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
                                     'dbpprop': 'http://dbpedia.org/property/'})

    dataset = graph.feature_domain_range('dbpprop:starring', domain_col_name='film', range_col_name='actor')\
        .expand('actor', [RDFPredicate('dbpprop:birthPlace', 'actor_country'), RDFPredicate('rdfs:label', 'actor_name')])\
        .expand('film', [RDFPredicate('rdfs:label', 'film_name'), RDFPredicate('dcterms:subject', 'subject'),
                         RDFPredicate('dbpprop:country', 'film_country')])\
        .cache()

    american_actors = dataset.filter({'actor_country': ['= USA']})

    prolific_actors = dataset.group_by(['actor'])\
        .count('film', 'film_count', unique=True).filter({'film_count': ['>= 10', '<=30']})

    films = american_actors.join(prolific_actors, join_col_name1='actor', join_type=JoinType.OuterJoin)\
        .join(dataset, join_col_name1='actor')

    sparql_query = films.to_sparql()

    print(sparql_query)

    endpoint = 'http://10.161.202.101:8890/sparql/'
    output_format = HttpClientDataFormat.PANDAS_DF

    client = HttpClient(endpoint_url=endpoint, return_format=output_format)
    df = dataset.execute(client, return_format=output_format)


movies_with_american_actors()
