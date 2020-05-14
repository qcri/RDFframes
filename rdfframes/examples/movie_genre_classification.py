import time
from rdfframes.knowledge_graph import KnowledgeGraph
from rdfframes.utils.constants import JoinType

from rdfframes.client.http_client import HttpClientDataFormat, HttpClient


def movies_with_american_actors():
    start = time.time()
    graph = KnowledgeGraph(graph_uri='http://dbpedia.org',
                           prefixes={'dcterms': 'http://purl.org/dc/terms/',
                                     'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
                                     'dbpp': 'http://dbpedia.org/property/',
                                     'dbpr': 'http://dbpedia.org/resource/',
                                     'dbpo': 'http://dbpedia.org/ontology/'})
    graph = KnowledgeGraph(graph_name='dbpedia')

    dataset = graph.feature_domain_range('dbpp:starring', 'film', 'actor')\
        .expand('actor', [('dbpp:birthPlace', 'actor_country'), ('rdfs:label', 'actor_name')])\
        .expand('film', [('rdfs:label', 'film_name'), ('dcterms:subject', 'subject'),
                         ('dbpp:country', 'film_country'), ('dbpo:genre', 'genre', True)])\
        .cache()
    # 26928 Rows. -- 4273 msec.
    american_actors = dataset.filter({'actor_country': ['regex(str(?actor_country), "USA")']})

    # 1606 Rows. -- 7659 msec.
    prolific_actors = dataset.group_by(['actor'])\
        .count('film', 'film_count', unique=True).filter({'film_count': ['>= 20']})

    #663,769 Rows. -- 76704 msec.
    films = american_actors.join(prolific_actors, join_col_name1='actor', join_type=JoinType.OuterJoin)\
        .join(dataset, join_col_name1='actor')
    #.select_cols(['film_name', 'actor_name', 'genre'])

    sparql_query = films.to_sparql()

    print(sparql_query)

    endpoint = 'http://10.161.202.101:8890/sparql/'
    output_format = HttpClientDataFormat.PANDAS_DF

    client = HttpClient(endpoint_url=endpoint, return_format=output_format)
    # [663769 rows x 8 columns]
    df = films.execute(client, return_format=output_format)
    print("duration = {} sec".format(time.time() - start))
    print(df)


def movies_with_american_actors_optional():
    graph = KnowledgeGraph(graph_uri='http://dbpedia.org',
                           prefixes={'dcterms': 'http://purl.org/dc/terms/',
                                     'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
                                     'dbpprop': 'http://dbpedia.org/property/',
                                     'dbpr': 'http://dbpedia.org/resource/'})

    dataset = graph.feature_domain_range('dbpprop:starring', domain_col_name='film', range_col_name='actor')\
        .expand('actor', [
            RDFPredicate('dbpprop:birthPlace', 'actor_country', optional=True),
            RDFPredicate('rdfs:label', 'actor_name', optional=True)])\
        .expand('film', [
            RDFPredicate('rdfs:label', 'film_name', optional=True),
            RDFPredicate('dcterms:subject', 'subject', optional=True),
            RDFPredicate('dbpprop:country', 'film_country', optional=True)])\
        .cache()
    # 26928 Rows. -- 4273 msec.
    american_actors = dataset.filter({'actor_country': ['regex(str(?actor_country), "USA")']})

    # 1606 Rows. -- 7659 msec.
    prolific_actors = dataset.group_by(['actor'])\
        .count('film', 'film_count', unique=True).filter({'film_count': ['>= 20', '<=30']})

    # 663769 Rows. -- 76511 msec.
    films = american_actors.join(prolific_actors, join_col_name1='actor', join_type=JoinType.OuterJoin)\
        .join(dataset, join_col_name1='actor')

    sparql_query = films.to_sparql()

    print(sparql_query)

    endpoint = 'http://10.161.202.101:8890/sparql/'
    output_format = HttpClientDataFormat.PANDAS_DF

    client = HttpClient(endpoint_url=endpoint, return_format=output_format)
    df = dataset.execute(client, return_format=output_format)
    print(df)


#movies_with_american_actors_optional()
movies_with_american_actors()
