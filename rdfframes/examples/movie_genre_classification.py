import time
from rdfframes.knowledge_graph import KnowledgeGraph
from rdfframes.utils.constants import JoinType

from rdfframes.client.http_client import HttpClientDataFormat, HttpClient

def movies_with_american_actors_cache():
    graph = KnowledgeGraph(graph_name='dbpedia')
    dataset = graph.feature_domain_range('dbpp:starring', 'movie', 'actor')\
        .expand('actor', [('dbpp:birthPlace', 'actor_country'), ('rdfs:label', 'actor_name')])\
        .expand('movie', [('rdfs:label', 'movie_name'), ('dcterms:subject', 'subject'),
                         ('dbpp:country', 'movie_country'), ('dbpp:genre', 'genre', True)])\
        .cache()
    # 26928 Rows. -- 4273 msec.
    american_actors = dataset.filter({'actor_country': ['regex(str(?actor_country), "USA")']})

    # 1606 Rows. -- 7659 msec.
    prolific_actors = dataset.group_by(['actor'])\
        .count('movie', 'movie_count', unique=True).filter({'movie_count': ['>= 200']})

    #663,769 Rows. -- 76704 msec.
    movies = american_actors.join(prolific_actors, join_col_name1='actor', join_type=JoinType.OuterJoin)\
        .join(dataset, join_col_name1='actor')
    #.select_cols(['movie_name', 'actor_name', 'genre'])

    sparql_query = movies.to_sparql()
    print(sparql_query)

def movies_with_american_actors():
    graph = KnowledgeGraph(graph_name='dbpedia')

    dataset1 = graph.feature_domain_range('dbpp:starring', 'movie1', 'actor')\
        .expand('actor', [('dbpp:birthPlace', 'actor_country1'), ('rdfs:label', 'actor_name1')])\
        .expand('movie1', [('rdfs:label', 'movie_name1'), ('dcterms:subject', 'subject1'),
                         ('dbpp:country', 'movie_country1'), ('dbpp:genre', 'genre1', True)])
    # 26928 Rows. -- 4273 msec.
    american_actors = dataset1.filter({'actor_country1': ['regex(str(?actor_country1), "USA")']})

    # 1606 Rows. -- 7659 msec.
    dataset2 = graph.feature_domain_range('dbpp:starring', 'movie2', 'actor')\
        .expand('actor', [('dbpp:birthPlace', 'actor_country2'), ('rdfs:label', 'actor_name2')])\
        .expand('movie2', [('rdfs:label', 'movie_name2'), ('dcterms:subject', 'subject2'),
                         ('dbpp:country', 'movie_country2'), ('dbpp:genre', 'genre2', True)])
    prolific_actors = dataset2.group_by(['actor'])\
        .count('movie2', 'movie_count2', unique=True).filter({'movie_count2': ['>= 200']})

    #663,769 Rows. -- 76704 msec.
    movies = american_actors.join(prolific_actors, join_col_name1='actor', join_type=JoinType.OuterJoin)\
    #    .join(dataset, join_col_name1='actor')
    #.select_cols(['movie_name', 'actor_name', 'genre'])

    sparql_query = movies.to_sparql()
    print(sparql_query)

def movies_with_american_actors_optional():
    graph = KnowledgeGraph(graph_uri='http://dbpedia.org',
                           prefixes={'dcterms': 'http://purl.org/dc/terms/',
                                     'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
                                     'dbpprop': 'http://dbpedia.org/property/',
                                     'dbpr': 'http://dbpedia.org/resource/'})

    dataset = graph.feature_domain_range('dbpprop:starring', domain_col_name='movie', range_col_name='actor')\
        .expand('actor', [
            RDFPredicate('dbpprop:birthPlace', 'actor_country', optional=True),
            RDFPredicate('rdfs:label', 'actor_name', optional=True)])\
        .expand('movie', [
            RDFPredicate('rdfs:label', 'movie_name', optional=True),
            RDFPredicate('dcterms:subject', 'subject', optional=True),
            RDFPredicate('dbpprop:country', 'movie_country', optional=True)])\
        .cache()
    # 26928 Rows. -- 4273 msec.
    american_actors = dataset.filter({'actor_country': ['regex(str(?actor_country), "USA")']})

    # 1606 Rows. -- 7659 msec.
    prolific_actors = dataset.group_by(['actor'])\
        .count('movie', 'movie_count', unique=True).filter({'movie_count': ['>= 20', '<=30']})

    # 663769 Rows. -- 76511 msec.
    movies = american_actors.join(prolific_actors, join_col_name1='actor', join_type=JoinType.OuterJoin)\
        .join(dataset, join_col_name1='actor')

    sparql_query = movies.to_sparql()

    print(sparql_query)

    endpoint = 'http://10.161.202.101:8890/sparql/'
    output_format = HttpClientDataFormat.PANDAS_DF

    client = HttpClient(endpoint_url=endpoint, return_format=output_format)
    df = dataset.execute(client, return_format=output_format)
    print(df)


#movies_with_american_actors_optional()
start = time.time()
movies_with_american_actors()
duration = time.time()-start
print("Duration = {} sec".format(duration))
