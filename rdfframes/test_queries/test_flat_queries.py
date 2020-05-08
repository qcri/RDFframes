import time

from rdfframes.knowledge_graph import KnowledgeGraph
from rdfframes.utils.constants import JoinType
from rdfframes.client.http_client import HttpClient, HttpClientDataFormat


def test_simple_query():
    start = time.time()
    # create a knowledge graph to store the graph uri and prefixes
    graph = KnowledgeGraph('twitter', 'https://twitter.com/',
                           prefixes={
                               "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                               "sioc": "http://rdfs.org/sioc/ns#",
                               "sioct": "http://rdfs.org/sioc/types#",
                               "to": "http://twitter.com/ontology/",
                               "dcterms": "http://purl.org/dc/terms/",
                               "xsd": "http://www.example.org/",
                               "foaf": "http://xmlns.com/foaf/0.1/"
                           })
    # return all the instances of the tweet class
    dataset = graph.entities(class_name='sioct:MicroblogPost',
                             new_dataset_name='tweets',
                             entities_col_name='tweet')
    sparql_query = dataset.to_sparql()
    print("sparql_query to return tweets =\n{}\n".format(sparql_query))

    endpoint = 'http://10.161.202.101:8890/sparql/'
    port = 8890
    output_format = HttpClientDataFormat.PANDAS_DF
    max_rows = 1000000
    timeout = 12000
    default_graph_url = 'http://twitter.com/'
    client = HttpClient(endpoint_url=endpoint,
                        port=port,
                        return_format=output_format,
                        timeout=timeout,
                        default_graph_uri=default_graph_url,
                        max_rows=max_rows
                        )

    #df = dataset.execute(client, return_format=output_format)
    duration = start - time.time()
    print("Done in {} secs".format(duration))
    #print(df.head(10))


def test_expand_query():
    start = time.time()
    # create a knowledge graph to store the graph uri and prefixes
    graph = KnowledgeGraph('twitter', 'https://twitter.com',
                           prefixes={
                               "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                               "sioc": "http://rdfs.org/sioc/ns#",
                               "sioct": "http://rdfs.org/sioc/types#",
                               "to": "http://twitter.com/ontology/",
                               "dcterms": "http://purl.org/dc/terms/",
                               "xsd": "http://www.example.org/",
                               "foaf": "http://xmlns.com/foaf/0.1/"
                           })
    # return all the instances of the tweet class
    dataset = graph.entities(class_name='sioct:microblogPost',
                             new_dataset_name='tweets',
                             entities_col_name='tweet')
    dataset = dataset.expand(src_col_name='tweet', predicate_list=[
        ('sioc:has_creater','tweep',False),
        ('sioc:content','text',True)
    ])
    sparql_query = dataset.to_sparql()
    print("sparql_query 1 =\n{}\n".format(sparql_query))


def test_join_query():
    start = time.time()
    # create a knowledge graph to store the graph uri and prefixes
    graph = KnowledgeGraph('twitter', 'https://twitter.com',
                           prefixes={
                               "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                               "sioc": "http://rdfs.org/sioc/ns#",
                               "sioct": "http://rdfs.org/sioc/types#",
                               "to": "http://twitter.com/ontology/",
                               "dcterms": "http://purl.org/dc/terms/",
                               "xsd": "http://www.example.org/",
                               "foaf": "http://xmlns.com/foaf/0.1/"
                           })
    # return all the instances of the tweet class
    dataset = graph.entities(class_name='sioct:microblogPost',
                             new_dataset_name='dataset1',
                             entities_col_name='tweet')
    dataset = dataset.expand(src_col_name='tweet', predicate_list=[
        ('sioc:has_creater', 'tweep', False),
        ('sioc:content', 'text', False)
    ])

    dataset2 = graph.entities(class_name='sioc:UserAccount',
                             new_dataset_name='dataset2',
                             entities_col_name='tweep')
    dataset2 = dataset2.expand(src_col_name='tweep', predicate_list=[
        ('sioc:has_name', 'name', False)
    ])

    # TODO: put the whole first dataset in one optional block. now, its in multiple optional blocks
    dataset.join(dataset2,'tweep','tweep','tweep', JoinType.RightOuterJoin)

    sparql_query = dataset.to_sparql()
    print("sparql_query 1 =\n{}\n".format(sparql_query))


def test_select_query():
    start = time.time()
    # create a knowledge graph to store the graph uri and prefixes
    graph = KnowledgeGraph('twitter', 'https://twitter.com',
                           prefixes={
                               "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                               "sioc": "http://rdfs.org/sioc/ns#",
                               "sioct": "http://rdfs.org/sioc/types#",
                               "to": "http://twitter.com/ontology/",
                               "dcterms": "http://purl.org/dc/terms/",
                               "xsd": "http://www.example.org/",
                               "foaf": "http://xmlns.com/foaf/0.1/"
                           })
    # return all the instances of the tweet class
    dataset = graph.entities(class_name='sioct:microblogPost',
                             new_dataset_name='tweets',
                             entities_col_name='tweet')
    dataset = dataset.expand(src_col_name='tweet', predicate_list=[
        ('sioc:has_creater', 'tweep',False),
        ('sioc:content', 'text',True)
    ])
    dataset.select_cols(['text', 'tweet'])
    sparql_query = dataset.to_sparql()
    print("sparql_query 1 =\n{}\n".format(sparql_query))


def test_filter_query():
    start = time.time()
    # create a knowledge graph to store the graph uri and prefixes
    graph = KnowledgeGraph('twitter', 'https://twitter.com',
                           prefixes={
                               "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                               "sioc": "http://rdfs.org/sioc/ns#",
                               "sioct": "http://rdfs.org/sioc/types#",
                               "to": "http://twitter.com/ontology/",
                               "dcterms": "http://purl.org/dc/terms/",
                               "xsd": "http://www.example.org/",
                               "foaf": "http://xmlns.com/foaf/0.1/"
                           })
    # return all the instances of the tweet class
    dataset = graph.entities(class_name='sioct:microblogPost',
                             new_dataset_name='tweets',
                             entities_col_name='tweet')
    dataset = dataset.expand(src_col_name='tweet', predicate_list=[
        ('sioc:has_creater', 'tweep', False),
        ('sioc:content', 'text', True)])\
        .filter({'text': [' >= \"aa\"']})\
        .select_cols(['tweet', 'text'])
    # TODO: make sure the order of filter when called before a join or optional is done before the join or the optional
    #  and when called after the join or optional are done after it
    sparql_query = dataset.to_sparql()
    print("sparql_query 1 =\n{}\n".format(sparql_query))


def test_sort_limit_offset_query():
    start = time.time()
    # create a knowledge graph to store the graph uri and prefixes
    graph = KnowledgeGraph('twitter', 'https://twitter.com',
                           prefixes={
                               "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                               "sioc": "http://rdfs.org/sioc/ns#",
                               "sioct": "http://rdfs.org/sioc/types#",
                               "to": "http://twitter.com/ontology/",
                               "dcterms": "http://purl.org/dc/terms/",
                               "xsd": "http://www.example.org/",
                               "foaf": "http://xmlns.com/foaf/0.1/"
                           })
    # return all the instances of the tweet class
    dataset = graph.entities(class_name='sioct:microblogPost',
                             new_dataset_name='tweets',
                             entities_col_name='tweet')
    dataset = dataset.expand(src_col_name='tweet', predicate_list=[
        ('sioc:has_creater', 'tweep',True),
        ('sioc:content', 'text', False)
    ])
    dataset.sort({'tweep': 'ASC'}).limit(10).offset(5)
    # TODO: do we care about limit after or before an offset? Do we allow one limit in each query?
    sparql_query = dataset.to_sparql()
    print("sparql_query 1 =\n{}\n".format(sparql_query))


def test_groupby_query():
    start = time.time()
    # create a knowledge graph to store the graph uri and prefixes
    graph = KnowledgeGraph('twitter', 'https://twitter.com',
                           prefixes={
                               "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                               "sioc": "http://rdfs.org/sioc/ns#",
                               "sioct": "http://rdfs.org/sioc/types#",
                               "to": "http://twitter.com/ontology/",
                               "dcterms": "http://purl.org/dc/terms/",
                               "xsd": "http://www.example.org/",
                               "foaf": "http://xmlns.com/foaf/0.1/"
                           })
    # return all the instances of the tweet class
    dataset = graph.entities(class_name='sioct:microblogPost',
                             new_dataset_name='tweets',
                            # class_col_name='tweet_class',
                             entities_col_name='tweet')
    dataset = dataset.expand(src_col_name='tweet', predicate_list=[
        ('sioc:has_creater', 'tweep',True),
        ('sioc:content', 'text',False)
    ])
    dataset = dataset.group_by(['tweep'])
    sparql_query = dataset.to_sparql()
    print("sparql_query 1 =\n{}\n".format(sparql_query))


def test_groupby_aggregation_query():
    start = time.time()
    # create a knowledge graph to store the graph uri and prefixes
    graph = KnowledgeGraph('twitter', 'https://twitter.com',
                           prefixes={
                               "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                               "sioc": "http://rdfs.org/sioc/ns#",
                               "sioct": "http://rdfs.org/sioc/types#",
                               "to": "http://twitter.com/ontology/",
                               "dcterms": "http://purl.org/dc/terms/",
                               "xsd": "http://www.example.org/",
                               "foaf": "http://xmlns.com/foaf/0.1/"
                           })
    # return all the instances of the tweet class
    dataset = graph.entities(class_name='sioc:microblogPost',
                             new_dataset_name='tweets',
                             entities_col_name='tweet')
    dataset = dataset.expand(src_col_name='tweet', predicate_list=[
        ('sioc:has_creater', 'tweep', False),
        ('sioc:content', 'text', False)
    ])
    grouped_dataset = dataset.group_by(['tweep'])\
        .count('tweet', 'tweets_count')\
        .select_cols(['tweep'])
    # TODO: when select after groupby and aggregation, remove the non-selected columns from the select clause
    #  including aggregation columns
    sparql_query = grouped_dataset.to_sparql()
    print("sparql_query 1 =\n{}\n".format(sparql_query))

if __name__ == '__main__':
    #test_simple_query()
    #test_expand_query()
    #test_expand_query()
    #test_select_query()
    test_filter_query()
    #test_join_query()
    #test_sort_limit_offset_query()
    #test_groupby_query()
    #test_groupby_aggregation_query()


