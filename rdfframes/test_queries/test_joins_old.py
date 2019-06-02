import time

from rdfframes.knowledge_graph import KnowledgeGraph
from rdfframes.dataset.rdfpredicate import RDFPredicate
from rdfframes.dataset.aggregation_fn_data import AggregationData
from rdfframes.utils.constants import JoinType
from rdfframes.client.http_client import HttpClient, HttpClientDataFormat


def test_expandable_expandable_join(join_type, optional1=False, optional2=False):
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
        RDFPredicate('sioc:has_creater', 'tweep', False),
        RDFPredicate('sioc:content', 'text', optional1)
    ]).select_cols(['tweep'])

    dataset2 = graph.entities(class_name='sioct:tweeter',
                             new_dataset_name='dataset2',
                             entities_col_name='tweeter')
    dataset2 = dataset2.expand(src_col_name='tweeter', predicate_list=[
        RDFPredicate('sioc:has_name', 'name', optional2)
    ])

    dataset.join(dataset2,'tweep','tweeter','tweep', join_type)

    sparql_query = dataset.to_sparql()
    print("SPARQL query with {} =\n{}\n".format(join_type, sparql_query))


def test_join_instead_of_expand(join_type):
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
    dataset1 = graph.entities(class_name='sioct:microblogPost',
                             new_dataset_name='dataset',
                             entities_col_name='tweet')\
        .expand(src_col_name='tweet', predicate_list=[RDFPredicate('sioc:has_creater', 'tweep', False)])

    dataset2 = graph.entities(class_name='sioct:microblogPost',
                             new_dataset_name='dataset',
                             entities_col_name='tweet')\
        .expand(src_col_name='tweet', predicate_list=[RDFPredicate('sioc:content', 'text', False)])

    dataset2.join(dataset1, 'tweet', 'tweet', 'tweet', join_type)

    sparql_query = dataset2.to_sparql()
    print("SPARQL query with {} =\n{}\n".format(join_type, sparql_query))


def test_expandable_expandable_3_joins(join_type):
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
    dataset = graph.entities(class_name='sioct:microblogPost',
                             new_dataset_name='dataset1',
                             entities_col_name='tweet')
    dataset = dataset.expand(src_col_name='tweet', predicate_list=[
        RDFPredicate('sioc:has_creater', 'tweep', False),
        RDFPredicate('sioc:content', 'text', False)
    ])

    dataset2 = graph.entities(class_name='sioct:tweep',
                              new_dataset_name='dataset2',
                              entities_col_name='tweep')
    dataset2 = dataset2.expand(src_col_name='tweep', predicate_list=[
        RDFPredicate('sioc:has_name', 'name', False),
        RDFPredicate('sioc:has_follower', 'follower', False)
    ])

    dataset2.join(dataset, 'tweep', 'tweep', 'tweep', join_type)

    dataset3 = graph.entities(class_name='sioct:tweep',
                              new_dataset_name='dataset3',
                              entities_col_name='tweeter')
    dataset3 = dataset3.expand(src_col_name='tweeter', predicate_list=[
        RDFPredicate('sioc:has_id', 'id', False)
    ])

    dataset3.join(dataset2, 'tweeter', 'follower', 'follower', join_type)

    sparql_query = dataset3.to_sparql()
    print("SPARQL query with {} =\n{}\n".format(join_type, sparql_query))


def test_expandable_expandable_join_w_selectcols():
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
            RDFPredicate('sioc:has_creater', 'tweep', False),
            RDFPredicate('sioc:content', 'text', False)
        ]).select_cols(['tweep', 'text'])

        dataset2 = graph.entities(class_name='sioct:tweeter',
                                  new_dataset_name='dataset2',
                                  entities_col_name='tweep')
        dataset2 = dataset2.expand(src_col_name='tweep', predicate_list=[
            RDFPredicate('sioc:has_name', 'name', False)
        ]).select_cols(['tweep', 'name'])

        dataset.join(dataset2, 'tweep', 'tweep', 'tweep', JoinType.InnerJoin)

        sparql_query = dataset.to_sparql()
        print("SPARQL query =\n{}\n".format(sparql_query))


def test_expandable_grouped_join(join_type):
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
    dataset = graph.entities(class_name='sioct:microblogPost',
                             new_dataset_name='dataset1',
                             entities_col_name='tweet')
    dataset = dataset.expand(src_col_name='tweet', predicate_list=[
        RDFPredicate('sioc:has_creater', 'tweep', False),
        RDFPredicate('sioc:content', 'text', False)
    ])

    dataset2 = graph.entities(class_name='sioct:microblogPost',
                             new_dataset_name='tweets',
                             entities_col_name='tweet')
    dataset2 = dataset2.expand(src_col_name='tweet', predicate_list=[
        RDFPredicate('sioc:has_creater', 'tweeter')
    ]).group_by(['tweeter']).count('tweet', 'tweets_count').filter(
        conditions_dict={'tweets_count': ['>= {}'.format(200), '<= {}'.format(300)]})
    dataset.join(dataset2, 'tweep', 'tweeter', 'user', join_type)
    dataset.select_cols(['user'])

    sparql_query = dataset.to_sparql()
    print("SPARQL query with {} =\n{}\n".format(join_type, sparql_query))

    # endpoint = 'http://10.161.202.101:8890/sparql/'
    # port = 8890
    # output_format = HttpClientDataFormat.PANDAS_DF
    # max_rows = 1000000
    # timeout = 12000
    # default_graph_url = 'http://twitter.com/'
    # client = HttpClient(endpoint_url=endpoint,
    #                     port=port,
    #                     return_format=output_format,
    #                     timeout=timeout,
    #                     default_graph_uri=default_graph_url,
    #                     max_rows=max_rows
    #                     )
    #
    # df = dataset.execute(client, return_format=output_format)
    # print(df.head(10))


def test_grouped_expandable_join(join_type):
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
    dataset = graph.entities(class_name='sioct:microblogPost',
                             new_dataset_name='dataset1',
                             entities_col_name='tweet')
    dataset = dataset.expand(src_col_name='tweet', predicate_list=[
        RDFPredicate('sioc:has_creater', 'tweep', False),
        RDFPredicate('sioc:content', 'text', False)
    ])

    dataset2 = graph.entities(class_name='sioct:microblogPost',
                             new_dataset_name='tweets',
                             entities_col_name='tweet')
    dataset2 = dataset2.expand(src_col_name='tweet', predicate_list=[
        RDFPredicate('sioc:has_creater', 'tweeter')
    ]).group_by(['tweeter']).count('tweet', 'tweets_count').filter(
        conditions_dict={'tweets_count': ['>= {}'.format(200), '<= {}'.format(300)]})
    dataset2= dataset2.expand(src_col_name='tweeter', predicate_list=[ RDFPredicate('rdf:type', 'sioc:UserAccount')])
    dataset2.join(dataset, 'tweeter', 'tweep', 'user', join_type)
    dataset2.select_cols(['user'])


    sparql_query = dataset2.to_sparql()
    print("SPARQL query with {} =\n{}\n".format(join_type, sparql_query))


def test_grouped_grouped_join(join_type):
    # create a knowledge graph to store the graph uri and prefixes
    graph = KnowledgeGraph('twitter', 'https://twitter.com/',
                           prefixes={
                               "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                               "sioc": "http://rdfs.org/sioc/ns#",
                               "sioct": "http://rdfs.org/sioc/types#",
                           })
    # return all the instances of the tweet class
    dataset = graph.entities(class_name='sioct:microblogPost',
                             new_dataset_name='dataset1',
                             entities_col_name='tweet')
    dataset = dataset.expand(src_col_name='tweet', predicate_list=[
        RDFPredicate('sioc:has_creater', 'tweep', False),
        RDFPredicate('sioc:content', 'text', False)
    ]).group_by(['tweep']).count(
        aggregation_fn_data=[AggregationData('tweet', 'tweets_count')]).filter(
        conditions_dict={'tweets_count': ['>= {}'.format(1000)]})

    graph2 = KnowledgeGraph('twitter2', 'https://twitter2.com/',
                           prefixes={
                               "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                               "sioc2": "http://rdfs.org/sioc2/ns#",
                               "sioct2": "http://rdfs.org/sioc2/types#",
                           })
    dataset2 = graph2.entities(class_name='sioct2:twitterPost',
                             new_dataset_name='tweets',
                             entities_col_name='tweet')
    dataset2 = dataset2.expand(src_col_name='tweet', predicate_list=[
        RDFPredicate('sioc2:has_creater', 'tweeter')
    ]).group_by(['tweeter']).count(
        aggregation_fn_data=[AggregationData('tweet', 'tweets_count2')]).filter(
        conditions_dict={'tweets_count2': ['>= {}'.format(200), '<= {}'.format(300)]})
    dataset.join(dataset2, 'tweep', 'tweeter', 'user', join_type)
    # dataset.select_cols(['user'])

    sparql_query = dataset.to_sparql()
    print("SPARQL query with {} =\n{}\n".format(join_type, sparql_query))

if __name__ == '__main__':
    test_expandable_expandable_join(JoinType.InnerJoin)
    # test_expandable_expandable_join(JoinType.OuterJoin)
    test_expandable_expandable_join(JoinType.LeftOuterJoin)
    # test_expandable_expandable_join(JoinType.RightOuterJoin)
    # test_expandable_expandable_join(JoinType.InnerJoin, True, True)
    # test_expandable_expandable_join(JoinType.LeftOuterJoin, True, True)
    # test_expandable_expandable_join(JoinType.RightOuterJoin, True, True)
    # test_join_instead_of_expand(JoinType.InnerJoin)
    # test_expandable_expandable_3_joins(JoinType.InnerJoin)
    # test_expandable_expandable_join_w_selectcols()
    # test_expandable_grouped_join(JoinType.InnerJoin)
    # test_expandable_grouped_join(JoinType.LeftOuterJoin)
    # test_expandable_grouped_join(JoinType.RightOuterJoin)
    # test_expandable_grouped_join(JoinType.OuterJoin)
    # test_grouped_expandable_join(JoinType.InnerJoin)
    # test_grouped_expandable_join(JoinType.LeftOuterJoin)
    ### test the join on non-groupby columns
    # test_grouped_expandable_join(JoinType.RightOuterJoin)
    #test_grouped_grouped_join(JoinType.InnerJoin)
    #test_grouped_grouped_join(JoinType.LeftOuterJoin)
    # test_grouped_grouped_join(JoinType.RightOuterJoin)
    # test_grouped_grouped_join(JoinType.OuterJoin)


