import time

from rdframe.knowledge_graph import KnowledgeGraph
from rdframe.dataset.rdfpredicate import RDFPredicate
from rdframe.dataset.aggregation_fn_data import AggregationData
from rdframe.utils.constants import JoinType


def test_simple_query():
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
                             #class_col_name='tweet_class',
                             entities_col_name='tweet')
    sparql_query = dataset.to_sparql()
    print("sparql_query 1 =\n{}\n".format(sparql_query))


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
                             #class_col_name='tweet_class',
                             entities_col_name='tweet')
    dataset = dataset.expand(src_col_name='tweet', predicate_list=[
        RDFPredicate('sioc:has_creater','tweep',False),
        RDFPredicate('sioc:content' ,'text',True)
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
                             #class_col_name='tweet_class',
                             entities_col_name='tweet')
    dataset = dataset.expand(src_col_name='tweet', predicate_list=[
        RDFPredicate('sioc:has_creater','tweep',False),
        RDFPredicate('sioc:content' ,'text',False)
    ])

    dataset2 = graph.entities(class_name='sioct:tweet',
                             new_dataset_name='dataset2',
                             # class_col_name='tweet_class',
                             entities_col_name='tweep')
    dataset2 = dataset2.expand(src_col_name='tweep', predicate_list=[
        RDFPredicate('sioc:has_name', 'name', False)
        #RDFPredicate('sioc:content', 'text', True)
    ])

    dataset.join(dataset2,'tweep','tweep','tweep', JoinType.InnerJoin)

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
                             #class_col_name='tweet_class',
                             entities_col_name='tweet')
    dataset = dataset.expand(src_col_name='tweet', predicate_list=[
        RDFPredicate('sioc:has_creater', 'tweep',False),
        RDFPredicate('sioc:content', 'text',True)
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
                             #class_col_name='tweet_class',
                             entities_col_name='tweet')
    dataset = dataset.expand(src_col_name='tweet', predicate_list=[
        RDFPredicate('sioc:has_creater', 'tweep',False),
        RDFPredicate('sioc:content', 'text',True)])\
        .filter({'text': [' >= \"aa\"']})\
        .select_cols(['tweet', 'text'])
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
                             #class_col_name='tweet_class',
                             entities_col_name='tweet')
    dataset = dataset.expand(src_col_name='tweet', predicate_list=[
        RDFPredicate('sioc:has_creater', 'tweep',True),
        RDFPredicate('sioc:content', 'text',False)
    ])
    dataset.sort({'tweep': 'ASC'}).limit(10).offset(5)
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
        RDFPredicate('sioc:has_creater', 'tweep',True),
        RDFPredicate('sioc:content', 'text',False)
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
    dataset = graph.entities(class_name='sioct:microblogPost',
                             new_dataset_name='tweets',
                             #class_col_name='tweet_class',
                             entities_col_name='tweet')
    dataset = dataset.expand(src_col_name='tweet', predicate_list=[
        RDFPredicate('sioc:has_creater', 'tweep', False),
        RDFPredicate('sioc:content', 'text', False)
    ])
    grouped_dataset = dataset.group_by(['tweep'])\
        .count([AggregationData('tweet', 'tweets_count')])
    sparql_query = grouped_dataset.to_sparql()
    print("sparql_query 1 =\n{}\n".format(sparql_query))

if __name__ == '__main__':
   #test_simple_query()
   #test_expand_query()
   #test_select_query()
   #test_filter_query()
   test_join_query()
    #test_sort_limit_offset_query()
    #test_groupby_query()
   # test_groupby_aggregation_query()


