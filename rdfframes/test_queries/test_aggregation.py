from rdfframes.knowledge_graph import KnowledgeGraph
from rdfframes.dataset.rdfpredicate import RDFPredicate
from rdfframes.dataset.aggregation_fn_data import AggregationData
from rdfframes.utils.constants import JoinType
from rdfframes.client.http_client import HttpClient, HttpClientDataFormat


def test_simple_query():
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
        RDFPredicate('sioc:has_creater', 'tweep', False),
        RDFPredicate('sioc:content', 'text', False)
    ])
    dataset = dataset.group_by(['tweep']).count(src_col_name='tweep', new_col_name='tweep_count', unique=True)
    sparql_query = dataset.to_sparql()
    print("sparql_query 1 =\n{}\n".format(sparql_query))


    # return all the instances of the tweet class
    dataset = graph.entities(class_name='sioc:microblogPost',
                             new_dataset_name='tweets',
                             entities_col_name='tweet')
    dataset = dataset.expand(src_col_name='tweet', predicate_list=[
        RDFPredicate('sioc:has_creater', 'tweep', False),
        RDFPredicate('sioc:content', 'text', False)
    ])
    dataset = dataset.group_by(['tweep'])\
        .count(unique=True)
    sparql_query = dataset.to_sparql()
    print("sparql_query 2 =\n{}\n".format(sparql_query))


    # return all the instances of the tweet class
    dataset = graph.entities(class_name='sioc:microblogPost',
                             new_dataset_name='tweets',
                             entities_col_name='tweet')
    dataset = dataset.expand(src_col_name='tweet', predicate_list=[
        RDFPredicate('sioc:has_creater', 'tweep', False),
        RDFPredicate('sioc:content', 'text', False)
    ])
    dataset = dataset.count(unique=True)
    sparql_query = dataset.to_sparql()
    print("sparql_query 3 =\n{}\n".format(sparql_query))

    # return all the instances of the tweet class
    dataset = graph.entities(class_name='sioc:microblogPost',
                             new_dataset_name='tweets',
                             entities_col_name='tweet')
    dataset = dataset.expand(src_col_name='tweet', predicate_list=[
        RDFPredicate('sioc:has_creater', 'tweep', False),
        RDFPredicate('sioc:content', 'text', False)
    ])
    dataset = dataset.group_by(['tweep']).count(src_col_name='tweet', new_col_name='tweet_count', unique=True)
    sparql_query = dataset.to_sparql()
    print("sparql_query 4 =\n{}\n".format(sparql_query))

    # return all the instances of the tweet class
    dataset = graph.entities(class_name='sioc:microblogPost',
                             new_dataset_name='tweets',
                             entities_col_name='tweet')
    dataset = dataset.expand(src_col_name='tweet', predicate_list=[
        RDFPredicate('sioc:has_creater', 'tweep', False)
    ])
    dataset = dataset.group_by(['tweep']).count(src_col_name='tweet', new_col_name='tweet_count', unique=True)
    dataset = dataset.expand(src_col_name='tweep', predicate_list=[RDFPredicate('sioc:content', 'text', False)])
    sparql_query = dataset.to_sparql()
    print("sparql_query 5 =\n{}\n".format(sparql_query))

if __name__ == '__main__':
    test_simple_query()
