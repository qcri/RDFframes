import time

from rdframe.knowledge_graph import KnowledgeGraph
from rdframe.dataset.rdfpredicate import RDFPredicate
from rdframe.dataset.aggregation_fn_data import AggregationData
from rdframe.utils.constants import JoinType

##FIXME: this function does not change the sparql variable names to include the new join column name
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
    ])

    dataset2 = graph.entities(class_name='sioct:tweeter',
                             new_dataset_name='dataset2',
                             entities_col_name='tweeter')
    dataset2 = dataset2.expand(src_col_name='tweeter', predicate_list=[
        RDFPredicate('sioc:has_name', 'name', optional2)
    ])

    dataset.join(dataset2,'tweep','tweeter','tweep', join_type)

    sparql_query = dataset.to_sparql()
    print("SPARQL query with {} =\n{}\n".format(join_type, sparql_query))


##FIXME: this function causes an infinite loop
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
    dataset = graph.entities(class_name='sioct:microblogPost',
                             new_dataset_name='dataset1',
                             entities_col_name='tweet')
    dataset1 = dataset.expand(src_col_name='tweet', predicate_list=[RDFPredicate('sioc:has_creater', 'tweep', False)])

    dataset2 = dataset.expand(src_col_name='tweet', predicate_list=[RDFPredicate('sioc:content', 'text', False)])

    dataset2.join(dataset1, 'tweet', 'tweet', 'tweet', join_type)

    sparql_query = dataset2.to_sparql()
    print("SPARQL query with {} =\n{}\n".format(join_type, sparql_query))


def test_expandable_expandable_3_joins(join_type):
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
    ])

    dataset2 = graph.entities(class_name='sioct:tweep',
                              new_dataset_name='dataset2',
                              entities_col_name='tweep')
    dataset2 = dataset2.expand(src_col_name='tweep', predicate_list=[
        RDFPredicate('sioc:has_name', 'name', False),
        RDFPredicate('sioc:has_follower', 'follower', False)
    ])

    dataset.join(dataset2, 'tweep', 'tweep', 'tweep', join_type)

    dataset3 = graph.entities(class_name='sioct:tweep',
                              new_dataset_name='dataset3',
                              entities_col_name='tweep')
    dataset3 = dataset3.expand(src_col_name='tweep', predicate_list=[
        RDFPredicate('sioc:has_id', 'id', False)
    ])

    dataset.join(dataset3, 'follower', 'tweep', 'follower', join_type)

    sparql_query = dataset.to_sparql()
    print("SPARQL query with {} =\n{}\n".format(join_type, sparql_query))


if __name__ == '__main__':
    test_expandable_expandable_join(JoinType.InnerJoin)
    # test_expandable_expandable_join(JoinType.LeftOuterJoin)
    # test_expandable_expandable_join(JoinType.RightOuterJoin)
    # test_expandable_expandable_join(JoinType.InnerJoin, True, True)
    # test_expandable_expandable_join(JoinType.LeftOuterJoin, True, True)
    # test_expandable_expandable_join(JoinType.RightOuterJoin, True, True)
    test_join_instead_of_expand(JoinType.InnerJoin)
    # test_expandable_expandable_3_joins(JoinType.InnerJoin)

