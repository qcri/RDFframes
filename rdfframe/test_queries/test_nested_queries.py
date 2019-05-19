import time

from rdfframe.knowledge_graph import KnowledgeGraph
from rdfframe.dataset.rdfpredicate import RDFPredicate
from rdfframe.dataset.aggregation_fn_data import AggregationData


def test_expand_after_group_by():
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
    sparql_query = dataset.to_sparql()
    print("sparql_query 1 =\n{}\n".format(sparql_query))

    # expand each tweet by the following features: text and tweep
    ds = dataset.expand(src_col_name='tweet', predicate_list=[
        RDFPredicate('sioc:has_creater', 'tweep'),
        RDFPredicate('sioc:content', 'text')
    ])
    sparql_query = ds.to_sparql()
    print("sparql_query 2 =\n{}\n".format(sparql_query))

    # return all the tweets of users whose tweep tweeted 250-300 twweets
    gds = ds.group_by(groupby_cols_list=['tweep'])\
        .count(aggregation_fn_data=[AggregationData('tweet', 'tweets_count')])\
        .filter(conditions_dict={'tweets_count': ['> {}'.format(250), '< {}'.format(300)]})
    sparql_query = gds.to_sparql()
    print("sparql_query 3 =\n{}\n".format(sparql_query))

    # expand these tweets by the following features: date, media, hashtags, users mentioned
    gds = gds.expand(src_col_name='tweet', predicate_list=[
        RDFPredicate('dcterms:created', 'date'),
        RDFPredicate('to:hasmedia', 'multimedia'),
        RDFPredicate('to:hashashtag', 'hashtag'),
        RDFPredicate('sioc:mentions', 'users_mentioned')
    ])
    sparql_query = gds.to_sparql()
    print("sparql_query 4 =\n{}\n\n\n\n".format(sparql_query))
    # select all the tweets and their features
    gds = gds.select_cols(['tweet', 'tweep', 'text', 'date', 'multimedia', 'hashtag', 'users_mentioned'])

    # ds.print_query_structure()
    gds.print_query_structure()
    sparql_query = gds.to_sparql()
    end_transformation = time.time()
    print('Transformed in {} sec'.format(end_transformation-start))
    print("sparql_query 5 =\n{}\n".format(sparql_query))


def test_filter_after_group_by():
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
                             class_col_name='tweet_class',
                             entities_col_name='tweet')
    # expand each tweet by the following features: text and tweep
    ds = dataset.expand(src_col_name='tweet', predicate_list=[
        RDFPredicate('sioc:has_creater', 'tweep'),
        RDFPredicate('sioc:content', 'text')
    ])
    # return all the tweets of users whose tweep tweeted 250-300 twweets
    gds = ds.group_by(groupby_cols_list=['tweep'])\
        .count(aggregation_fn_data=[AggregationData('tweet', 'tweets_count')])\
        .filter(conditions_dict={'tweets_count': ['> {}'.format(250), '< {}'.format(300)]})

    # expand these tweets by the following features: date, media, hashtags, users mentioned
    gds = gds.filter({'text': ' >= aa'})
    gds.print_query_structure()
    sparql_query = gds.to_sparql()
    end_transformation = time.time()
    print('Transformed in {} sec'.format(end_transformation-start))
    print("sparql_query 1 =\n{}\n".format(sparql_query))

def test_select_non_group_by_col():
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
                             class_col_name='tweet_class',
                             entities_col_name='tweet')
    # expand each tweet by the following features: text and tweep
    ds = dataset.expand(src_col_name='tweet', predicate_list=[
        RDFPredicate('sioc:has_creater', 'tweep'),
        RDFPredicate('sioc:content', 'text')
    ])
    # return all the tweets of users whose tweep tweeted 250-300 twweets
    gds = ds.group_by(groupby_cols_list=['tweep'])\
        .count(aggregation_fn_data=[AggregationData('tweet', 'tweets_count')])\
        .filter(conditions_dict={'tweets_count': ['> {}'.format(250), '< {}'.format(300)]})

    gds.select_cols(['tweet', 'tweep'])
    gds.select_cols(['text'])
    gds.print_query_structure()
    sparql_query = gds.to_sparql()
    end_transformation = time.time()
    print('Transformed in {} sec'.format(end_transformation-start))
    print("sparql_query 1 =\n{}\n".format(sparql_query))




if __name__ == '__main__':
    #test_expand_after_group_by()
    #test_filter_after_group_by()
    test_select_non_group_by_col()


