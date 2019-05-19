from rdfframe.knowledge_graph import KnowledgeGraph
from rdfframe.dataset.rdfpredicate import RDFPredicate
from rdfframe.dataset.aggregation_fn_data import AggregationData
import time


def test_users_tweets_count():
    """
    In twitter dataset, retrieve all users having tweets count >= count_threshold
    :return:
    """

    start = time.time()
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
    dataset = graph.entities(class_name='sioct:microblogPost',
                             new_dataset_name='tweets',
                             class_col_name='tweet_class',
                             entities_col_name='tweet')

    ds = dataset.expand(src_col_name='tweet', predicate_list=[
        RDFPredicate('sioc:has_creater', 'tweep', True),
        RDFPredicate('sioc:content', 'text'),
        RDFPredicate('dcterms:created', 'date'),
        RDFPredicate('to:hasmedia', 'multimedia'),
        RDFPredicate('to:hashashtag', 'hashtag'),
        RDFPredicate('sioc:mentions', 'users_mentioned')
    ])



    ds = ds.expand(src_col_name='tweep', predicate_list=[
        RDFPredicate('sioc:name', 'tweep_name')
    ])

    gds = ds.group_by(groupby_cols_list=['tweep'])
    gds = gds.count(aggregation_fn_data=[AggregationData('tweet', 'tweets_count')])
    gds = gds.filter(conditions_dict={'tweets_count': ['> {}'.format(250), '< {}'.format(300)]})
    gds = gds.select_cols(['tweep', 'tweets_count', 'tweep_name', 'text', 'date', 'hashtag'])

    ds = ds.sort({'tweep': 'ASC'}).limit(10).offset(5)

    ds = ds.select_cols(['tweet', 'tweep', 'text', 'date', 'multimedia', 'hashtag', 'users_mentioned'])


    # ds.print_query_structure()
    # gds.print_query_structure()

    sparql = ds.to_sparql()
    ##ds.get_columns()
    end_transformation = time.time()
    print('Transformed in {} sec'.format(end_transformation-start))
    print(sparql)

    # client = Client("http://192.168.10.2:8890/sparql")
    # df = gds.execute(client, return_format='df')
    # print('Executed in {} sec'.format(time.time() - end_transformation))
    # df.describe()


if __name__ == '__main__':
    test_users_tweets_count()
