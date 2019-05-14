from sparqlwrapper_client import SPARQLWrapperClient
from rdframe.client.http_client import HttpClientDataFormat, HttpClient
from rdframe.knowledge_graph import KnowledgeGraph
from rdframe. dataset.rdfpredicate import RDFPredicate
from rdframe.dataset.aggregation_fn_data import AggregationData


def test_flat_query():
    endpoint = 'http://10.161.202.101:8890/sparql/'
    port = 8890
    output_format = HttpClientDataFormat.PANDAS_DF
    max_rows = 1000000
    timeout = 12000
    default_graph_url = 'http://twitter.com'
    client = HttpClient(endpoint_url=endpoint,
                        port=port,
                        return_format=output_format,
                        timeout=timeout,
                        default_graph_uri=default_graph_url,
                        max_rows=max_rows
                        )

    graph = KnowledgeGraph('twitter',
                           'http://twitter.com/',
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
        RDFPredicate('sioc:has_creater', 'tweep')
    ]).group_by(['tweep']).count(
        aggregation_fn_data=[AggregationData('tweet', 'tweets_count')]).filter(
        conditions_dict={'tweets_count': ['>= {}'.format(200), '<= {}'.format(300)]})

    ds = ds.expand(src_col_name='tweet', predicate_list=[
        RDFPredicate('sioc:content', 'text', optional=False),
        RDFPredicate('dcterms:created', 'date', optional=True),
        RDFPredicate('to:hasmedia', 'multimedia', optional=True),
        RDFPredicate('to:hashashtag', 'hashtag', optional=True),
        RDFPredicate('sioc:mentions', 'users_mentioned', optional=True)
    ])

    ds = ds.select_cols(['tweet', 'tweep', 'text', 'date', 'multimedia', 'hashtag', 'users_mentioned'])

    print("Sparql Query = \n{}".format(ds.to_sparql()))

    df = ds.execute(client, return_format=output_format)
    print(df.head(10))

    return df


if __name__ == '__main__':
    test_flat_query()
