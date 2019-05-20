from rdfframes.client.http_client import HttpClientDataFormat
from rdfframes.client.http_client import HttpClient


def test_small_results():
    print('test_small_results:')
    query = 'SELECT ?tweet (min(?tag) AS ?min_tags)  FROM <http://twitter.com/> WHERE {?tweet <http://rdfs.org/sioc/ns#has_creater> <http://twitter.com/9977822/> . ?tweet <http://twitter.com/ontology/hashashtag> ?tag} GROUP BY ?tweet;'
    endpoint = 'http://10.161.202.101:8890/sparql/'
    port = 8890
    output_format = HttpClientDataFormat.CSV
    max_rows = 1000
    timeout = 120
    default_graph_url = 'http://twitter.com'
    client = HttpClient(endpoint_url=endpoint,
                        port=port,
                        return_format=output_format,
                        timeout=timeout,
                        default_graph_uri=default_graph_url,
                        max_rows=max_rows
                        )
    for res in client.execute_query(query, HttpClientDataFormat.CSV, 'response_data.txt'):
        print('data with type {} and length {} retrieved'.format(type(res).__name__, len(res)))


def test_large_results():
    print('test_large_results:')
    query = 'SELECT ?tweet (min(?tag) AS ?min_tags)  FROM <http://twitter.com/> WHERE {?tweet <http://rdfs.org/sioc/ns#has_creater> <http://twitter.com/9977822/> . ?tweet <http://twitter.com/ontology/hashashtag> ?tag} GROUP BY ?tweet LIMIT 10 OFFSET 10;'
    endpoint = 'http://10.161.202.101/sparql/'
    port = 8890
    output_format = HttpClientDataFormat.CSV
    max_rows = 10
    timeout = 120
    default_graph_url = 'http://twitter.com'
    client = HttpClient(endpoint_url=endpoint,
                        port=port,
                        return_format=output_format,
                        timeout=timeout,
                        default_graph_uri=default_graph_url,
                        max_rows=max_rows
                        )
    for res in client.execute_query(query, HttpClientDataFormat.CSV, 'response_data.txt'):
        print('data with type {} and length {} retrieved'.format(type(res).__name__, len(res)))


def test_query_with_limit(limit, offset, return_format=HttpClientDataFormat.DEFAULT, out_file=None):
    query = 'SELECT ?tweet (min(?tag) AS ?min_tags) FROM <http://twitter.com/> WHERE {{?tweet <http://rdfs.org/sioc/ns#has_creater> <http://twitter.com/9977822/> . ?tweet <http://twitter.com/ontology/hashashtag> ?tag}} GROUP BY ?tweet LIMIT {} OFFSET {};'.format(limit, offset)
    endpoint = 'http://10.161.202.101:8890/sparql/'
    port = 8890
    output_format = return_format
    max_rows = 10
    timeout = 120
    default_graph_url = 'http://twitter.com'
    client = HttpClient(endpoint_url=endpoint,
                        port=port,
                        return_format=output_format,
                        timeout=timeout,
                        default_graph_uri=default_graph_url,
                        max_rows=max_rows
                        )
    for res in client.execute_query(query, output_format, out_file):
        print('data with type {} and length {} retrieved'.format(type(res).__name__, len(res)))


def test_query_with_large_limit():
    print('test_query_with_large_limit:')
    test_query_with_limit(100, 10, HttpClientDataFormat.PANDAS_DF)


def test_query_with_small_limit():
    print('test_query_with_small_limit:')
    test_query_with_limit(10, 10, HttpClientDataFormat.HTML)


if __name__ == '__main__':
    test_small_results()
    test_large_results()
    test_query_with_small_limit()
    test_query_with_large_limit()
