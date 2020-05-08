from rdfframes.knowledge_graph import KnowledgeGraph
from rdfframes.client.http_client import HttpClientDataFormat, HttpClient


def explore_dblp():
    graph = KnowledgeGraph(
        graph_name = 'dblp',
        graph_uri='http://dblp.l3s.de',
        prefixes={
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            "swrc": "http://swrc.ontoware.org/ontology#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "dc": "http://purl.org/dc/elements/1.1/",
            "dcterm": "http://purl.org/dc/terms/",
            "dblprc": "http://dblp.l3s.de/d2r/resource/conferences/"
    })

    endpoint = 'http://10.161.202.101:8890/sparql/'
    port = 8890
    output_format = HttpClientDataFormat.PANDAS_DF
    max_rows = 1000000
    timeout = 12000
    client = HttpClient(endpoint_url=endpoint,
                        port=port,
                        return_format=output_format,
                        timeout=timeout,
                        max_rows=max_rows
                        )

    classes = graph.classes_and_freq().sort({'frequency': 'DESC'})
    #class_with_max_freq = graph.classes_and_freq().max('frequency').to_sparql()
    attributes_of_papers = graph.features('swrc:InProceedings')
    attributes_of_papers_with_freq = graph.features_and_freq('swrc:InProceedings')
    papers = graph.entities('swrc:InProceedings')
    #papers_with_features = graph.entities_and_features('swrc:InProceedings').to_sparql()
    num_papers = graph.num_entities('swrc:InProceedings')

    print("{}".format(classes.to_sparql()))
    df = classes.execute(client, return_format=output_format)

    #print("{}".format(attributes_of_papers.to_sparql()))
    #df = attributes_of_papers.execute(client, return_format=output_format)

    print(df)


def important_vldb_authors():
    """
    Returns the SPARQL query that finds all authors that have more than 20 vldb papers using dblp data.
    """
    graph = KnowledgeGraph(
        graph_name = 'dblp',
        graph_uri='http://dblp.l3s.de',
        prefixes={
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            "swrc": "http://swrc.ontoware.org/ontology#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "dc": "http://purl.org/dc/elements/1.1/",
            "dcterm": "http://purl.org/dc/terms/",
            "dblprc": "http://dblp.l3s.de/d2r/resource/conferences/"
    })

    dataset = graph.entities(class_name='swrc:InProceedings',
                             new_dataset_name='papers',
                             entities_col_name='paper')
    dataset = dataset.expand(src_col_name='paper', predicate_list=[
        ('dc:title', 'title'),
        ('dc:creator', 'author'),
        ('swrc:series', 'conference')])\
        .filter(conditions_dict={'conference': ['= <https://dblp.l3s.de/d2r/resource/conferences/vldb>']})
    grouped_dataset = dataset.group_by(['author'])\
        .count('paper', 'papers_count')\
        .filter(conditions_dict={'papers_count': ['>= {}'.format(20)]})

    grouped_dataset = grouped_dataset.select_cols(['author', 'papers_count'])
    print("SPARQL Query = \n{}".format(grouped_dataset.to_sparql()))


def important_topics():
    """
    Returns the SPARQL query to identify the hot areas of research in a field of databases.
    First, we identify a list of the top conferences of the computer science field of interest.
    We then identify the authors who have published more than 20 papers in these conferences since the year 2000.
    Next, we find the titles of all papers published by these authors in the specified conferences since 2005.
    """
    graph = KnowledgeGraph(
        graph_name = 'dblp',
        graph_uri='http://dblp.l3s.de',
        prefixes={
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            "swrc": "http://swrc.ontoware.org/ontology#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "dc": "http://purl.org/dc/elements/1.1/",
            "dcterm": "http://purl.org/dc/terms/",
            "dblprc": "http://dblp.l3s.de/d2r/resource/conferences/"
    })
    endpoint = 'http://10.161.202.101:8890/sparql/'
    port = 8890
    output_format = HttpClientDataFormat.PANDAS_DF
    max_rows = 1000000
    timeout = 12000
    client = HttpClient(endpoint_url=endpoint,
                        port=port,
                        return_format=output_format,
                        timeout=timeout,
                        max_rows=max_rows
                        )

    dataset = graph.entities('swrc:InProceedings', entities_col_name='paper')\
        .expand(src_col_name='paper', predicate_list=[('dc:creator', 'author'), ('dcterm:issued', 'date'),
            ('swrc:series', 'conference'), ('dc:title', 'title')])
    dataset = dataset.cache()
    
    authors = dataset.filter({'date':['>= 2000'], 'conference': ['IN (dblprc:vldb, dblprc:sigmod)']})\
        .group_by(['author'])\
        .count('paper', 'papers_count')\
        .filter({'papers_count':['>= 20']})

    titles = dataset.join(authors, 'author').filter({'date': ['>= 2005']}).select_cols(['title'])

    print("SPARQL Query = \n{}".format(titles.to_sparql()))

    df = titles.execute(client, return_format=output_format)
    print(df)


if __name__ == '__main__':
    #important_vldb_authors()
    important_topics()
    #explore_dblp()
