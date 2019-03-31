from rdframe.knowledge_graph import KnowledgeGraph
from rdframe.dataset.rdfpredicate import RDFPredicate
from rdframe.dataset.aggregation_fn_data import AggregationData


def important_vldb_authors():
    """
    Returns the SPARQL query that finds all authors that have more than 20 vldb papers using dblp data.
    """
    graph = KnowledgeGraph(prefixes={
                               "swrc": "http://swrc.ontoware.org/ontology#",
                               "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                               "dc": "http://purl.org/dc/elements/1.1/",
                           })

    dataset = graph.entities(class_name='swrc:InProceedings',
                             new_dataset_name='papers',
                             entities_col_name='paper')
    dataset = dataset.expand(src_col_name='paper', predicate_list=[
        RDFPredicate('dc:title', 'title'),
        RDFPredicate('dc:creator', 'author'),
        RDFPredicate('swrc:series', 'conference')])\
        .filter(conditions_dict={'conference': ['= <https://dblp.l3s.de/d2r/resource/conferences/vldb>']})
    grouped_dataset = dataset.group_by(['author'])\
        .count(aggregation_fn_data=[AggregationData('paper', 'papers_count')])\
        .filter(conditions_dict={'papers_count': ['>= {}'.format(20)]})

    grouped_dataset = grouped_dataset.select_cols(['author', 'papers_count'])
    print("SPARQL Query = \n{}".format(grouped_dataset.to_sparql()))

def important_topics():
    """
    Returns the SPARQL query to identify the hot areas of research in a field of computer science.
    First, we identify a list of the top conferences of the computer science field of interest.
    We then identify the authors who have published more than 20 papers in these conferences since the year 2000.
    Next, we find the titles of all papers published by these authors in the specified conferences since 2005.
    """
    graph = KnowledgeGraph(prefixes={
        "swrc": "http://swrc.ontoware.org/ontology#",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "dc": "http://purl.org/dc/elements/1.1/",
        "dcterm": "purl.org/dc/terms/"
    })
    dataset = graph.entities(class_name='swrc:InProceedings',
                             new_dataset_name='papers',
                             entities_col_name='paper')\
        .expand(src_col_name='paper', predicate_list=[
            RDFPredicate('dc:creator', 'author'),
            RDFPredicate('dcterm:issued', 'date'),
            RDFPredicate('swrc:series', 'conference')])\
        .filter({'date':['> 2000'], 'conference': ['IN (<https://dblp.l3s.de/d2r/resource/conferences/vldb>, '
                                                   '<https://dblp.l3s.de/d2r/resource/conferences/sigmod>)']})\
        .group_by(['author'])\
        .count([AggregationData('paper', 'papers_count')])\
        .filter({'papers_count':['>= 20']})\
        .expand(src_col_name='paper', predicate_list=[RDFPredicate('dc:title', 'title')])\
        .filter({'date': ['>= 2005']})\
        .select_cols(['title'])

    print("SPARQL Query = \n{}".format(dataset.to_sparql()))




if __name__ == '__main__':
    # important_vldb_authors()
    important_topics()