from rdfframes.query_builder.querymodel import QueryModel
from orderedset import OrderedSet


if __name__ == '__main__':


    ####### complete query
    subquery = QueryModel()
    subquery.add_triple("tweet", "sioc:has_creater", "tweep")
    #subquery.add_variable("tweeter")
    subquery.add_group_columns(OrderedSet(["tweep"]))
    subquery.add_aggregate_pair("tweet", "COUNT", "tweet_count", "distinct")
    subquery.add_having_condition("tweet_count", "< 300")
    subquery.add_having_condition("tweet_count", "> 250")
    subquery.add_select_column("tweep")

    twitterquery = QueryModel()
    prefixes = {
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "sioc": "http://rdfs.org/sioc/ns#",
        "sioct": "http://rdfs.org/sioc/types#",
        "to": "http://twitter.com/ontology/",
        "dcterms": "http://purl.org/dc/terms/",
        "xsd": "http://www.example.org/",
        "foaf": "http://xmlns.com/foaf/0.1/",
    }
    twitterquery.add_prefixes(prefixes)
    twitterquery.add_graphs(["http://twitter.com/"])
    twitterquery.add_variable("tweep")
    twitterquery.add_subquery(subquery)
    twitterquery.add_triple("tweet", "sioc:has_creater", "tweep")
    twitterquery.add_triple("tweet", " sioc:content", "text")
    twitterquery.add_optional_triples("tweet", "sioc:mentions", "mentions")
    twitterquery.add_triple("tweet", "to:hashashtag", "hashtag")
    twitterquery.add_triple("tweet", 'dcterms:created', 'date')
    twitterquery.add_triple("tweet", 'to:hasmedia', 'multimedia')
    twitterquery.add_order_columns([("tweep", "ASC")])
    twitterquery.add_select_column("tweet")
    twitterquery.add_select_column("tweep")
    twitterquery.add_select_column("text")
    twitterquery.add_select_column("date")
    twitterquery.add_select_column("hashtag")
    twitterquery.add_select_column("mentions")
    twitterquery.add_select_column("multimedia")

    #twitterquery.add_variable("text")
    #twitterquery.add_variable("hashtag")
    #twitterquery.add_variable("mentions")

    #twitterquery.add_filter_condition("hashtag"," < 7")

    print(twitterquery.to_sparql())






