from rdfframes.knowledge_graph import KnowledgeGraph
from rdfframes.utils.constants import JoinType
from rdfframes.client.http_client import HttpClientDataFormat, HttpClient
from rdfframes.dataset.rdfpredicate import PredicateDirection


graph = KnowledgeGraph(graph_name='dbpedia')

endpoint = 'http://10.161.202.101:8890/sparql/'
output_format = HttpClientDataFormat.PANDAS_DF

client = HttpClient(endpoint_url=endpoint, return_format=output_format)


movies = graph.feature_domain_range('dbpp:starring', domain_col_name='movie', range_col_name='actor')
american_actors = movies.expand('actor', [('dbpp:birthPlace', 'actor_country')])\
    .filter({'actor_country': ['=dbpr:United_States']})
american_prolific = american_actors.group_by(['actor'])\
    .count('movie', 'movie_count', unique=True).filter({'movie_count': ['>=50']})

movies = american_prolific.expand('actor', [('dbpp:starring', 'movie', False, PredicateDirection.INCOMING),
                                            ('dbpp:academyAward', 'award', True)])
print(movies.to_sparql())




"""

movies = graph.feature_domain_range('dbpp:starring', domain_col_name='movie', range_col_name='actor').cache()

big_american_name = movies.group_by(['actor'])\
    .count('movie', 'movie_count', unique=True).filter({'movie_count': ['>=60']}) \
    .select_cols(['actor', 'movie_count'])\
    .expand('actor', [('dbpp:birthPlace', 'actor_country')])\
    .filter({'actor_country': ['=dbpr:United_States']}).select_cols(['actor','movie_count']).cache()

many_actors = movies.group_by(['movie'])\
    .count('actor', 'actor_count').filter({'actor_count': ['>=30']}).cache()

many_actors_with_actors = many_actors.expand('movie', [('dbpp:starring', 'actor')])
big_american_name_with_movies = big_american_name.expand('actor', [('dbpp:starring', 'movie', False, PredicateDirection.INCOMING)])

big_production_with_big_american_actors = many_actors\
    .join(big_american_name_with_movies, 'movie', join_type=JoinType.LeftOuterJoin)

big_american_name_with_big_production_movies = big_american_name.join(many_actors_with_actors, 'actor', join_type=JoinType.LeftOuterJoin)


final_result = big_production_with_big_american_actors.join(big_american_name_with_big_production_movies, 'actor', join_type=JoinType.OuterJoin)\
    .select_cols(['movie', 'actor', 'movie_count', 'actor_count'])

print(final_result.to_sparql())


#print(final_result.to_sparql())



PREFIX  dbpp: <http://dbpedia.org/property/>
PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX  dbpo: <http://dbpedia.org/ontology/>
PREFIX  dbpr: <http://dbpedia.org/resource/>

SELECT DISTINCT  ?movie ?actor ?movie_count ?actor_count
FROM <http://dbpedia.org>
WHERE
  {  
     
  { SELECT DISTINCT  ?movie ?actor ?movie_count ?actor_count
      WHERE
        {   { { SELECT DISTINCT  ?movie (COUNT(?actor) AS ?actor_count)
                WHERE
                  { ?movie  dbpp:starring  ?actor }
                GROUP BY ?movie
                HAVING ( COUNT(?actor) >= 71 )
              }
            }
          UNION
            { { SELECT DISTINCT  ?actor (COUNT(?movie) AS ?movie_count)
                WHERE
                  { ?movie  dbpp:starring  ?actor }
                GROUP BY ?actor
                HAVING ( COUNT(?movie) >= 181 )
              }
              ?actor  dbpp:birthPlace  ?actor_country
              FILTER ( ?actor_country = dbpr:United_States )
            }
        }
    }
      OPTIONAL{  SELECT DISTINCT ?movie1 as ?movie ?actor1 as ?actor WHERE {?movie1  dbpp:starring  ?actor1 }}

  }      
  
  



PREFIX  dbpp: <http://dbpedia.org/property/>
PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX  dbpo: <http://dbpedia.org/ontology/>
PREFIX  dbpr: <http://dbpedia.org/resource/>

SELECT DISTINCT ?movie1 ?actor2 ?movie_count ?actor_count
FROM <http://dbpedia.org>
WHERE
  { { SELECT DISTINCT  ?movie1 ?actor2 ?movie_count ?actor_count
      WHERE
        {   { SELECT DISTINCT  ?movie1 (COUNT(?actor1) AS ?actor_count)
                WHERE
                  { ?movie1  dbpp:starring  ?actor1 }
                GROUP BY ?movie1
                HAVING ( COUNT(?actor1) >= 30 )
              }
          UNION
            { { SELECT DISTINCT  ?actor2 (COUNT(?movie2) AS ?movie_count)
                WHERE
                  { ?movie2  dbpp:starring  ?actor2 }
                GROUP BY ?actor2
                HAVING ( COUNT(?movie2) >= 60 )
              }
              ?actor2  dbpp:birthPlace  ?actor_country
              FILTER ( ?actor_country = dbpr:United_States )
            }
        }
    }
    Optional {?movie1  dbpp:starring  ?actor2 }    
  }
 



Optimized version of the code
# 600 Rows. -- 1214 msec.
I want 2561 Rows. -- 1233 msec.
In this query, count of movies is 30 Rows. -- 245 msec.
number of actors and their count is 399 Rows. -- 582 msec.
number of american actors and their counts is 20 Rows. -- 82 msec.
number of american actors and their counts and their movies is 1478 Rows. -- 93 msec.
union is 2561 Rows. -- 130 msec.
Now final result is 2561 Rows. -- 300 msec.

# only union of the 2 datasets without join 5322 Rows. -- 178 msec.
# inner join 56236 Rows. -- 1508 msec.
PREFIX  dbpp: <http://dbpedia.org/property/>
PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX  dbpo: <http://dbpedia.org/ontology/>
PREFIX  dbpr: <http://dbpedia.org/resource/>

SELECT DISTINCT  ?movie1 ?actor2 ?movie_count ?actor_count
FROM <http://dbpedia.org>
WHERE
  { { SELECT DISTINCT  ?movie1 ?actor2 ?movie_count ?actor_count
      WHERE
        {   { { SELECT DISTINCT  ?movie1 (COUNT(?actor1) AS ?actor_count)
                WHERE
                  { ?movie1  dbpp:starring  ?actor1 }
                GROUP BY ?movie1
                HAVING ( COUNT(?actor1) >= 71 )
              }
              ?movie1  dbpp:starring  ?actor1
            }
          UNION
            { { SELECT DISTINCT  ?actor2 (COUNT(?movie2) AS ?movie_count)
                WHERE
                  { ?movie2  dbpp:starring  ?actor2 }
                GROUP BY ?actor2
                HAVING ( COUNT(?movie2) >= 181 )
              }
              ?actor2  dbpp:birthPlace  ?actor_country
              FILTER ( ?actor_country = dbpr:United_States )
              ?movie2  dbpp:starring  ?actor2
            }
        }
    }
 OPTIONAL { ?movie1  dbpp:starring  ?actor2 }
  }




    
   



# 571 Rows. -- 28 msec.
PREFIX  dbpp: <http://dbpedia.org/property/>
PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX  dbpo: <http://dbpedia.org/ontology/>
PREFIX  dbpr: <http://dbpedia.org/resource/>
SELECT DISTINCT ?actor2 ?movie_count
FROM <http://dbpedia.org>
WHERE {
    { SELECT  ?actor2 (COUNT(?movie2) AS ?movie_count)
      WHERE
        { ?movie2  dbpp:starring  ?actor2
        }
      GROUP BY ?actor2
      HAVING ( COUNT(?movie2) >= 10 )
    }
        ?actor2  dbpp:birthPlace  ?actor_country .
        FILTER ( ?actor_country = dbpr:United_States )  .
        ?movie2  dbpp:starring  ?actor2  
}


# 4751 Rows. -- 107 msec.
# 1083 Rows. -- 59 msec.
PREFIX  dbpp: <http://dbpedia.org/property/>
PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX  dbpo: <http://dbpedia.org/ontology/>
PREFIX  dbpr: <http://dbpedia.org/resource/>
SELECT DISTINCT ?movie1 ?actor_count
FROM <http://dbpedia.org>
WHERE {
{ SELECT  ?movie1 (COUNT(?actor1) AS ?actor_count)
      WHERE
        { ?movie1  dbpp:starring  ?actor1
        }
      GROUP BY ?movie1
      HAVING ( COUNT(?actor1) >= 10 )
    }
    ?movie1  dbpp:starring  ?actor1
}





"""






"""
2561 Rows. -- 1668 msec.

Query generated by RDFFrames
PREFIX  dbpp: <http://dbpedia.org/property/>
PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX  dbpo: <http://dbpedia.org/ontology/>
PREFIX  dbpr: <http://dbpedia.org/resource/>

SELECT DISTINCT ?movie ?actor ?actor_count ?movie_count
FROM <http://dbpedia.org>
WHERE
  { {   { SELECT  ?movie ?actor ?actor_count ?movie_count
          WHERE
            { { SELECT  DISTINCT ?movie ?actor ?movie_count
                WHERE
                  { ?movie  dbpp:starring    ?actor .
                    ?actor  dbpp:birthPlace  ?actor_country
                    FILTER ( ?actor_country = dbpr:United_States )
                    { SELECT DISTINCT  ?actor (COUNT(DISTINCT ?movie) AS ?movie_count)
                      WHERE
                        { ?movie  dbpp:starring  ?actor }
                      GROUP BY ?actor
                      HAVING ( COUNT(DISTINCT ?movie) >= 181 )
                    }
                  }
              }
              OPTIONAL
                { SELECT DISTINCT ?movie ?actor ?actor_count
                  WHERE
                    { ?movie  dbpp:starring  ?actor
                      { SELECT DISTINCT  ?movie (COUNT(?actor) AS ?actor_count)
                        WHERE
                          { ?movie  dbpp:starring  ?actor }
                        GROUP BY ?movie
                        HAVING ( COUNT(?actor) >= 71 )
                      }
                    }
                }
            }
        }
      UNION
        { SELECT  ?movie ?actor ?actor_count ?movie_count
          WHERE
            { { SELECT DISTINCT ?movie ?actor ?actor_count
                WHERE
                  { ?movie  dbpp:starring  ?actor
                    { SELECT DISTINCT  ?movie (COUNT(?actor) AS ?actor_count)
                      WHERE
                        { ?movie  dbpp:starring  ?actor }
                      GROUP BY ?movie
                      HAVING ( COUNT(?actor) >= 71 )
                    }
                  }
              }
              OPTIONAL
                { SELECT DISTINCT ?movie ?actor ?movie_count
                  WHERE
                    { ?movie  dbpp:starring    ?actor .
                      ?actor  dbpp:birthPlace  ?actor_country
                      FILTER ( ?actor_country = dbpr:United_States )
                      { SELECT DISTINCT  ?actor (COUNT(DISTINCT ?movie) AS ?movie_count)
                        WHERE
                          { ?movie  dbpp:starring  ?actor }
                        GROUP BY ?actor
                        HAVING ( COUNT(DISTINCT ?movie) >= 181 )
                      }
                    }
                }
            }
        }
    }
  }
  
with filter threshold for both = 10
69531 Rows. -- 3321 msec.
optimized 71239 Rows. -- 2244 msec.
"""


"""

movies1 = graph.feature_domain_range('dbpp:starring', domain_col_name='movie1', range_col_name='actor1').cache()

american_actors = movies1.expand('actor1', [('dbpp:birthPlace', 'actor_country')]).filter(
    {'actor_country': ['regex(str(?actor_country), "USA")']})



many_movies = movies1.group_by(['actor1'])\
    .count('movie1', 'movie_count', unique=True).filter({'movie_count': ['>=60']})

big_american_name = american_actors.join(many_movies,'actor1',join_type=JoinType.InnerJoin)

movies2 = graph.feature_domain_range('dbpp:starring', domain_col_name='movie2', range_col_name='actor2').cache()

many_actors = movies2.group_by(['movie2'])\
    .count('actor2', 'actor_count').filter({'actor_count': ['>=50']}).cache()

big_production = many_actors.expand('movie2', [('dbpp:starring', 'actor2')])

final_result = big_american_name.join(big_production, join_col_name1= 'actor1', join_col_name2='actor2',  new_column_name='actor', join_type=JoinType.OuterJoin)

print(final_result.to_sparql())

"""


"""
RDFFRames SPARQL query
0 Rows. -- 1277 msec.

PREFIX  dbpp: <http://dbpedia.org/property/>
PREFIX  dcterms: <http://purl.org/dc/terms/>
PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX  dbpo: <http://dbpedia.org/ontology/>
PREFIX  dbpr: <http://dbpedia.org/resource/>

SELECT  *
FROM <http://dbpedia.org>
WHERE
  { {   { SELECT  *
          WHERE
            { { SELECT  *
                WHERE
                  { ?movie1  dbpp:starring    ?actor .
                    ?actor   dbpp:birthPlace  ?actor_country
                    FILTER regex(str(?actor_country), "USA")
                    { SELECT DISTINCT  ?actor (COUNT(DISTINCT ?movie1) AS ?movie_count)
                      WHERE
                        { ?movie1  dbpp:starring  ?actor }
                      GROUP BY ?actor
                      HAVING ( COUNT(DISTINCT ?movie1) >= 60 )
                    }
                  }
              }
              OPTIONAL
                { SELECT  *
                  WHERE
                    { ?movie2  dbpp:starring  ?actor
                      { SELECT DISTINCT  ?movie2 (COUNT(?actor) AS ?actor_count)
                        WHERE
                          { ?movie2  dbpp:starring  ?actor }
                        GROUP BY ?movie2
                        HAVING ( COUNT(?actor2) >= 50 )
                      }
                    }
                }
            }
        }
      UNION
        { SELECT  *
          WHERE
            { { SELECT  *
                WHERE
                  { ?movie2  dbpp:starring  ?actor
                    { SELECT DISTINCT  ?movie2 (COUNT(?actor) AS ?actor_count)
                      WHERE
                        { ?movie2  dbpp:starring  ?actor }
                      GROUP BY ?movie2
                      HAVING ( COUNT(?actor2) >= 50 )
                    }
                  }
              }
              OPTIONAL
                { SELECT  *
                  WHERE
                    { ?movie1  dbpp:starring    ?actor .
                      ?actor   dbpp:birthPlace  ?actor_country
                      FILTER regex(str(?actor_country), "USA")
                      { SELECT DISTINCT  ?actor (COUNT(DISTINCT ?movie1) AS ?movie_count)
                        WHERE
                          { ?movie1  dbpp:starring  ?actor }
                        GROUP BY ?actor
                        HAVING ( COUNT(DISTINCT ?movie1) >= 60 )
                      }
                    }
                }
            }
        }
    }
  }
"""