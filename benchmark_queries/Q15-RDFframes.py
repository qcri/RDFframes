
from rdfframes.client.http_client import HttpClientDataFormat, HttpClient
from time import time
from rdfframes.knowledge_graph import KnowledgeGraph
from rdfframes.client.http_client import HttpClientDataFormat, HttpClient
from rdfframes.client.sparql_endpoint_client import SPARQLEndpointClient
from rdfframes.utils.constants import JoinType
__author__ = "Ghadeer"


endpoint = 'http://10.161.202.101:8890/sparql/'
port = 8890
output_format = HttpClientDataFormat.PANDAS_DF
max_rows = 1000000
timeout = 12000
"""
client = HttpClient(endpoint_url=endpoint,
  port=port,
    return_format=output_format,
    timeout=timeout,
    max_rows=max_rows
    )
"""
client = SPARQLEndpointClient(endpoint)

graph =  KnowledgeGraph(graph_name='dbpedia')

  
def books_with_authors_cache(): #'releaseDate':['>= 1990','<= 2020']})\     ,('dbpp:religion','religion', True)])\
    graph = KnowledgeGraph(graph_name='dbpedia')
    dataset = graph.feature_domain_range('dbpp:author', 'book', 'author')\
        .expand('author', [('dbpp:birthPlace', 'author_country'),(' dbpp:education','education')])\
        .expand('book', [('rdfs:label', 'work_name'),('dbpp:country','country', True),('dcterms:subject', 'subject'),
                         ('dbpp:publisher','publisher', True)])\
        .cache()
    american_authors = dataset.filter({'author_country': ['regex(str(?author_country), "USA")']}) 
    
    famous_authors = dataset.group_by(['author'])\
        .count('book', 'book_count', unique=True).filter({'book_count': ['>= 2']})

    books = american_authors.join(famous_authors, join_col_name1='author', join_type=JoinType.OuterJoin)
    print(books.to_sparql())
    df = books.execute(client)
    print(df.shape)
    print(df.head())
    #'dbpp:university', 'university', 'alumni')\



start = time()
books_with_authors_cache()
#univ_with_grads_cache()
duration = time()-start
print("Duration of books_with_authors_cache datasets = {} sec".format(duration))

"""

RDFframes
SELECT * 
FROM <http://dbpedia.org>
WHERE {
        { {
        SELECT * 
        WHERE {
                {
                SELECT * 
                WHERE {
                        ?book dbpp:author ?author .
                        ?author dbpp:birthPlace ?author_country .
                        ?author  dbpp:education ?education .
                        ?book rdfs:label ?work_name .
                        ?book dcterms:subject ?subject .
                        FILTER ( regex(str(?author_country), "USA") ) 

                        OPTIONAL {
                                ?book dbpp:country ?country .
                                        }       OPTIONAL {
                                ?book dbpp:publisher ?publisher .
                                        }
                        }
                }
         OPTIONAL       {
                SELECT DISTINCT ?author  (COUNT(DISTINCT ?book) AS ?book_count) 
                WHERE {
                        ?book dbpp:author ?author .
                        ?author dbpp:birthPlace ?author_country .
                        ?author  dbpp:education ?education .
                        ?book rdfs:label ?work_name .
                        ?book dcterms:subject ?subject .
                        OPTIONAL {
                                ?book dbpp:country ?country .
                                        }       OPTIONAL {
                                ?book dbpp:publisher ?publisher .
                                        }
                        } GROUP BY ?author 
                HAVING ( ( COUNT(DISTINCT ?book) >= 2 ) )

                        }
                }
        }
        UNION
        {
        SELECT * 
        WHERE {
                {
                SELECT DISTINCT ?author  (COUNT(DISTINCT ?book) AS ?book_count) 
                WHERE {
                        ?book dbpp:author ?author .
                        ?author dbpp:birthPlace ?author_country .
                        ?author  dbpp:education ?education .
                        ?book rdfs:label ?work_name .
                        ?book dcterms:subject ?subject .
                        OPTIONAL {
                                ?book dbpp:country ?country .
                                        }       OPTIONAL {
                                ?book dbpp:publisher ?publisher .
                                        }
                        } GROUP BY ?author 
                HAVING ( ( COUNT(DISTINCT ?book) >= 2 ) )
                }
         OPTIONAL       {
                SELECT * 
                WHERE {
                        ?book dbpp:author ?author .
                        ?author dbpp:birthPlace ?author_country .
                        ?author  dbpp:education ?education .
                        ?book rdfs:label ?work_name .
                        ?book dcterms:subject ?subject .
                        FILTER ( regex(str(?author_country), "USA") ) 

                        OPTIONAL {
                                ?book dbpp:country ?country .
                                        }       OPTIONAL {
                                ?book dbpp:publisher ?publisher .
                                        }
                        }

                        }
                }
        }
         }
        }
"""

### naive
"""


PREFIX  dbpp: <http://dbpedia.org/property/>
PREFIX  dcterms: <http://purl.org/dc/terms/>
PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX  dbpo: <http://dbpedia.org/ontology/>
PREFIX  dbpr: <http://dbpedia.org/resource/>

SELECT  *
FROM <http://dbpedia.org>
WHERE
  { SELECT  *
    WHERE
      { { SELECT  ?book ?author ?author_country ?work_name ?subject
          WHERE
            { { SELECT  *
                WHERE
                  { ?book  dbpp:author  ?author }
              }
              { SELECT  *
                WHERE
                  { ?author  dbpp:birthPlace  ?author_country }
              }
              { SELECT  *
                WHERE
                  { ?author  dbpp:education  ?education }
              }
              { SELECT  *
                WHERE
                  { ?book  rdfs:label  ?work_name }
              }
              { SELECT  *
                WHERE
                  { ?book  dcterms:subject  ?subject }
              }
              FILTER regex(str(?author_country), "USA")
              OPTIONAL
                { { SELECT  ?country
                    WHERE
                      { ?book  dbpp:country  ?country }
                  }
                }
              OPTIONAL
                { { SELECT  ?publisher
                    WHERE
                      { ?book  dbpp:publisher  ?publisher }
                  }
                }
            }
        }
        OPTIONAL
          { SELECT  *
            WHERE
              { SELECT DISTINCT  ?author (COUNT(DISTINCT ?book) AS ?book_count)
                WHERE
                  { SELECT  ?book ?author ?author_country ?work_name ?subject
                    WHERE
                      { { SELECT  *
                          WHERE
                            { ?book  dbpp:author  ?author }
                        }
                        { SELECT  *
                          WHERE
                            { ?author  dbpp:birthPlace  ?author_country }
                        }
                        { SELECT  *
                          WHERE
                            { ?author  dbpp:education  ?education }
                        }
                        { SELECT  *
                          WHERE
                            { ?book  rdfs:label  ?work_name }
                        }
                        { SELECT  *
                          WHERE
                            { ?book  dcterms:subject  ?subject }
                        }
                        OPTIONAL
                          { { SELECT  ?country
                              WHERE
                                { ?book  dbpp:country  ?country }
                            }
                          }
                        OPTIONAL
                          { { SELECT  ?publisher
                              WHERE
                                { ?book  dbpp:publisher  ?publisher }
                            }
                          }
                      }
                  }
                GROUP BY ?author
                HAVING ( COUNT(DISTINCT ?book) >= 2 )
              }
          }
      }
  }
  
  
  
  
  
  PREFIX  dbpp: <http://dbpedia.org/property/>
PREFIX  dcterms: <http://purl.org/dc/terms/>
PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX  dbpo: <http://dbpedia.org/ontology/>
PREFIX  dbpr: <http://dbpedia.org/resource/>

SELECT  *
FROM <http://dbpedia.org>
WHERE
  { SELECT  *
    WHERE
      { { SELECT  *
          WHERE
            { SELECT DISTINCT  ?author (COUNT(DISTINCT ?book) AS ?book_count)
              WHERE
                { SELECT  ?book ?author ?author_country ?work_name ?subject
                  WHERE
                    { { SELECT  *
                        WHERE
                          { ?book  dbpp:author  ?author }
                      }
                      { SELECT  *
                        WHERE
                          { ?author  dbpp:birthPlace  ?author_country }
                      }
                      { SELECT  *
                        WHERE
                          { ?author  dbpp:education  ?education }
                      }
                      { SELECT  *
                        WHERE
                          { ?book  rdfs:label  ?work_name }
                      }
                      { SELECT  *
                        WHERE
                          { ?book  dcterms:subject  ?subject }
                      }
                      OPTIONAL
                        { { SELECT  ?country
                            WHERE
                              { ?book  dbpp:country  ?country }
                          }
                        }
                      OPTIONAL
                        { { SELECT  ?publisher
                            WHERE
                              { ?book  dbpp:publisher  ?publisher }
                          }
                        }
                    }
                }
              GROUP BY ?author
              HAVING ( COUNT(DISTINCT ?book) >= 2 )
            }
        }
        OPTIONAL
          { SELECT  ?book ?author ?author_country ?work_name ?subject
            WHERE
              { { SELECT  *
                  WHERE
                    { ?book  dbpp:author  ?author }
                }
                { SELECT  *
                  WHERE
                    { ?author  dbpp:birthPlace  ?author_country }
                }
                { SELECT  *
                  WHERE
                    { ?author  dbpp:education  ?education }
                }
                { SELECT  *
                  WHERE
                    { ?book  rdfs:label  ?work_name }
                }
                { SELECT  *
                  WHERE
                    { ?book  dcterms:subject  ?subject }
                }
                FILTER regex(str(?author_country), "USA")
                OPTIONAL
                  { { SELECT  ?country
                      WHERE
                        { ?book  dbpp:country  ?country }
                    }
                  }
                OPTIONAL
                  { { SELECT  ?publisher
                      WHERE
                        { ?book  dbpp:publisher  ?publisher }
                    }
                  }
              }
          }
      }
  }
  """
  ######full query
  
"""
  PREFIX  dbpp: <http://dbpedia.org/property/>
PREFIX  dcterms: <http://purl.org/dc/terms/>
PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX  dbpo: <http://dbpedia.org/ontology/>
PREFIX  dbpr: <http://dbpedia.org/resource/>

SELECT  *
FROM <http://dbpedia.org>
WHERE
  {   { SELECT  *
        WHERE
          { { SELECT  ?book ?author ?author_country ?work_name ?subject
              WHERE
                { { SELECT  *
                    WHERE
                      { ?book  dbpp:author  ?author }
                  }
                  { SELECT  *
                    WHERE
                      { ?author  dbpp:birthPlace  ?author_country }
                  }
                  { SELECT  *
                    WHERE
                      { ?author  dbpp:education  ?education }
                  }
                  { SELECT  *
                    WHERE
                      { ?book  rdfs:label  ?work_name }
                  }
                  { SELECT  *
                    WHERE
                      { ?book  dcterms:subject  ?subject }
                  }
                  FILTER regex(str(?author_country), "USA")
                  OPTIONAL
                    { { SELECT  ?country
                        WHERE
                          { ?book  dbpp:country  ?country }
                      }
                    }
                  OPTIONAL
                    { { SELECT  ?publisher
                        WHERE
                          { ?book  dbpp:publisher  ?publisher }
                      }
                    }
                }
            }
            OPTIONAL
              { SELECT  *
                WHERE
                  { SELECT DISTINCT  ?author (COUNT(DISTINCT ?book) AS ?book_count)
                    WHERE
                      { SELECT  ?book ?author ?author_country ?work_name ?subject
                        WHERE
                          { { SELECT  *
                              WHERE
                                { ?book  dbpp:author  ?author }
                            }
                            { SELECT  *
                              WHERE
                                { ?author  dbpp:birthPlace  ?author_country }
                            }
                            { SELECT  *
                              WHERE
                                { ?author  dbpp:education  ?education }
                            }
                            { SELECT  *
                              WHERE
                                { ?book  rdfs:label  ?work_name }
                            }
                            { SELECT  *
                              WHERE
                                { ?book  dcterms:subject  ?subject }
                            }
                            OPTIONAL
                              { { SELECT  ?country
                                  WHERE
                                    { ?book  dbpp:country  ?country }
                                }
                              }
                            OPTIONAL
                              { { SELECT  ?publisher
                                  WHERE
                                    { ?book  dbpp:publisher  ?publisher }
                                }
                              }
                          }
                      }
                    GROUP BY ?author
                    HAVING ( COUNT(DISTINCT ?book) >= 2 )
                  }
              }
          }
      }
    UNION
      { SELECT  *
        WHERE
          { { SELECT  *
              WHERE
                { SELECT DISTINCT  ?author (COUNT(DISTINCT ?book) AS ?book_count)
                  WHERE
                    { SELECT  ?book ?author ?author_country ?work_name ?subject
                      WHERE
                        { { SELECT  *
                            WHERE
                              { ?book  dbpp:author  ?author }
                          }
                          { SELECT  *
                            WHERE
                              { ?author  dbpp:birthPlace  ?author_country }
                          }
                          { SELECT  *
                            WHERE
                              { ?author  dbpp:education  ?education }
                          }
                          { SELECT  *
                            WHERE
                              { ?book  rdfs:label  ?work_name }
                          }
                          { SELECT  *
                            WHERE
                              { ?book  dcterms:subject  ?subject }
                          }
                          OPTIONAL
                            { { SELECT  ?country
                                WHERE
                                  { ?book  dbpp:country  ?country }
                              }
                            }
                          OPTIONAL
                            { { SELECT  ?publisher
                                WHERE
                                  { ?book  dbpp:publisher  ?publisher }
                              }
                            }
                        }
                    }
                  GROUP BY ?author
                  HAVING ( COUNT(DISTINCT ?book) >= 2 )
                }
            }
            OPTIONAL
              { SELECT  ?book ?author ?author_country ?work_name ?subject
                WHERE
                  { { SELECT  *
                      WHERE
                        { ?book  dbpp:author  ?author }
                    }
                    { SELECT  *
                      WHERE
                        { ?author  dbpp:birthPlace  ?author_country }
                    }
                    { SELECT  *
                      WHERE
                        { ?author  dbpp:education  ?education }
                    }
                    { SELECT  *
                      WHERE
                        { ?book  rdfs:label  ?work_name }
                    }
                    { SELECT  *
                      WHERE
                        { ?book  dcterms:subject  ?subject }
                    }
                    FILTER regex(str(?author_country), "USA")
                    OPTIONAL
                      { { SELECT  ?country
                          WHERE
                            { ?book  dbpp:country  ?country }
                        }
                      }
                    OPTIONAL
                      { { SELECT  ?publisher
                          WHERE
                            { ?book  dbpp:publisher  ?publisher }
                        }
                      }
                  }
              }
          }
      }
  }
"""
