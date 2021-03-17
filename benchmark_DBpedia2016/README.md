## Benchmark Queries on the DBpedia 2016 Knowledge Graph

This folder contains the 15 queries we used as a synthetic workload to benchmark the RDFframes API on the 2016 version of DBpedia.
These queries are designed to exercise different features of RDFFrames and SPARQL. 
The details of the queries can be found in the VLDBJ paper.
This folder contains queries over an RDF engine storing the 2016 version of the DBpedia knowledge graph.
Each query is associated with 4 different files.
For example, the 4 files for Query 1 (Q1) are Q1-RDFframes.py, Q1-RDFframes.sparql,
Q1-expert.sparql,
and Q1-naive.sparql.

* Q1-RDFframes.py: The Python code using the RDFframes API to generate the required dataframe.
* Q1-RDFframes.saprql: The SPARQL query generated from the Pythong code by the RDFframes library. 
* Q1-expert.sparql: A semantically equivalent SPARQL query that is written by a SPARQL expert.
* Q1-naive.sparql: The query as produced by a naive query generator. Specifically, each RDFframes API call is converted to a subquery that contains the pattern corresponding to that API call, and all the subqueries are combined by one outer query.

The different versions of the query (RDFframes, expert, and naive) should return the same results. We verified this in our setup.

The __curl_execute_example__ file is a script showing how to run the queries against the SPARQL endpoint of an RDF store using curl.
