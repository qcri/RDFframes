## RDFframes Source Code


**knowledge_graph.py** represents one or more knowledge graphs (URIs). It contains a group of convenience functions to initialize datasets before applying any operations on them.

**client** package contains the implemention of the _client_ class and its different implementaions (sparql_endpoint_client and Http client ). It is the SPARQL client that handles communication with a sparql end-point or local RDF engine. 

**dataset** package contains the implemention of the _dataset_ which represents a table filled by data obtained from a Knowledge Graph. The dataset can be an expandable dataset or a grouped dataset. The expandable dataset is a representation of a flat table filled by entities obtained by following a particular path in the Knowledge Graph. Grouped dataset represents a special kind of dataset resulted from running _group by_ operator on a dataset object.

**query_buffer** package encapsulates two important components of _RDFframes_. 1)_Query queue_ which is the object that stores the sequence of API calls. Processing this queue produces the SPARQL query. 2)_Query operator_ represents an expansion step of a dataset. Each operator is a node that will be added to the _query queue_ when an API call is made. The nodes are added in the _query queue_ in order to preserve the execution order of expansion call to be transformed later to SPARQL query. 

**query_builder** package contains the implementation of the _query model_, _queue to query model_, and _query builder_. _Query model_ represents the intermediate representation between _query queue_ and the final SPARQL query. _Queue to query model_is the component responsible for reading the API calls' queue and map each operator to its associated SPARQL element in the query model. Finally, the _query builder_ parse the query model to generate and the final, optimized SPARQL query.

**utils** package contains helper and auxiliary functions for the API work.
