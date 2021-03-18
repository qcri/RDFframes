## RDFframes Source Code

**client** package contains the implemention of the _client_ class and its different implementaions (sparql_endpoint_client and Http client ). It is the SPARQL client that handles communication with a sparql end-point or local RDF engine. 

**dataset** package contains the implemention of the _dataset_ which represents a table filled by data obtained from a Knowledge Graph. The dataset can be an expandable dataset or a grouped dataset. The expandable dataset is a representation of a flat table filled by entities obtained by following a particular path in the Knowledge Graph. Grouped dataset represents a special kind of dataset resulted from running _group by_ operator on a dataset object.

**query_buffer** package encapsulates two important components of _RDFframes_. 1) _query queue_ which is the object that stores the sequence of API calls. Processing this queue produces the SPARQL query. 2) _query operator_ represents an expansion step of a dataset. Each operator is a node that will be added to the _query queue_ when an API call is made. The nodes are added in the _query queue_ in order to preserve the execution order of expansion call to be transformed later to SPARQL query. 

