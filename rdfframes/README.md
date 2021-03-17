## RDFframes Source Code

**client** package contains the implemention of the _client_ class and its different implementaions (sparql_endpoint_client and Http client ). It is the SPARQL client that handles communication with a sparql end-point or local RDF engine. 

**dataset** package contains the implemention of the _dataset_ which represents a table filled by data obtained from a Knowledge Graph. The dataset can be an expandable dataset or a grouped dataset. The expandable dataset is a representation of a flat table filled by entities obtained by following a particular path in the Knowledge Graph. Grouped dataset represents a special kind of dataset resulted from running _group by_ operator on a dataset object.
