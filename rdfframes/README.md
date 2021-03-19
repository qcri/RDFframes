## RDFframes Source Code

**knowledge_graph.py:** High-level class representing one or more knowledge graphs. Each graph is identified by a URI. The class contains a group of convenience functions to initialize datasets before applying any operations on them.

**client:** Package containing the definition of the _Client_ abstract class and its two implementations. The _Client_ class is the SPARQL client that handles communication with the RDF engine. One implementation of the class communicates with an RDF engine over HTTP, and one communicates with a SPARQL endpoint.

**dataset:** Package containing the definition of the _Dataset_ abstract class, which represents a table (i.e., dataframe) filled with data obtained from a knowledge graph. The dataset can be an _expandable dataset_ or a _grouped dataset_. An expandable dataset represents a  table with entities obtained by path navigation in a knowledge graph. A grouped dataset is a special kind of dataset that results from running the _group by_ operator on a dataset object.

**query_buffer:** Package containing two important components of _RDFframes_: (1) _Query queue_ which is the object that stores the sequence of API calls. Processing this queue produces the SPARQL query. (2) _Query operator_ which represents an expansion step of a dataset. Each operator is a node that is added to the query queue when a particular API call is made. The nodes are added to the queue in FIFO order to preserve the execution order of expansion calls, which are later transformed to a SPARQL query.

**query_builder:** Package containing the implementation of the _query model_, _queue to query model_, and _query builder_. A query model is the intermediate representation between a query queue and the final SPARQL query. The _queue to query model_ component is responsible for reading the operators corresponding to API calls from the queue and mapping each operator to its associated SPARQL element in the query model. After the query model is constructed, the _query builder_ parses the query model to generate the final, optimized SPARQL query.

**utils:** Package containing constants and helper functions used by the RDFframes implementation.
