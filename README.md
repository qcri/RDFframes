#RDFrame


A Python library that enables data scientists to extract data from knowledge graphs encoded in [RDF](https://www.w3.org/TR/2014/REC-rdf11-concepts-20140225/) into familiar tabular formats using familiar procedural Python abstractions.
RDFrames provides an easy-to-use, efficient, and scalable API for users who are familiar with the PyData (Python for Data) ecosystem but are not experts in [SPARQL](https://www.w3.org/TR/sparql11-query/).
The API calls are internally converted into optimized SPARQL queries, which are then executed on a local RDF engine or a remote SPARQL endpoint.
The results are returned in tabular format, such as a pandas dataframe.

##Installation via ``pip``


You can directly install the library via pip by using:

```
 $ pip install RDFrame
```   
##getting started


