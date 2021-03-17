# [RDFFrames: Knowledge Graph Access for Machine Learning Tools](https://arxiv.org/abs/2002.03614)

A Python library that enables data scientists to extract data from knowledge graphs encoded in [RDF](https://www.w3.org/TR/2014/REC-rdf11-concepts-20140225/) into familiar tabular formats using familiar procedural Python abstractions.
RDFframes provides an easy-to-use, efficient, and scalable API for users who are familiar with the PyData (Python for Data) ecosystem but are not experts in [SPARQL](https://www.w3.org/TR/sparql11-query/).
The API calls are internally converted into optimized SPARQL queries, which are then executed on a local RDF engine or a remote SPARQL endpoint.
The results are returned in tabular format, such as a pandas dataframe.

## Modifications to this Website Made in Response to VLDBJ Reviews
## (This section will be removed in the final version of the repository)

In response to the reviews of our VLDBJ submission, we have made the following changes to this repository:

1. Added the source code of all the case studies in the paper to the case_studies folder. The folder also includes a description of the case studies. The case study code includes both the RDFframes API calls and the machine learning code using external libraries such as scikit-learn. 
2. Added all the synthetic queries used in the benchmark. The benchmark_DBpedia2016 folder contains the queries on the 2016 version of DBpedia (used in the original submission of the paper), and the benchmark_DBpedia2020 folder contains the queries on the 2020 version of DBpedia (used in the revised version of the paper). The folders include the RDFframes code for each query and the three variants of each query used in the benchmark (Expert-written SPARQL, Naive Query Generation, and RDFframes). The folders also include simple and detailed instructions on how to run the queries using curl to replicate the benchmark.
3. Expanded the explanation of the RDFframes source code in the rdfframes folder.
4. Expanded the ''Requirements to Use RDFframes'' section in this README file (the next section).


## Requirement to Use RDFframes

1. An RDF database engine storing one or more RDF knowledge graphs (e.g., Virtuoso), or SPARQL endpoints providing access to such knowledge graphs. RDFframes handles all communication and integration issues with the engine or endpoint. 

2. Install the RDFframes library via pip by using:
   ```
   $ pip install RDFframes
   ```   
 3. Import RDFframes in your code as shown in the example code in the case_studies folder.


## Getting started

First create a ``KnowledgeGraph`` to specify any namespaces that will be used in the query and optionally the graph name and URI.
For example:
```python
graph = KnowledgeGraph(prefixes={
                               "swrc": "http://swrc.ontoware.org/ontology#",
                               "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                               "dc": "http://purl.org/dc/elements/1.1/",
                           })
```

Then create a ``Dataset`` using one of our convenience functions. All the convenience functions are methods in the
```KnowledgeGraph``` class. 
For example, the following code retrieves all instances of the class ``swrc:InProceedings``:

```python
dataset = graph.entities(class_name='swrc:InProceedings',
                             new_dataset_name='papers',
                             entities_col_name='paper')
```

There are two types of datasets: ``ExpandableDataset`` and ``GroupedDataset``. 
An ``ExpandableDataset`` represents a simple flat table, while a ``GroupedDataset`` is a table split into groups as a result of a group-by operation.
The convenience functions on the ``KnowledgeGraph`` return an ``ExpandableDataset``.

After instantiating a dataset, you can use the API to perform operations on it. 
For example, the following code retrieves all authors and titles of conference papers:
```python
dataset = dataset.expand(src_col_name='paper', predicate_list=[
        RDFPredicate('dc:title', 'title'),
        RDFPredicate('dc:creator', 'author'),
        RDFPredicate('swrc:series', 'conference')])\
```

Using the ``group_by`` operation results in a ``GroupedDataset``:
```python
grouped_dataset = dataset.group_by(['author'])
```

Aggregation can be done in both an ``ExpandableDataset`` and ``GroupedDataset``.
For example, the following code counts the number of papers per author and keeps only the authors that have more than 20 papers:
```python
grouped_dataset = grouped_dataset.count(aggregation_fn_data=[AggregationData('paper', 'papers_count')])\
        .filter(conditions_dict={'papers_count': ['>= 20']})
```

## Convenience Functions to create an initial dataset

To create an initial ```Dataset```, you need to use one of the convenience functions. The API 
provides convenience functions that can be used by most of the machine learning and data analytics tasks including:

```python
KnowledgeGraph.classes_and_freq()
```
This function retrieves all the classes in the graph and all the number of instances of each class.
It returns a table of two columns, the first one contains the name of the class and the second one
contains the name of the frequency of the clases.
```python
KnowledgeGraph.features_and_freq(class_name)
```
Retrieves all the features of the instances of the class ```class_name``` and how many instances have each features.
This is critical for many machine learning tasks as knowing how many observed features of entities helps us decide 
on which features to use for.
```python
KnowledgeGraph.entities(class_name)
```
Retrieves all the instances of the class ```class_name```. This is the starting point for most machine 
learning models. The return dataset contains one column of the entities of the specified class and can be
expanded to add features of the instances.
```python
KnowledgeGraph.features(class_name)
```
Retrieves all the features of the class ```class_name```. This function can be used to explore the dataset and learn
what features are available in the data for a specific class.
```python
KnowledgeGraph.entities_and_features(class_name, features, )
```
Retrieves all instances of the class ```class_name``` and the features of the instances specified in the list 
```features```.
```python
KnowledgeGraph.num_entities(class_name)
```
Returns the number of instances of the class ```class_name``` in the dataset.
```python
KnowledgeGraph.feature_domain_range(feature)
```
Returieves the domain (subjects) and the range (objects) of the predicate ```feature``` occuring in the dataset.
```python
KnowledgeGraph.describe_entity(entity)
```
Returns the class and features of the entity.



