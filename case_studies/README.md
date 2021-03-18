## Example Case Studies Using RDFframes

This folder contains the end-to-end code for four example case studies using RDFframes. These case studies are presented in the VLDBJ paper about RDFframes. The goal of the case studies is to show the RDFframes API usage in machine learning and data science applications. They demonstrate the usability, flexibility, and efficiency of using RDFframes to process knowledge graphs and the advantage of integrating RDFframes in the PyData ecosystem. To this end, the code includes not only the RDFframes API calls, but also code for importing and using external machine learning libraries to process the data for each case study.

### Movie Genre Classification 
  
  In this case study, we use the RDFframes API to build a pandas dataframe of movies from the DBpedia knowledge graph (2020 version), along with features that can be used for the task of movie genre classification. The dataframe is then used to train a classifier to predict the genre for movies that do not have one (genre is an optional predicate). The classifier used in the case study is a random forest classifier from the scikit-learn library. The code is in the __movie_genre_classification.py__ file. 
  
 
### Song Genre Classification 
 
   This case study is similar to the Movie Genre Classification case study. However, instead of building a dataframe of movies, the case study uses the RDFframes API to build a dataframe of songs from DBpedia and features that can be used for classifying song genres. As in movie genre classification, the classifier used is a random forest classifier.  The code is in the  __song_genre_classification.py__ file. 
   
###  Topic Modeling
   In this case study, we show the use of RDFframes to query the DBLP bibliography dataset in RDF format to conduct topic modeling to identify the active areas of database research. Details of how active areas of database research are defined can be found in the VLDBJ paper. RDFframes produces a pandas dataframe that is used for topic modeling. Topic modeling relies on the rich PyData ecosystem: NLP libraries for stop-word removal and scikit-learn for topic modeling using SVD. The code is available in the __topic_modeling.py__ file. 
 
 ### Knowledge Graph Embbeding
 
 In this case study, we demonstrate using the RDFframes API to return a pandas dataframe with all triples in the knowledge graph excluding triples that have a literal as the object.
 This is the first step in training a knowledge graph embedding model.
 After extracting this dataframe, we use the AmpliGraph Python Library to train and evaluate a knowledge graph embedding model (specifically, the ComplEx model). The code is in the  __knowledge_graph_embedding.py__ file. 
