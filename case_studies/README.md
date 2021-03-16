## About

This folder contains the end-to-end code for case studies presented in the paper. The goal of these case studies is to show the RDFframes API usage in machine learning and data science application. It further demonstrates the usability, flexibility, and efficiency of using RDFframes to process knowledge graphs and the advantage of integrating RDFframes in the PyData ecosystem. Thus, the code includes importing and using external machine learning libraries to develop a concrete code for each case study.   

  ###  Topic Modeling
   In this case study, we show the use of RDFframes to query the DBLP bibliography dataset in RDF format to conduct topic modeling to identify the active areas of     database research. RDFframes produces a pandas dataframe utilized for topic modeling with the aid of a rich PyData ecosystem (NLP libraries for stop-word           removal and scikit-learn for topic modeling). The code is availble at __topic_modeling.py__ python file. 
  
  ### Movies Genre Classification 
  
  In this case study, we use RDFframes API to build a dataframe of movies from DBpedia knowledge graph (version 2020), along with the relevant information for the     task of movie genre classification. The dataframe then used to train a classifier to predict the genre for movies that do not have one, using any standard           classifier implemented in Python. The code is availble at __movie_genre_classification.py__ python file. 

 ### Songs Genre Classification 
 
   In this case study, we use RDFframes API to build a dataframe of songs from DBpedia knowledge graph (version 2020), along with the relevant information for the    task of songs genre classification. The dataframe then used to train a classifier to predict the Music genre for each song, using any standard classifier implemented in Python. The code is availble at __songs_genre_classification.py__ python file. 
 
 ### Knowledge Graph Embbeding
