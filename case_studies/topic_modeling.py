# RDFFrames imports, graph, prefixes, and client

  import pandas as pd
  from rdfframes.client.http_client import HttpClientDataFormat, HttpClient
  from rdfframes.knowledge_graph import KnowledgeGraph
  graph = KnowledgeGraph(
          graph_uri = 'http://dblp.l3s.de',
          prefixes = {"xsd": "http://www.w3.org/2001/XMLSchema#",
                    "swrc": "http://swrc.ontoware.org/ontology#",
                    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                    "dc": "http://purl.org/dc/elements/1.1/",
                    "dcterm": "http://purl.org/dc/terms/",
                    "dblprc": "http://dblp.l3s.de/d2r/resource/conferences/"})
  output_format = HttpClientDataFormat.PANDAS_DF
  client = HttpClient(endpoint_url=endpoint, port=port,return_format=output_format)

  # RDFFrames code for creating the dataframe
  papers = graph.entities('swrc:InProceedings', paper)
  papers = papers.expand('paper',[('dc:creator', 'author'),('dcterm:issued', 'date'), ('swrc:series', 'conference'),
                        ('dc:title', 'title')]).cache()
  authors = papers.filter({'date': ['>=2005'],'conference': ['In(dblp:vldb, dblp:sigmod)']}).group_by(['author'])
                   . count('paper', 'n_papers').filter({'n_papers': '>=20', 'date': ['>=2005']})
  titles = papers.join(authors, 'author', InnerJoin).select_cols(['title'])
  df = titles.execute(client, return_format=output_format)

  # Preprocessing and cleaning
  from nltk.corpus import stopwords
  df['clean_title'] = df['title'].str.replace("[^a-zA-Z#]", " ")
  df['clean_title'] = df['clean_title'].apply(lambda x: x.lower())
  df['clean_title'] = df['clean_title'].apply(lambda x: ' '.join([w for w in str(x).split() if len(w)>3])) 
  stop_words        = stopwords.words('english')
  tokenized_doc     = df['clean_title'].apply(lambda x: x.split())
  df['clean_title'] = tokenized_doc.apply(lambda x:[item for item in x if item not in stop_words])

  # Vectorization and SVD model using the scikit-learn library
  from sklearn.feature_extraction.text import TfidfVectorizer
  from sklearn.decomposition import TruncatedSVD
  vectorizer  = TfidfVectorizer(stop_words='english', max_features= 1000, max_df = 0.5, smooth_idf=True)
  Tfidf_title = vectorizer.fit_transform(df['clean_title'])
  svd_model   = TruncatedSVD(n_components=20, algorithm='randomized',n_iter=100, random_state=122)
  svd_model.fit(Tfidf_titles)  

  # Extracting the learned topics and their keyterms
  terms = vectorizer.get_feature_names()
  for i, comp in enumerate(svd_model.components_):
      terms_comp   = zip(terms, comp)
      sorted_terms = sorted(terms_comp, key= lambda x:x[1], reverse=True)[:7]
      print_string = "Topic"+str(i)+": "
      for t in sorted_terms:
          print_string += t[0] + " "
