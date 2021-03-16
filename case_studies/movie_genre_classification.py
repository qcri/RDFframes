
  # RDFFrames imports, graph, and prefixes
  
  from rdfframes.knowledge_graph import KnowledgeGraph
  from rdfframes.dataset.rdfpredicate import RDFPredicate
  from rdfframes.utils.constants import JoinType
  from rdfframes.client.http_client import HttpClientDataFormat, HttpClient
  
  # External packages import: Sklearn, NTLK
  from sklearn.model_selection import train_test_split
  from sklearn.preprocessing import StandardScaler
  from sklearn.ensemble import RandomForestClassifier
  import re
  import nltk
 
  # Graph, client, and the SPARQL endpoint URI
   
  graph = KnowledgeGraph(graph_uri='http://dbpedia.org',
                         prefixes= {'dcterms': 'http://purl.org/dc/terms/',
                                  'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
                                  'dbpprop': 'http://dbpedia.org/property/',
                                  'dbpr': 'http://dbpedia.org/resource/'}) 
  endpoint = 'http://10.161.202.101:8890/sparql/'
  output_format = HttpClientDataFormat.PANDAS_DF
  timeout = 12000
  client = HttpClient(endpoint_url=endpoint, return_format=output_format)

  # RDFFrames code for creating the dataframe
  
  dataset = graph.feature_domain_range('dbpp:starring', 'movie', 'actor')\
           .expand('actor', [('dbpp:birthPlace', 'actor_country'), ('rdfs:label', 'actor_name')])\
           .expand('movie', [('rdfs:label', 'movie_name'),('dcterms:subject', 'subject'), ('dbpp:genre', 'genre', True)]).cache() 
    
  american_actors = dataset.filter({'actor_country': ['regex(str(?actor_country), "USA")']}) 
  prolific_actors = dataset.group_by(['actor'])    .count('movie', 'movie_count', unique=True).filter({'movie_count': ['>= 100']})
  movies = american_actors.join(prolific_actors, join_col_name1='actor', join_type=JoinType.OuterJoin).join(dataset, join_col_name1='actor')\
          .select_cols([ "actor_name","movie_name","actor_country","genre","subject"]) 
  sparql_query = movies.to_sparql()
  print(sparql_query)
  
  #  execution
 
  df = movies.execute(client, return_format=output_format)

  # Preprocessing and preparation
 
  regex = re.compile(
        r'^(?:http|ftp)s?://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
        r'localhost|' #localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
  
  # for cleaning the URL
  def clean(dataframe):
    for i, row in df.iterrows():
      if df.loc[i]['genre'] != None:
      value =df.at[i, 'genre']
      if re.match(regex,str(value)) is not None:
          df.at[i, 'genre'] = value.split('/')[-1]
    return dataframe

  # Remove URL from the 'genre' and convert to label keys
  df=clean(df)

  # Find the most most frequent genres
  all_genres = nltk.FreqDist(df['genre'].values)
  all_genres_df = pd.DataFrame({'genre':list(all_genres.keys()), 'Count':list(all_genres.values())})
  all_genres_df.sort_values(by=['Count'],ascending=False)
  
  # In this example, use 900 movies as a cut off for the frequent movies
  most_frequent_genres = all_genres_df[all_genres_df['Count']> 900]
  df = df[df['genre'].isin(list(most_frequent_genres['genre']))]

  # Features and factorization

  df= df.apply(lambda col: pd.factorize(col, sort=True)[0])
  features = ["movie_name", "actor_name", "actor_country","subject","movie_country", "subject"]
  df = df.dropna(subset=['genre'])
  x = df[features]
  y = df['genre']
  x_train, x_test, y_train, y_test = train_test_split(x, y, random_state=20)
  sc = StandardScaler()
  x_train = sc.fit_transform(x_train)
  x_test = sc.fit_transform(x_test)

  # Random Forest classifier
  
  model=RandomForestClassifier(n_estimators=100)
  model.fit(x_train,y_train)
  model.fit(x_train,y_train)
  y_pred=clf.predict(x_test)
  print("Accuracy:",metrics.accuracy_score(y_test, y_pred))
    
