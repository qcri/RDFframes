


  # RDFFrames import 
  from rdfframes.knowledge_graph import KnowledgeGraph
  from rdfframes.dataset.rdfpredicate import RDFPredicate
  from rdfframes.client.http_client import HttpClientDataFormat, HttpClient    
  
  # External API imports: ampligraph library
  
  from ampligraph.latent_features import ComplEx
  from ampligraph.evaluation import evaluate_performance, mrr_score, hits_at_n_score
  from ampligraph.evaluation import train_test_split_no_unseen 
  
  # Client and the SPARQL Endpoint
  
  endpoint = 'http://10.161.202.101:8890/sparql/'
  port = 8890
  output_format = HttpClientDataFormat.PANDAS_DF
  client = HttpClient(endpoint_url=endpoint, port=port, return_format=output_format, timeout=timeout,
                      default_graph_uri=default_graph_url, max_rows=max_rows)
  
  # Get all triples where the object is a URI
  dataset = graph.feature_domain_range(s, p, o).filter({o: ['isURI']})
  
  # execute 
  df = dataset.execute(client, return_format=output_format)
    
  # Train/test split and create ComplEx model from ampligraph library
  
  triples = df.to_numpy()
  X_train, X_test = train_test_split_no_unseen(triples, test_size=10000)
  
  # use ComplEx model to build the embedding 
  model = ComplEx(batches_count=50,epochs=300,k=100,eta=20, optimizer='adam',optimizer_params={'lr':1e-4}, 
          loss='multiclass_nll',regularizer='LP', regularizer_params={'p':3, 'lambda':1e-5}, seed=0,verbose=True)
  model.fit(X_train)
  
  # Evaluate the embedding model
  filter_triples = np.concatenate((X_train, X_test))
  ranks = evaluate_performance(X_test, model=model, filter_triples=filter_triples,
                                use_default_protocol=True, verbose=True)
  mr  = mr_score(ranks)
  mrr = mrr_score(ranks)
