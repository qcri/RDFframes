


  # Get all triples where the object is a URI
  from rdfframes.knowledge_graph import KnowledgeGraph
  from rdfframes.dataset.rdfpredicate import RDFPredicate
  from rdfframes.client.http_client import HttpClientDataFormat, HttpClient    
  output_format = HttpClientDataFormat.PANDAS_DF
  client = HttpClient(endpoint_url=endpoint, port=port, return_format=output_format, timeout=timeout,
                      default_graph_uri=default_graph_url, max_rows=max_rows)
  dataset = graph.feature_domain_range(s, p, o).filter({o: ['isURI']})
  df = dataset.execute(client, return_format=output_format)
    
  # Train/test split and create ComplEx model from ampligraph library
  from ampligraph.evaluation import train_test_split_no_unseen 
  triples = df.to_numpy()
  X_train, X_test = train_test_split_no_unseen(triples, test_size=10000)
  from ampligraph.latent_features import ComplEx
  from ampligraph.evaluation import evaluate_performance, mrr_score, hits_at_n_score
  model = ComplEx(batches_count=50,epochs=300,k=100,eta=20, optimizer='adam',optimizer_params={'lr':1e-4}, 
          loss='multiclass_nll',regularizer='LP', regularizer_params={'p':3, 'lambda':1e-5}, seed=0,verbose=True)
  model.fit(X_train)
  # Evaluate embedding model
  filter_triples = np.concatenate((X_train, X_test))
  ranks = evaluate_performance(X_test, model=model, filter_triples=filter_triples,
                                use_default_protocol=True, verbose=True)
  mr  = mr_score(ranks)
  mrr = mrr_score(ranks)
