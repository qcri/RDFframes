''' Get a list of actors available in DBpedia or YAGO graphs. '''

from rdfframes.knowledge_graph import KnowledgeGraph
from rdfframes.utils.constants import JoinType
from rdfframes.client.http_client import HttpClientDataFormat, HttpClient
from time import time

graph1 =  KnowledgeGraph(graph_name='dbpedia')
graph2 = KnowledgeGraph(graph_name='yago',
                        graph_uri='http://yago-knowledge.org/',
                        prefixes={
                            'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
                            'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
                            'yago': 'http://yago-knowledge.org/resource/',
                            'yagoinfo': 'http://yago-knowledge.org/resource/infobox/en/'
                        })
graph3 = KnowledgeGraph( graph_name = 'dblp', graph_uri='http://dblp.l3s.de',
                        prefixes={
                        "xsd": "http://www.w3.org/2001/XMLSchema#",
                        "swrc": "http://swrc.ontoware.org/ontology#",
                        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                        "dc": "http://purl.org/dc/elements/1.1/",
                        "dcterm": "http://purl.org/dc/terms/",
                        "dblprc": "http://dblp.l3s.de/d2r/resource/conferences/",
                        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                         })


def join(join_type):
    dbpedia_person = graph1.entities("dbpo:Person",entities_col_name="person1")  \
         .expand('person1', [('dbpp:birthPlace', 'country1'), ('dbpp:name', 'name1')]) \
        .filter({'country1': ['regex(str(?country1), "USA")']})\
            .filter({'name1':['regex(str(?name1),"Abraham")']}).select_cols(['name1'])
          
    yago_person = graph2.feature_domain_range("rdf:type","person2","p")\
        .expand('p', [ ('rdfs:label', 'label')])\
            .expand('person2',[('yagoinfo:name',"name2"),('yago:isCitizenOf', 'country2')])\
                .filter({'label': ['="person"@eng']}).filter({'country2': ['= yago:United_States']})\
                    .filter({'name2':['regex(str(?name2),"Abraham")']})
                                                                                
    dbpl_person = graph3.feature_domain_range("dc:creator","paper","author")\
        .expand('author', [ ('rdfs:label', 'name3')])\
            .expand('paper', [('dcterm:issued','date')])\
                .filter({'date': ['>= 2015'] })\
                    .filter({'name3':['regex(str(?name3),"Abraham")']})#.select_cols(['name3'])
      
    endpoint = 'http://10.161.202.101:8890/sparql/'
    output_format = HttpClientDataFormat.PANDAS_DF
    client = HttpClient(endpoint_url=endpoint, return_format=output_format)
    shared_ppl = dbpl_person.join(yago_person, 'name3','name2', join_type=join_type)
    shared_ppl2 = shared_ppl.join(dbpedia_person, 'name3','name1', join_type=join_type)
    print(shared_ppl2.to_sparql())


join(JoinType.OuterJoin)





