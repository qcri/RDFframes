''' Get a list of the books in DBpedia that were written by American authors who wrote more than two books. 
    For each author, return the birthPlace, country, education, and for each book return the title, subject, country (if available), and publisher (if available). '''


from rdfframes.knowledge_graph import KnowledgeGraph
from rdfframes.utils.constants import JoinType


graph =  KnowledgeGraph(graph_name='dbpedia')

  
def books_with_authors_cache():
    graph = KnowledgeGraph(graph_name='dbpedia2020')
    dataset = graph.feature_domain_range('dbpp:author', 'book', 'author')\
        .expand('author', [('dbpp:birthPlace', 'author_country'),(' dbpp:education','education')])\
        .expand('book', [('rdfs:label', 'work_name'),('dcterms:subject', 'subject')]).cache()

    american_authors = dataset.filter({'author_country': ['regex(str(?subject), "History")']}) 
    
    famous_authors = dataset.group_by(['author'])\
        .count('book', 'book_count', unique=True).filter({'book_count': ['>= 2']})

    books = american_authors.join(famous_authors, join_col_name1='author', join_type=JoinType.OuterJoin)
    print(books.to_sparql())


books_with_authors_cache()
