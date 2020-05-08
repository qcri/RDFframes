"""RDF predicate to expand a column in a dataset
"""

__author__ = 'Abdurrahman Ghanem <abghanem@hbku.edu.qa>'


class PredicateDirection:
    OUTGOING = 'OUTGOING'
    INCOMING = 'INCOMING'


#class RDFPredicate:
#    """
#    Holds the information of an RDF predicate for expanding a column in a dataset
#    """

#    def __init__(self, p_uri, new_col_name, optional=False, directionality=PredicateDirection.OUTGOING):
#        """
#        :param p_uri: the URI of the RDF predicate
#        :param new_col_name: the column name in the dataset
#        :param directionality: in or out direction
#        """
#        self.uri = p_uri
#        self.new_col_name = new_col_name
#        self.direction = directionality
#        self.optional = optional
