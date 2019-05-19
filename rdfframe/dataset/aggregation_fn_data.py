"""Encapsulates aggregation function's data
"""

__author__ = 'Abdurrahman Ghanem <abghanem@hbku.edu.qa>'


class AggregationData:
    """
    Holds the information needed by an aggregation function
    """

    def __init__(self, src_col_name, new_col_name=None, agg_parameter=None):
        """

        :param src_col_name: which column the aggregation is performed on
        :param new_col_name: the alias of the aggregation column
        :param agg_parameter: can be Distinct for count
        """
        self.src_col_name = src_col_name
        self.new_col_name = new_col_name
        self.agg_parameter = agg_parameter
