"""Main class represents a SPARQL query that generates a table-like dataset
"""
import copy
import time
import pandas as pd

from rdfframes.query_buffer.query_operators.shared.limit_operator import LimitOperator
from rdfframes.query_buffer.query_operators.shared.offset_operator import OffsetOperator
from rdfframes.query_buffer.query_operators.shared.sort_operator import SortOperator
from rdfframes.query_buffer.query_operators.shared.select_operator import SelectOperator
from rdfframes.query_buffer.query_queue import QueryQueue
from rdfframes.query_builder.queue2querymodel import Queue2QueryModelConverter
from rdfframes.utils.constants import JoinType
from rdfframes.utils.helper_functions import is_uri


__author__ = """
Abdurrahman Ghanem <abghanem@hbku.edu.qa>
Aisha Mohamed <ahmohamed@qf.org.qa>
Zoi Kaoudi <zkaoudi@hbku.edu.qa
Ghadeer Abuoda <gabuoda@hbku.edu.qa>
"""


class Dataset:
    """
    The Dataset abstract base class which represents a table filled by entities obtained by following
    a particular path in the Knowledge Graph
    """

    def __init__(self, graph, dataset_name):
        """
        Initializes a new dataset whose data is derived from a graph
        :param graph: KnowledgeGraph object representing the graph(s) from where the dataset is derived
        :param dataset_name: the name of the created dataset the query tree
        """
        self.graph = graph
        self.name = dataset_name
        self.query_queue = QueryQueue(self)
        self.columns = []
        self.old_columns = []
        self.cached = False
        self.is_grouped = False

    def expand(self, src_col_name, predicate_list):
        """
        Expand the dataset from the source column based on the given predicates. Each entry in the predicate list
        should have a predicate URI, the new column name which will be used to name the new dataset column and a flag to
        indicate whether the expansion is ingoing or outgoing from the source column
        Requires src_col_name in self.columns
        Ensures for each predicate, new_col_name in the new dataset
        :param src_col_name: the starting point
        :param predicate_list: list of RDFPredicate objects each one containing: (1) predicate URI, (2) new column name,
         (3) optional flag, and (4) ingoing or outgoing flag
        :return: the same dataset object, but logically a new column is appended.
        """
        pass

    def join(self, dataset2, join_col_name1, join_col_name2=None, new_column_name=None, join_type=JoinType.InnerJoin):
        """
        Join this dataset with datset 2.
        :param dataset2:
        :param join_col_name1:

        :param join_col_name2:
        :param new_column_name:
        :param join_type:
        :return:
        """
        pass

    def filter(self, predicate_dict):
        """
        Apply the given filters on the corresponding columns in the dataset.
        :param predicate_dict: mapping from column name to a list of predicates to apply on the column. Format:
        {'col_name': [pred1, pred2 ... etc], ...}
        :return: the same dataset object logically with the filtered column.
        """
        pass

    def select_cols(self, col_list):
        """
        Select the columns of interest from the returned dataset when executing the SPARQL query
        :param col_list: list of column names to return
        :return: a dataset that contains only the selected columns and the rows that have a value for at least one of
            the selected columns
        """
        invalid_cols = [col for col in col_list if col not in self.columns]
        if len(invalid_cols) > 0:
            raise Exception('Columns {} are not defined in the dataset'.format(invalid_cols))

        select_node = SelectOperator(self.name, col_list)
        self.query_queue.append_node(select_node)

        # change the dataset to contain only the new columns
        self.old_columns = copy.copy(self.columns)
        self.columns = col_list

        return self

    def group_by(self, groupby_cols_list):
        """
        Group the table by the column names passed in groupby_cols_list
        :param groupby_cols_list: list of column names to group the table by
        :return: GroupedDataset object derived from self dataset with groupby_cols_list as grouping columns
        """
        pass

    def sort(self, sort_dict):
        """
        sorts the dataset based on the given column names and the corresponding sort orders
        :param sort_dict: mapping from the sorting column names to the corresponding sort order. Format:
        {'sort_col1': 'DESC', 'sort_col2': 'ASC', ... etc}
        :return: the same dataset object logically ordered
        """
        sort_node = SortOperator(self.name, sort_dict)
        self.query_queue.append_node(sort_node)
        return self

    def limit(self, threshold):
        """
        limit the number of returned records to the passed threshold
        :param threshold: the cut off threshold
        :return: the same dataset object
        """
        limit_node = LimitOperator(self.name, threshold)
        self.query_queue.append_node(limit_node)
        return self

    def offset(self, offset):
        """
        starts returning records after the passed offset (offset keyword in SPARQL)
        :param offset: the offset (int)
        :return: the same dataset object
        """
        offset_node = OffsetOperator(self.name, offset)
        self.query_queue.append_node(offset_node)
        return self

    # aggregate functions

    def sum(self, aggregation_fn_data):
        """
        Runs sum aggregate function on the pass rdframe columns list and returns the summation of the passed columns as
        a list of scalar values
        :param aggregation_fn_data: list of AggregationData class holding the aggregation functions' information
        :return: the same dataset object
        """
        pass

    def avg(self, aggregation_fn_data):
        """
        Runs average aggregate function on the pass rdframe columns list and returns the average of the passed columns as
        a list of scalar values
        :param aggregation_fn_data: list of AggregationData class holding the aggregation functions' information
        :return: the same dataset object
        """
        pass

    def min(self, aggregation_fn_data):
        """
        Runs min aggregate function on the pass rdframe columns list and returns the min of the passed columns as
        a list of scalar values
        :param aggregation_fn_data: list of AggregationData class holding the aggregation functions' information
        :return: the same dataset object
        """
        pass

    def max(self, aggregation_fn_data):
        """
        Runs max aggregate function on the pass rdframe columns list and returns the max of the passed columns as
        a list of scalar values
        :param aggregation_fn_data: list of AggregationData class holding the aggregation functions' information
        :return: the same dataset object
        """
        pass

    def count(self, aggregation_fn_data):
        """
        Runs count aggregate function on the pass rdframe columns list and returns the count of the passed columns as
        a list of scalar values
        :param aggregation_fn_data: list of AggregationData objects holding the aggregation functions' information
        :return: the same dataset object
        """
        pass

    def print_query_structure(self, filename=None):
        """
        prints the queue that keeps all API calls to this dataset
        :return:
        """
        self.query_queue.print_query_queue(filename)

    def to_sparql(self):
        """
        Converts the API calls of this dataset to a SPARQL query
        :return: the SPARQL query as a string
        """
        converter = Queue2QueryModelConverter(self)
        query_model = converter.to_query_model()
        query_string = query_model.to_sparql()
        return query_string

    def execute(self, client, return_format=None, output_file=None):
        """
        converts this dataset to a sparql query, send it to the sparql endpoint or RDF engine and
        returns the result in the specified return format
        :param client: client to communicate with the SPARQL endpoint/RDF engine
        :param return_format: one of ['df', 'csv']
        :param output_file: file to save the results in
        :return:
        """
        start_time = time.time()
        query_string = self.to_sparql()
        res = client.execute_query(query_string, return_format=return_format, output_file=output_file)
        return res

    def type(self):
        """
        return the type of the dataset as string
        :return: dataset type as string
        """
        pass

    def get_columns(self):
        """
        prints the column names of this dataset
        :return:
        """
        print("columns:")
        for i in range(0,len(self.columns)):
            print(self.columns[i])

    def add_column(self, column):
        if not is_uri(column) and column.find(":") < 0:
            self.columns.append(column)

    def rem_column(self, column):
        if column in self.columns:
            self.columns.remove(column)

    def cache(self):
        self.cached = True
        #return self._cache_dataset()
        return self

    def _cache_dataset(self):
        ds = copy.deepcopy(self)
        ds.cached = False
        return ds





