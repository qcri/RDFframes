"""Main class represents a SPARQL query that generates a table-like dataset
"""
import copy
import warnings

from rdfframes.query_buffer.query_operators.shared.limit_operator import LimitOperator
from rdfframes.query_buffer.query_operators.shared.offset_operator import OffsetOperator
from rdfframes.query_buffer.query_operators.shared.sort_operator import SortOperator
from rdfframes.query_buffer.query_operators.shared.select_operator import SelectOperator
from rdfframes.query_buffer.query_operators.shared.join_operator import JoinOperator
from rdfframes.query_buffer.query_operators.shared.expansion_operator import ExpansionOperator
from rdfframes.query_buffer.query_queue import QueryQueue
from rdfframes.query_builder.queue2querymodel import Queue2QueryModelConverter
from rdfframes.utils.constants import JoinType
from rdfframes.dataset.rdfpredicate import PredicateDirection
from rdfframes.utils.helper_functions import is_uri
from rdfframes.utils.constants import _TIMEOUT, _MAX_ROWS


__author__ = """
Abdurrahman Ghanem <abghanem@hbku.edu.qa>
Aisha Mohamed <ahmohamed@hbku.edu.qa>
Zoi Kaoudi <zkaoudi@hbku.edu.qa>
Ghadeer Abuoda <gabuoda@hbku.edu.qa>
"""


class Dataset:
    """
    The Dataset abstract base class which represents a table filled by data obtained from a Knowledge Graph
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
        :param predicate_list: list of RDF predicates each one containing: (1) predicate URI, (2) new column name,
         (3) optional flag, and (4) ingoing or outgoing flag
        :return: the same dataset object, but logically a new column is appended.
        """
        if src_col_name not in self.columns:
            raise Exception("{} doesn't exist in the dataset".format(src_col_name))

        if self.cached:
            ds = self._cache_dataset()
            return ds.expand(src_col_name, predicate_list)

        for predicate in predicate_list:
            if len(predicate) > 3:
                direction = predicate[3]
                is_optional = predicate[2]
            else:
                direction = PredicateDirection.OUTGOING
                if len(predicate) > 2:
                    is_optional = predicate[2]
                else:
                    is_optional = False
            operator = ExpansionOperator(self.name, src_col_name, predicate[0], predicate[1],
                                         direction, is_optional=is_optional)
            self.query_queue.append_node(operator)
            self.add_column(predicate[1])
            self.add_column(predicate[0])
        return self

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
        """
        Join this dataset with datset 2. The join key in this dataset is join_col_name1.
        The join key is dataset2 is join_col_name2 if passed. Otherwise, it is assumed to be the same (join_col_name1).
        If new_col_name is passed, rename the join column in the new dataset to new_Col_name,.
        :param dataset2:
        :param join_col_name1:
        :param join_col_name2:
        :param new_column_name:
        :param join_type:
        :return:
        """
        if self.cached:
            ds = self._cache_dataset()
            return ds.join(dataset2, join_col_name1, join_col_name2, new_column_name, join_type)

        if join_col_name1 not in self.columns:
            raise Exception("column {} not in the dataset".format(join_col_name1))
        # specify the join key in dataset2
        if join_col_name2 is None:
            if join_col_name1 not in dataset2.columns:
                raise Exception("No join key specified for dataset2 and join_col_name1 is not in dataset2")
            else:
                join_col_name2 = join_col_name1
        elif join_col_name2 not in dataset2.columns:
            raise Exception("Join key {} doesn't exist in dataset2".format(join_col_name2))

        warn_cols = []
        for col in dataset2.columns:
            if col != join_col_name2 and col in self.columns:
                warn_cols.append(col)
        if len(warn_cols) > 0:
            warnings.warn("columns {} are common between dataset 1 and 2. All these columns will be used as "
                          "join columns".format(warn_cols))

        # find the new column name
        if new_column_name is None:
            new_column_name = join_col_name1
        else: # new_column_name is not None
            self.rem_column(join_col_name1)
            self.add_column(new_column_name)

        node = JoinOperator(self, dataset2, join_col_name1, join_col_name2, join_type, new_column_name)

        # ds1.columns = union(ds1.columns, ds2.columns)
        for col in dataset2.columns:
            if col not in self.columns and col != join_col_name2:
                self.add_column(col)

        self.query_queue.append_node(node)
        # TODO: if we allow the join between different graphs, Union the graphs

        return self

    def filter(self, conditions_dict):
        """
        Apply the given filters on the corresponding columns in the dataset.
        :param conditions_dict: mapping from column name to a list of predicates to apply on the column. Format:
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

    def execute(self, client, return_format=None, output_file=None, timeout = _TIMEOUT, limit = _MAX_ROWS):
        """
        converts this dataset to a sparql query, send it to the sparql endpoint or RDF engine and
        returns the result in the specified return format
        :param client: client to communicate with the SPARQL endpoint/RDF engine
        :param return_format: one of ['df', 'csv']
        :param output_file: file to save the results in
        :return:
        """
        query_string = self.to_sparql()
        res = client.execute_query(query_string, timeout=timeout, limit=limit, return_format=return_format,
                                   output_file=output_file)
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
        return self

    def _cache_dataset(self):
        ds = copy.deepcopy(self)
        ds.cached = False
        return ds





