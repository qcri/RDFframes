"""Represents a flat dataset
"""
import copy
import warnings

from rdfframes.query_buffer.query_operators.expandable.seed_operator import SeedOperator
from rdfframes.query_buffer.query_operators.shared.aggregation_operator import AggregationOperator
from rdfframes.query_buffer.query_operators.shared.expansion_operator import ExpansionOperator
from rdfframes.query_buffer.query_operators.shared.filter_operator import FilterOperator
from rdfframes.query_buffer.query_operators.shared.groupby_operator import GroupByOperator
from rdfframes.query_buffer.query_operators.shared.join_operator import JoinOperator
from rdfframes.query_buffer.query_operators.shared.integer_count_node import IntegerCountOperator
from rdfframes.dataset.dataset import Dataset
from rdfframes.dataset.grouped_dataset import GroupedDataset
from rdfframes.utils.constants import JoinType, AggregationFunction
from rdfframes.dataset.rdfpredicate import  PredicateDirection

__author__ = """
Abdurrahman Ghanem <abghanem@hbku.edu.qa>
Aisha Mohamed <ahmohamed@qf.org.qa>
Ghadeer Abuoda <gabuoda@hbku.edu.qa>
"""


class ExpandableDataset(Dataset):
    """
    The ExpandableDataset class is a representation of a flat table filled by entities obtained by following
    a particular path in the Knowledge Graph
    """

    def __init__(self, graph, dataset_name, seed_uri_list, seed_col_name):
        """
        Initializes a new dataset whose data is derived from a graph
        :param graph: KnowledgeGraph object representing the graph(s) from where the dataset is derived
        :param dataset_name: the name of the created dataset
        :param seed_uri_list: list of uris to initialize the first column of the dataset with
        :param seed_col_name: the name of the seed column
        """
        super(ExpandableDataset, self).__init__(graph, dataset_name)
        # creating and appending the root node to the query tree
        seed_node = SeedOperator(dataset_name, seed_uri_list, seed_col_name)
        self.query_queue.append_node(seed_node)
        self.columns.append(seed_col_name)
        self.agg_columns = []
        self.is_grouped = False

    def expand(self, src_col_name, predicate_list):
        """
        Expand the dataset from the source column based on the given predicates. Each entry in the predicate list
        should have a predicate URI, the new column name which will be used to name the new dataset column and a flag to
        indicate whether the expansion is ingoing or outgoing from the source column
        :param src_col_name: the starting column name
        :param predicate_list: list of RDFPredicate objects
        :return: the same dataset object, but logically a new column is appended. Actually a new node representing the
        operation is added to the query_buffer
        """
        if src_col_name not in self.columns:
            raise Exception("column {} not in the dataset".format(src_col_name))

        if self.cached:
            ds = self._cache_dataset()
            return ds.expand(src_col_name, predicate_list)

        for predicate in predicate_list:
            #if isinstance(predicate, RDFPredicate):
            #    node = ExpansionOperator(self.name, src_col_name, predicate.uri, predicate.new_col_name,
            #                                 predicate.direction, is_optional=predicate.optional)
            #    self.query_queue.append_node(node)
            #    self.add_column(predicate.new_col_name)
            #    self.add_column(predicate.uri)
            #else:
            if True:
                if len(predicate) > 3:
                    direction = predicate[3]
                    is_optional = predicate[2]
                else:
                    direction = PredicateDirection.OUTGOING
                    if len(predicate) > 2:
                        is_optional = predicate[2]
                    else:
                        is_optional = False
                node = ExpansionOperator(self.name, src_col_name, predicate[0], predicate[1],
                                         direction, is_optional=is_optional)
                self.query_queue.append_node(node)
                self.add_column(predicate[1])
                self.add_column(predicate[0])
        return self

    def join(self, dataset2, join_col_name1, join_col_name2=None, new_column_name=None, join_type=JoinType.InnerJoin):
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
            raise Exception("Join key {} doesn't exist in dataset 2".format(join_col_name2))

        warn_cols = []
        for col in dataset2.columns:
            if col != join_col_name2 and col in self.columns:
                warn_cols.append(col)
        if len(warn_cols) > 0:
            warnings.warn("columns {} are common between dataset 1 and 2. All these columns will be used as join columns".format(warn_cols))

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
        invalid_cols = [col for col in conditions_dict.keys() if col not in self.columns]
        if len(invalid_cols) > 0:
            raise Exception('Columns {} are not defined in the dataset'.format(invalid_cols))

        if self.cached:
            ds = self._cache_dataset()
            return ds.filter(conditions_dict)

        for col, conditions in conditions_dict.items():
            for cond in conditions:
                filter_node = FilterOperator(self.name, col, cond)
                self.query_queue.append_node(filter_node)
        return self

    def group_by(self, groupby_cols_list):
        """
        Group the table by the column names passed in groupby_cols_list
        :param groupby_cols_list: list of column names to group the table by
        :return: GroupedDataset object derived from self dataset with groupby_cols_list as grouping columns
        """
        invalid_cols = [col for col in groupby_cols_list if col not in self.columns]
        if len(invalid_cols) > 0:
            raise Exception('Columns {} are not defined in the dataset'.format(invalid_cols))

        if self.cached:
            ds = self._cache_dataset()
            return ds.group_by(groupby_cols_list)            

        groupby_ds_name = GroupedDataset.generated_grouped_ds_name(self.name, groupby_cols_list)
        groupby_node = GroupByOperator(self.name, groupby_cols_list, groupby_ds_name)

        grouped_ds = GroupedDataset(self, groupby_cols_list, groupby_node, groupby_ds_name)
        groupby_node.grouped_dataset = grouped_ds
        self.query_queue.append_node(groupby_node)

        return grouped_ds

    # aggregate functions

    def sum(self, src_col_name=None, new_col_name='sum'):
        """
        :param src_col_name: the column to find the sum of its values
        :param new_col_name: the new column name of the sum
        :return: if src_col_name is not None and is a groupby column, return a dataset with a new column name. else
            return an integer
        """
        if src_col_name not in self.columns:
            raise Exception("Aggregation column {} doesn't exist in this dataset".format(src_col_name))

        if self.cached:
            ds = self._cache_dataset()
            return ds.sum(src_col_name, new_col_name)

        agg_col = src_col_name
        # TODO: Don't allow any more operations on the dataset
        agg_node = AggregationOperator(self.name, agg_col, AggregationFunction.SUM, new_col_name, None)
        self.query_queue.append_node(agg_node)
        #self.add_column(new_col_name)
        # change the dataset to contain only the new columns
        self.old_columns = copy.copy(self.columns)
        self.columns = new_col_name
        self.agg_columns.append(new_col_name)
        return self

    def avg(self, src_col_name=None, new_col_name='avg'):
        """
        :param src_col_name: the column to find the max of its values
        :param new_col_name: the new column name of the max
        :return: if src_col_name is not None and is a groupby column, return a dataset with a new column name. else
            return an integer
        """
        if src_col_name not in self.columns:
            raise Exception("Aggregation column {} doesn't exist in this dataset".format(src_col_name))

        if self.cached:
            ds = self._cache_dataset()
            return ds.avg(src_col_name, new_col_name)

        agg_col = src_col_name
        # TODO: Don't allow any more operations on the dataset
        agg_node = AggregationOperator(self.name, agg_col, AggregationFunction.AVG, new_col_name, None)
        self.query_queue.append_node(agg_node)
        #self.add_column(new_col_name)
        self.old_columns = copy.copy(self.columns)
        self.columns = new_col_name
        self.agg_columns.append(new_col_name)
        return self

    def min(self, src_col_name=None, new_col_name='min'):
        """
        :param src_col_name: the column to find the min of its values
        :param new_col_name: the new column name of the min
        :return: if src_col_name is not None and is a groupby column, return a dataset with a new column name. else
            return an integer
        """
        if src_col_name not in self.columns:
            raise Exception("Aggregation column {} doesn't exist in this dataset".format(src_col_name))
        if self.cached:
            ds = self._cache_dataset()
            return ds.min(src_col_name, new_col_name)

        agg_col = src_col_name
        # TODO: Don't allow any more operations on the dataset
        agg_node = AggregationOperator(self.name, agg_col, AggregationFunction.MIN, new_col_name, None)
        self.query_queue.append_node(agg_node)
        #self.add_column(new_col_name)
        self.old_columns = copy.copy(self.columns)
        self.columns = new_col_name
        self.agg_columns.append(new_col_name)
        return self

    def max(self, src_col_name=None, new_col_name='max'):
        """
        :param src_col_name: the column to find the max of its values
        :param new_col_name: the new column name of the max
        :return: if src_col_name is not None and is a groupby column, return a dataset with a new column name. else
            return an integer
        """
        if src_col_name not in self.columns:
            raise Exception("Aggregation column {} doesn't exist in this dataset".format(src_col_name))
        if self.cached:
            ds = self._cache_dataset()
            return ds.max(src_col_name, new_col_name)

        agg_col = src_col_name
        # TODO: Don't allow any more operations on the dataset
        agg_node = AggregationOperator(self.name, agg_col, AggregationFunction.MAX, new_col_name, None)
        self.query_queue.append_node(agg_node)
        #self.add_column(new_col_name)
        self.old_columns = copy.copy(self.columns)
        self.columns = new_col_name
        self.agg_columns.append(new_col_name)
        return self

    def count(self, src_col_name=None, new_col_name='count', unique=True):
        """
        :param src_col_name: the column to count its values
        :param new_col_name: the new column name of the count
        :param unique: if True retun the number of unique values else return the size of the result set
        :return: if src_col_name is not None and is a groupby column, return a dataset with a new column name. else
            return an integer
        """
        if self.cached:
            ds = self._cache_dataset()
            return ds.count(src_col_name, new_col_name, unique)

        if unique:
            param = "DISTINCT"
        else:
            param = None
        if src_col_name is not None:
            if src_col_name not in self.columns:
                raise Exception("Aggregation column {} doesn't exist in this dataset".format(src_col_name))
            agg_col = src_col_name
            # TODO: Don't allow any more operations on the dataset
            agg_node = AggregationOperator(self.name, agg_col, AggregationFunction.COUNT, new_col_name, param)
            self.query_queue.append_node(agg_node)
            #self.add_column(new_col_name)
            self.old_columns = copy.copy(self.columns)
            self.columns = new_col_name
            self.agg_columns.append(new_col_name)
        else:
            # TODO: Don't allow any more operations on the dataser
            agg_node = IntegerCountOperator(self.name, new_col_name, param)
            self.query_queue.append_node(agg_node)
            self.old_columns = copy.copy(self.columns)
            self.columns = new_col_name
            #self.add_column(new_col_name)
            self.agg_columns.append(new_col_name)
        return self

    def type(self):
        """
        return the type of the dataset as string
        :return: dataset type as string
        """
        return "ExpandableDataset"
