"""Represents a flat dataset
"""
from rdfframes.query_buffer.query_operators.expandable.seed_operator import SeedOperator
from rdfframes.query_buffer.query_operators.shared.aggregation_operator import AggregationOperator
from rdfframes.query_buffer.query_operators.shared.filter_operator import FilterOperator
from rdfframes.query_buffer.query_operators.shared.groupby_operator import GroupByOperator
from rdfframes.query_buffer.query_operators.shared.integer_count_node import IntegerCountOperator
from rdfframes.dataset.dataset import Dataset
from rdfframes.dataset.grouped_dataset import GroupedDataset
from rdfframes.utils.constants import AggregationFunction

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

    def sum(self, src_col_name, new_col_name='sum'):
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
        self.columns = [src_col_name, new_col_name]
        self.agg_columns.append(new_col_name)
        return self

    def avg(self, src_col_name, new_col_name='avg'):
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
        self.columns = [src_col_name, new_col_name]
        self.agg_columns.append(new_col_name)
        return self

    def min(self, src_col_name, new_col_name='min'):
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
        self.columns = [src_col_name, new_col_name]
        self.agg_columns.append(new_col_name)
        return self

    def max(self, src_col_name, new_col_name='max'):
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
        self.columns = [src_col_name, new_col_name]
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
            self.columns = [src_col_name, new_col_name]
            self.agg_columns.append(new_col_name)
        else:
            # TODO: Don't allow any more operations on the dataser
            agg_node = IntegerCountOperator(self.name, new_col_name, param)
            self.query_queue.append_node(agg_node)
            self.columns = [new_col_name]
            self.agg_columns.append(new_col_name)
        return self

    def type(self):
        """
        return the type of the dataset as string
        :return: dataset type as string
        """
        return "ExpandableDataset"
