from rdframe.query_buffer.query_operators.grouped.groupby_seed_operator import GroupBySeedOperator
from rdframe.query_buffer.query_operators.grouped.grouped_aggregation_operator import GroupedAggregationOperator
from rdframe.query_buffer.query_operators.grouped.having_operator import HavingOperator
from rdframe.query_buffer.query_operators.shared.expansion_operator import ExpansionOperator
from rdframe.query_buffer.query_operators.shared.filter_operator import FilterOperator
from rdframe.query_buffer.query_operators.shared.groupby_operator import GroupByOperator
from rdframe.query_buffer.query_operators.shared.select_operator import SelectOperator
from rdframe.dataset.dataset import Dataset
from rdframe.utils.constants import AggregationFunction
from rdframe.utils.constants import JoinType

__author__ = """
Abdurrahman Ghanem <abghanem@hbku.edu.qa>
Aisha Mohamed <ahmohamed@qf.org.qa>
Ghadeer Abuoda <gabuoda@hbku.edu.qa>
"""


class GroupedDataset(Dataset):
    """
    The GroupedDataset class represents a special kind of dataset resulted from running group by operator on a Dataset
    object. It holds reference to the dataset it was created from (parent dataset) and which columns are used to perform
    the grouping
    """

    def __init__(self, parent_dataset, grouping_cols, groupby_node, groupby_dataset_name):
        """
        Initialize the new GroupedDataset object with the parent dataset and the grouping columns
        :param parent_dataset: the dataset from which self dataset is derived from
        :param grouping_cols: the columns used to group the parent dataset
        :param groupby_node: the groupby Operator in the original's dataset query graph
        :param groupby_dataset_name: the new name assigned to this groupby dataset
        """
        super(GroupedDataset, self).__init__(parent_dataset.graph, groupby_dataset_name)

        self.parent_dataset = parent_dataset
        self.grouping_cols = grouping_cols
        self.agg_columns = []
        self.columns = grouping_cols.copy()

        # create groupby seed Operator and append it to the query tree
        gbsn = GroupBySeedOperator(self.name, groupby_node)
        self.query_queue.append_node(gbsn)

    def expand(self, src_col_name, predicate_list):
        """
        Expands the dataset from the source column based on the given predicates. Each entry in the prdicate list
        should have predicate URI, the new column name which will be used to name the new dataset column and flag to
        indicate whether the expansion is ingoing or outgoing from the source column and a flag to indicate if this
        predicate is optional or not
        each predicate is translated to the following graph pattern
        if predicate.direction = outgoing:
            src_col_name predicate.uri new_col_val
        if predicate.direction = ingoing:
            new_col_val predicate.uri src_col_name
        :param src_col_name: the starting column that will be expanded
        :param predicate_list: list of instances of RDFPredicate, each containing 1) predicate URI, 2) new column name
        and 3) ingoing or outgoing flag
        :return: the same dataset object, but logically a new column is appended. Actually a new Operator representing
        the operation is added to the query_buffer for each predicate.
        """
        for predicate in predicate_list:
            operator = ExpansionOperator(self.name, src_col_name, predicate.uri, predicate.new_col_name,
                                         predicate.direction, is_optional=predicate.optional)
            self.query_queue.append_node(operator)
            self.add_column(predicate.new_col_name)
            self.add_column(predicate.uri)
        return self

    def join(self, other_dataset, join_col_name, other_dataset_join_col_name=None, join_type=JoinType.InnerJoin):
        """
        Join the self dataset with the passed dataset based on a common column holding the same name
        :param other_dataset: the other dataset to merge with
        :param join_col_name: the join column of the self dataset
        :param other_dataset_join_col_name: the join column in the other dataset, if None use join_col_name
        :param join_type: the join operation type: inner, left outer, right outer, by default inner join is performed
        :return: the same dataset object, but logically with the columns from other_dataset appended to myself.
        """
        pass

    def filter(self, conditions_dict):
        """
        Apply the given filters on the corresponding columns in the dataset.
        :param conditions_dict: mapping from column name to a list of predicates to apply on the column. Format:
        {'col_name': [pred1, pred2 ... etc], ...}
        :return: the same dataset object logically with the filtered column.
        """
        for src_col_name in conditions_dict:
            if src_col_name in self.agg_columns:
                operator = HavingOperator(self.name, src_col_name, conditions_dict[src_col_name])
                self.query_queue.append_node(operator)
            else:
                for col, conditions in conditions_dict.items():
                    for cond in conditions:
                        operator = FilterOperator(self.name, col, cond)
                        self.query_queue.append_node(operator)
        return self

    def select_cols(self, col_list):
        """
        Select the columns of interest from the returned dataset when executing the SPARQL query
        :param col_list: list of column names to return
        :return: the same dataset
        """
        all_cols = [col for col in set(self.columns + self.parent_dataset.columns)]
        invalid_cols = [col for col in col_list if col not in all_cols]

        if len(invalid_cols) > 0:
            raise Exception('Columns {} are not defined'.format(invalid_cols))
        select_node = SelectOperator(self.name, col_list)
        self.query_queue.append_node(select_node)

        return self

    def group_by(self, groupby_cols_list):
        """
        Group the table by the column names passed in groupby_cols_list
        :param groupby_cols_list: list of column names to group the table by
        :return: GroupedDataset object derived from self dataset with groupby_cols_list as grouping columns
        """
        groupby_ds_name = GroupedDataset.generated_grouped_ds_name(self.name, groupby_cols_list)
        groupby_node = GroupByOperator(self.name, groupby_cols_list, groupby_ds_name)
        self.query_queue.append_node(groupby_node)

        return GroupedDataset(self, groupby_cols_list, groupby_node, groupby_ds_name)

    # aggregate functions

    def sum(self, aggregation_fn_data):
        """
        Runs sum aggregate function on the pass rdframe columns list and returns the summation of the passed columns as
        a list of scalar values
        :param aggregation_fn_data: list of AggregationData class holding the aggregation functions' information
        :return: the same dataset object
        """
        for agg_fn_data in aggregation_fn_data:
            agg_col = agg_fn_data.src_col_name
            if agg_col not in self.grouping_cols:
                tag = agg_fn_data.new_col_name
                param = agg_fn_data.agg_parameter
                agg_node = GroupedAggregationOperator(self.name, agg_col, AggregationFunction.SUM, tag, param)
                self.query_queue.append_node(agg_node)
                self.columns.append(tag)
                self.agg_columns.append(tag)
        return self

    def avg(self, aggregation_fn_data):
        """
        Runs average aggregate function on the pass rdframe columns list and returns the average of the passed columns as
        a list of scalar values
        If the col in the src_cols is a non-group_by col in the parent dataset the the returned dataset will have
        a new col and corresponding value in new_cols will be the new_col_name.
        If the new col in the src_cols_list is a group_by col in this dataset, return a dictionary where the
        key is the tag and the value is the count.
        :param aggregation_fn_data: list of AggregationData class holding the aggregation functions' information
        :return: the same dataset object
        """
        for agg_fn_data in aggregation_fn_data:
            agg_col = agg_fn_data.src_col_name
            if agg_col not in self.grouping_cols:
                tag = agg_fn_data.new_col_name
                param = agg_fn_data.agg_parameter
                agg_node = GroupedAggregationOperator(self.name, agg_col, AggregationFunction.AVG, tag, param)
                self.query_queue.append_node(agg_node)
                self.columns.append(tag)
                self.agg_columns.append(tag)
        return self

    def min(self, aggregation_fn_data):
        """
        Runs min aggregate function on the pass rdframe columns list and returns the min of the passed columns.
        if the col in the src_cols_list is a non-group_by col in the parent dataset the the returned dataset will have
        a new col and corresponding value in dst_list_tags will be the new_col_name.
        if the new col in the src_cols_list is a group_by col in this dataset, return a dictionary where the
        key is the tag and the value is the min.
        :param aggregation_fn_data: list of AggregationData class holding the aggregation functions' information
        :return: the same dataset object
        """
        for agg_fn_data in aggregation_fn_data:
            agg_col = agg_fn_data.src_col_name
            if agg_col not in self.grouping_cols:
                tag = agg_fn_data.new_col_name
                param = agg_fn_data.agg_parameter
                agg_node = GroupedAggregationOperator(self.name, agg_col, AggregationFunction.MIN, tag, param)
                self.query_queue.append_node(agg_node)
                self.columns.append(tag)
                self.agg_columns.append(tag)
        return self

    def max(self, aggregation_fn_data):
        """
        Runs max aggregate function on the pass rdframe columns list and returns the max of the passed columns.
        if the col in the src_cols_list is a non-group_by col in the parent dataset the the returned dataset will have
        a new col and corresponding value in dst_list_tags will be the new_col_name.
        if the new col in the src_cols_list is a group_by col in this dataset, return a dictionary where the
        key is the tag and the value is the sum.
        :param aggregation_fn_data: list of AggregationData class holding the aggregation functions' information
        :return: the same dataset object
        """
        for agg_fn_data in aggregation_fn_data:
            agg_col = agg_fn_data.src_col_name
            if agg_col not in self.grouping_cols:
                tag = agg_fn_data.new_col_name
                param = agg_fn_data.agg_parameter
                agg_node = GroupedAggregationOperator(self.name, agg_col, AggregationFunction.MAX, tag, param)
                self.query_queue.append_node(agg_node)
                self.columns.append(tag)
                self.agg_columns.append(tag)
        return self

    def count(self, aggregation_fn_data):
        """
        Runs count aggregate function on the pass rdframe columns list and returns the count of the passed columns.
        if the cols in the src_cols_list are non-group_by cols in the parent dataset the the returned datase will have
        new cols and corresponding values in dst_list_tags will be the new_col_name.
        if the new col in the src_cols_list is a group_by col in this dataset, return a dictionary where the
        key is the tag and the value is the count.
        :param aggregation_fn_data: list of AggregationData class holding the aggregation functions' information
        :return: dictionary of column count tags to the count value
        """
        for agg_fn_data in aggregation_fn_data:
            agg_col = agg_fn_data.src_col_name
            if agg_col not in self.grouping_cols:
                tag = agg_fn_data.new_col_name
                param = agg_fn_data.agg_parameter

                agg_node = GroupedAggregationOperator(self.name, agg_col, AggregationFunction.COUNT, tag, param)
                self.query_queue.append_node(agg_node)
                self.columns.append(tag)
                self.agg_columns.append(tag)
        return self

    def type(self):
        """
        return the type of the dataset as string
        :return: dataset type as string
        """
        return "GroupedDataset"

    @staticmethod
    def generated_grouped_ds_name(parent_ds_name, grouping_cols):
        return '{}.grouped.{}'.format(parent_ds_name, ','.join(sorted(grouping_cols)))
