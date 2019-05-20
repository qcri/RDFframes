__author__ = "Abdurrahman Ghanem <abghanem@hbku.edu.qa>"


_TIMEOUT = 1000  # timeout in seconds for one query
_MAX_ROWS = 1000000  # maximum number of rows returned in the result set


class JoinType:
    InnerJoin = 'InnerJoin'
    LeftOuterJoin = 'LeftOuterJoin'
    RightOuterJoin = 'RightOuterJoin'
    OuterJoin = "OuterJoin"


class SortingOrder:
    ASC = 'ASC'
    DESC = 'DESC'


class AggregationFunction:
    COUNT = 'COUNT'
    SUM = 'SUM'
    AVG = 'AVG'
    MIN = 'MIN'
    MAX = 'MAX'


class ReturnFormat:
    DataFrame = 'df'
    CSV = 'csv'


def is_subset(l1, l2):
    """
    returns true of l1 is a subset of l2 and 0 otherwise
    """
    if len(l2) < len(l1):
        return False
    for c in l1:
        if c not in l2:
            return False
    return True
