import sys
import io

from SPARQLWrapper import SPARQLWrapper, CSV, JSON, TSV
import pandas as pd

from rdfframes.utils.constants import _TIMEOUT, ReturnFormat, _MAX_ROWS
from rdfframes.client.client import Client

__author__ = "Aisha Mohamed <ahmohamed@qf.org.qa>"



class SPARQLEndpointClient(Client):
    """
    class for sparql client that handles communication with a sparql end-point
    over http using the sparql wrapper library.
    """
    def __init__(self, endpoint):
        """
        Constructs an instance of the client class
        :param endpoint: string of the SPARQL endpoint's URI hostname:port
        :type endpoint: string
        """
        super(SPARQLEndpointClient, self).__init__(endpoint=endpoint)
        self.endpoint = endpoint

    def get_endpoint(self):
        """
        :return a string of the endpont URI
        """
        return self.endpoint

    def set_endpoint(self, endpoint):
        """
        updates self.endpoint with the new endpoint
        :param endpoint: endpoint uri
        """
        self.endpoint = endpoint

    def execute_query(self, query, timeout=_TIMEOUT, limit=_MAX_ROWS, return_format=None, output_file=None):
        """
        Connects to a sparql endpoint
        :param query:
        :param timeout:
        :param output_file:
        :return:
        """
        client = SPARQLWrapper(self.endpoint)
        client.setTimeout(_TIMEOUT)
        offset = 0
        results_string = []  # where all the results are concatenated
        continue_streaming = True
        while continue_streaming:
            if limit > 1:  # This query doesn't return one constant value
                query_string = query + " OFFSET {} LIMIT {}".format(str(offset), str(limit))
            else:
                query_string = query
            query_string = query_string.encode()
            client.setQuery(query_string)
            try:
                client.setReturnFormat(CSV)
                result = client.query().convert().decode("UTF-8").split("\n", 1)
                if len(results_string) == 0:  # Add the returned table header
                    header = result[0]
                    results_string.append(header + "\n")
                # if the number of rows is less then the maximum number of rows
                if result[1].count('\n') < _MAX_ROWS:
                    continue_streaming = False
                offset = offset + limit
            except Exception as e:
                print(e)
                sys.exit()
            results_string.append(result[1])
        # convert it to a dataframe
        results_string = ''.join(results_string)
        f = io.StringIO(results_string)
        f.seek(0)
        df = pd.read_csv(f, sep=',') # to get the values and the header
        return df
