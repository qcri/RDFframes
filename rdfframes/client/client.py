from rdfframes.utils.constants import _TIMEOUT, ReturnFormat, _MAX_ROWS
from rdfframes.utils.helper_functions import is_uri


__author__ = "Aisha Mohamed <ahmohamed@qf.org.qa>"


class Client:
    """
    Abstract class for the SPARQL client that handles communication with a sparql end-point or local RDF engine
    """
    def __init__(self, endpoint):
        """
        Constructs an instance of the client class
        :param endpoint: string of the SPARQL endpoint's URI hostname:port
        :type endpoint: string
        """
        if not is_uri(endpoint):
            raise Exception("endpoint is not a valid URI")
        self.endpoint_url = None
        self.set_endpoint(endpoint)

    def is_alive(self, endpoint=None):
        """
        :param endpoint string of the SPARQL endpoint's URI
        :type endpoint string
        :return if endpoint is not None return True if endpoint is alive else
            return False. if endpoint is None return True if self.endpoint is 
            alive and False otherwise.
        """
        pass

    def get_endpoint(self):
        """
        :return a string of the endpont URI
        """
        return self.endpoint_url

    def set_endpoint(self, endpoint):
        """
        updates self.endpoint with the new endpoint
        :param endpoint: endpoint uri
        """
        pass

    def execute_query(self, query, timeout=_TIMEOUT, limit=_MAX_ROWS, return_format=None, output_file=None):
        """
        Connects to a sparql endpoint
        :param query:
        :param timeout:
        :param output_file:
        :return:
        """
        pass
