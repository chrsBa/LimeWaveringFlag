from rdflib import URIRef

from graph_db import GraphDB
from src.query_transformer import QueryTransformer


class MessageHandler:

    def __init__(self):
        self.graphDB = GraphDB()
        self.query_transformer = QueryTransformer()

    def handle_message(self, message: str) -> str:
        """
        Orchestrate message processing and response generation.

        Args:
            message (str): The incoming message to be processed.

        Returns:
            str: The response after processing the message.
        """

        query_as_sparql = self.query_transformer.transform(message)
        print("Generated query: ", query_as_sparql)


        try:
            response = self.graphDB.execute_query(query_as_sparql)
        except Exception as e:
            response = "Please enter a valid SPARQL Query. Your query caused the following error: " + str(e)

        return response
