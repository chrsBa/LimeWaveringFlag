from http.client import responses

from graph_db import GraphDB
from src.transformer import Transformer
from src.vector_store.vector_store import VectorStore


class MessageHandler:

    def __init__(self):
        self.graphDB = GraphDB()
        self.vector_store = VectorStore()
        self.transformer = Transformer(self.vector_store)
        self.retries = 3

    def handle_message(self, message: str) -> str:
        """
        Orchestrate message processing and response generation.

        Args:
            message (str): The incoming message to be processed.

        Returns:
            str: The response after processing the message.
        """
        retried = 0

        try:
            response = ""
            while retried < self.retries:
                print(message)
                query_as_sparql = self.transformer.transform_query(
                    message, retried)
                print("Generated query: ", query_as_sparql)
                response = self.graphDB.execute_query(query_as_sparql)
                if response.strip() == "":
                    retried += 1
                else:
                    break
        except Exception as e:
            response = "Please enter a valid SPARQL Query. Your query caused the following error: " + str(e)

        if response.strip() != "":
            return self.transformer.transform_answer(message, response)
        return response
