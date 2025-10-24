
from graph_db import GraphDB
from src.query_transformer import QueryTransformer
from src.vector_store.vector_store import VectorStore


class MessageHandler:

    def __init__(self):
        self.graphDB = GraphDB()
        self.vector_store = VectorStore()
        self.query_transformer = QueryTransformer(self.vector_store)

    def handle_message(self, message: str) -> str:
        """
        Orchestrate message processing and response generation.

        Args:
            message (str): The incoming message to be processed.

        Returns:
            str: The response after processing the message.
        """
        try:
            query_as_sparql = self.query_transformer.transform(message)
            print("Generated query: ", query_as_sparql)
        except Exception as e:
            return "I could not find any information about the topic you asked for."


        try:
            response = self.graphDB.execute_query(query_as_sparql)
        except Exception as e:
            response = "Please enter a valid SPARQL Query. Your query caused the following error: " + str(e)

        return response
