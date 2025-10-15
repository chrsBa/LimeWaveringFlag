from graph_db import GraphDB


class MessageHandler:

    def __init__(self):
        self.graph = GraphDB()

    def handle_message(self, message: str) -> str:
        """
        Orchestrate message processing and response generation.

        Args:
            message (str): The incoming message to be processed.

        Returns:
            str: The response after processing the message.
        """
        try:
            response = self.graph.execute_query(message)
        except Exception as e:
            response = "Please enter a valid SPARQL Query. Your query caused the following error: " + str(e)

        return response
