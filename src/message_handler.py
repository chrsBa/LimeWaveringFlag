from rdflib.compare import graph_diff

from embedding_search import EmbeddingSearch
from graph_db import GraphDB
from transformer import Transformer
from vector_store.vector_store import VectorStore


class MessageHandler:

    def __init__(self):
        self.graphDB = GraphDB()
        self.vector_store = VectorStore()
        self.transformer = Transformer(self.vector_store)
        self.embedding_search = EmbeddingSearch(self.vector_store)
        self.retries = 2

    def handle_message(self, message: str) -> tuple[str, str]:
        """
        Orchestrate message processing and response generation.

        Args:
            message (str): The incoming message to be processed.

        Returns:
            str: The response after processing the message.
        """
        retried = 0

        try:
            graph_response = ""
            while retried < self.retries:
                print(message)
                extracted_entity, extracted_relation = self.transformer.extract_text_entities(message, retried)
                query_as_sparql = self.transformer.get_query_for_entity_relation(extracted_entity, extracted_relation)
                print("Generated query: ", query_as_sparql)
                graph_response = self.graphDB.execute_query(query_as_sparql)
                if graph_response.strip() == "":
                    retried += 1
                else:
                    break

        except Exception as e:
            graph_response = "Please enter a valid SPARQL Query. Your query caused the following error: " + str(e)

        embedding_response = self.embedding_search.nearest_neighbor(extracted_entity, extracted_relation)
        formatted_embedding_response = self.transformer.transform_answer(message, embedding_response, 'Embeddings')
        if graph_response.strip() != "":
            return (self.transformer.transform_answer(message, graph_response, 'Factual'),
                    formatted_embedding_response)
        return graph_response, formatted_embedding_response
