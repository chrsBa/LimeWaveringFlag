from .embedding_search import EmbeddingSearch
from .graph_db import GraphDB
from .transformer import Transformer
from .vector_store.vector_store import VectorStore


class MessageHandler:

    def __init__(self):
        self.graphDB = GraphDB()
        self.vector_store = VectorStore()
        self.transformer = Transformer(self.vector_store)
        self.embedding_search = EmbeddingSearch(self.vector_store)

    def handle_message(self, message: str) -> tuple[str, str]:
        """
        Orchestrate message processing and response generation.

        Args:
            message (str): The incoming message to be processed.

        Returns:
            str: The response after processing the message.
        """
        extracted_relation, extracted_entity = self.transformer.extract_text_entities(message)

        print("extracted entity " + extracted_entity)
        print("extracted relation " + extracted_relation)
        query_as_sparql = self.transformer.get_query_for_entity_relation(extracted_entity, extracted_relation)
        print("Generated query: ", query_as_sparql)
        graph_response = self.graphDB.execute_query(query_as_sparql)

        embedding_response = self.embedding_search.nearest_neighbor(extracted_entity, extracted_relation)
        formatted_embedding_response = self.transformer.transform_answer(message, embedding_response, 'Embeddings')
        if graph_response.strip() != "":
            return (self.transformer.transform_answer(message, graph_response, 'Factual'),
                    formatted_embedding_response)
        return graph_response, formatted_embedding_response
