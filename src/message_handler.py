from .embedding_search import EmbeddingSearch
from .graph_db import GraphDB
from .transformer import Transformer
from .vector_store.vector_store import VectorStore
from .suggestion_search import SuggestionSearch


class MessageHandler:

    def __init__(self, vector_store: VectorStore):
        self.graphDB = GraphDB()
        self.vector_store = vector_store
        self.transformer = Transformer(self.vector_store)
        self.embedding_search = EmbeddingSearch(self.vector_store)
        self.suggestion_search = SuggestionSearch(self.vector_store, self.graphDB)

    def handle_factual_question(self, message: str, extracted_entity: str, extracted_relation: str) -> str:
        query_as_sparql = self.transformer.get_query_for_entity_relation(extracted_entity, extracted_relation)
        graph_response =  self.graphDB.execute_query(query_as_sparql)
        if graph_response.strip() == "":
            return "I could not find a factual answer to your question. Please try rephrasing it or ask something else."
        return self.transformer.transform_answer(message, graph_response, 'Factual')

    def handle_embedding_question(self, message: str, extracted_entity: str, extracted_relation: str) -> str:
        embedding_response, response_type = self.embedding_search.nearest_neighbor(extracted_entity, extracted_relation)
        if embedding_response.strip() == "":
            return "I could not find an answer based on embeddings to your question. Please try rephrasing it or ask something else."
        return self.transformer.transform_answer(message, embedding_response, 'Embeddings',
                                                                         response_type)

    def handle_suggestion_question(self, message: str, extracted_entities: list[str]) -> str:
        suggestion_response = self.suggestion_search.find_suggestions(extracted_entities)
        if len(suggestion_response) == 0:
            return "I could not find any suggestions based on your input. Please try rephrasing it or provide different entities."
        return self.transformer.transform_answer(message, ', '.join(suggestion_response), 'Suggestion')