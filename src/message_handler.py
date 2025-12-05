from .embedding_search import EmbeddingSearch
from .graph_db import GraphDB
from .transformer import Transformer
from .vector_store.vector_store import VectorStore
from .suggestion_search import SuggestionSearch
from .multimedia_search import MultimediaSearch


class MessageHandler:

    def __init__(self, vector_store: VectorStore):
        self.graph_db = GraphDB()
        self.vector_store = vector_store
        self.transformer = Transformer(self.vector_store)
        self.embedding_search = EmbeddingSearch(self.vector_store)
        self.suggestion_search = SuggestionSearch(self.vector_store, self.graph_db)
        self.multimedia_search = MultimediaSearch(self.graph_db)

    def handle_factual_question(self, message: str, extracted_entity: str, extracted_relation: str) -> str:
        query_as_sparql = self.transformer.get_query_for_entity_relation(extracted_entity, extracted_relation)
        graph_response =  self.graph_db.execute_query(query_as_sparql)
        if graph_response is None or graph_response.strip() == "":
            return "I could not find a factual answer to your question. Please try rephrasing it or ask something else."
        return self.transformer.transform_answer(message, graph_response)

    def handle_embedding_question(self, message: str, extracted_entity: str, extracted_relation: str) -> str:
        embedding_response, response_type = self.embedding_search.nearest_neighbor(extracted_entity, extracted_relation)
        if embedding_response is None or embedding_response.strip() == "":
            return "I could not find an answer based on embeddings to your question. Please try rephrasing it or ask something else."
        return self.transformer.transform_answer(message, embedding_response)

    def handle_suggestion_question(self, message: str, extracted_entities_map: dict[str, str]) -> str:
        suggestion_response = self.suggestion_search.find_suggestions(extracted_entities_map)
        if len(suggestion_response) == 0:
            return "I could not find any suggestions based on your input. Please try rephrasing it or provide different entities."
        return self.transformer.transform_answer(message, ', '.join(suggestion_response))
    

    def handle_multimedia_question(self, extracted_entity: str) -> str:
        multimedia_response = self.multimedia_search.find_picture_for_entity(extracted_entity)
        if multimedia_response is None:
            return "I could not find any pictures based on your input. Please try rephrasing it or ask for another picture."

        formatted_response = f'image:{multimedia_response.replace('.jpg', '')}'
        print(f"multimedia response: {formatted_response}")
        return formatted_response