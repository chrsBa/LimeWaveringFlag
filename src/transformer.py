import re
import string

from langchain_ollama import ChatOllama

from src.vector_store.vector_store import VectorStore


class Transformer:
    def __init__(self, vector_store: VectorStore):
        self.transform_llm = ChatOllama(model="gemma3:4b", temperature=0.7)
        self.vector_store = vector_store


    def get_query_for_entity_relation(self, entity: str, relation: str) -> str:
        return f"""SELECT (COALESCE(?objLabel, STR(?obj)) AS ?result) WHERE {{
                    <{entity}> <{relation}> ?obj .
                    OPTIONAL {{
                        ?obj rdfs:label ?objLabel .
                    }}
                }}
                """

    def extract_text_entities(self, text_query: str) ->tuple[str, str]:
        relation_search_query, entity_search_query = self.extract_named_entities(text_query)
        if entity_search_query is None:
            entity_search_query = text_query
        if relation_search_query is None:
            relation_search_query = text_query
 
        entity_search_query = self.clean_text_query(entity_search_query)

        node_result = self.vector_store.find_similar_entity(entity_search_query, 3)
        print(entity_search_query, node_result[0]['metadata'])
        node = node_result[0]['metadata']['entity']
        pred_result = self.vector_store.find_similar_relation(relation_search_query, 3)
        print(relation_search_query, pred_result[0]['metadata'])
        pred = pred_result[0]['metadata']['entity']

        return pred, node
    
    def extract_multiple_entities(self, text_query: str) -> dict[str, str]:
        entities_search_query = self.clean_text_query(text_query)
        entities = []
        match = re.search(r"(?:like|such as|including|for example)\s+(.*?)(?:\bcan you\b|\bgive\b|\brecommend\b|[?.!]|$)", entities_search_query, re.IGNORECASE)

        movies = match.group(1) if match else entities_search_query
        parts = movies.split(",")
        if len(parts) == 1:
            parts = movies.split(" and ")
        
        for part in parts:
            entities.append(self.vector_store.find_movie_with_label(part))

        entities = {entity[0]['metadata']['label']: entity[0]['metadata']['entity'] for entity in entities if entity}
        return entities


    @staticmethod
    def clean_text_query(text_query: str) -> str:
        # Remove ?, quotes and 'movie' keywords to optimize similarity search
        return (text_query
                .replace('"', '')
                .replace("'", '')
                .replace('the movie ', '')
                .replace('the film ', '')
                .replace('?', '')
                .replace("‘", '')
                .replace("’", '')
                .replace("“", '"')
                .replace("”", '"')
                .strip())


    @staticmethod
    def extract_named_entities(question: str) -> tuple[str, str] | tuple[None, None]:
        print(f"Extracting named entities from question: {question}")
        factual_question_patterns = [
            {
                "pattern": r"when (?:was|did)?\s+['\"]?(.*?)['\"]?(?:\s+.*)?$",
                "entity_group_index": 1,
                "relation_group_index": None,
                "default_relation": "release_date",
            },
            {
                "pattern": "who is the ([^\\s]+) of (.*)",
                "entity_group_index": 2,
                "relation_group_index": 1,
            },
            {
                "pattern": "who was the ([^\\s]+) of (.*)",
                "entity_group_index": 2,
                "relation_group_index": 1,
            },
            {
                "pattern": "who was the ([^\\s]+) for (.*)",
                "entity_group_index": 2,
                "relation_group_index": 1,
            },
            {
                "pattern": "who was the ([^\\s]+) in (.*)",
                "entity_group_index": 2,
                "relation_group_index": 1,
            },
            {
                "pattern": "who ([^\\s]+) (.*)",
                "entity_group_index": 2,
                "relation_group_index": 1,
            },
            {
                "pattern": "who wrote the ([^\\s]+) of (.*)",
                "entity_group_index": 2,
                "relation_group_index": 1,
            },
            {
                "pattern": "who wrote the ([^\\s]+) for (.*)",
                "entity_group_index": 2,
                "relation_group_index": 1,
            },
            {
                "pattern": "what ([^\\s]+) is (.*)",
                "entity_group_index": 2,
                "relation_group_index": 1,
            },
            {
                "pattern": "what is the ([^\\s]+) of (.*)",
                "entity_group_index": 2,
                "relation_group_index": 1,
            },
            {
                "pattern": "where was (.*) ([^\\s]+)",
                "entity_group_index": 1,
                "relation_group_index": 2,
            },
            {
                "pattern": "where is ([^\\s]+) (.*)",
                "entity_group_index": 1,
                "relation_group_index": 2,
            },
            {
                "pattern": "from what ([^\\s]+) is (.*)",
                "entity_group_index": 2,
                "relation_group_index": 1,
            },
        ]

        for pattern in factual_question_patterns:
            match = re.search(pattern["pattern"], question.translate(str.maketrans('', '', string.punctuation)), re.IGNORECASE)

            if match:
                entity = match.group(pattern["entity_group_index"]).strip()

                if "default_relation" in pattern:
                    relation = pattern["default_relation"]
                else:
                    relation = match.group(pattern["relation_group_index"]).strip()

                print("relation: " + relation)
                print("entity: " + entity)
                return relation, entity

        # Check if something in quotes is present (probably an entity)
        quote_match = re.search(r'"(.*?)"|\'(.*?)\'', question)
        if quote_match:
            return None, quote_match.group(1) or quote_match.group(2)

        return None, None


    def transform_answer(self, question:str, factual_answer: str, source: str, type="") -> str:
        print(f"Transforming answer: {factual_answer}")
        prompt = f"""
        Given the question: "{question}" and the factual answer: "{factual_answer}", generate a concise and informative answer.
        DO NOT ADD ANY ADDITIONAL INFORMATION THAT IS NOT PRESENT IN THE FACTUAL ANSWER. 
        ONLY TRANSFORM IT TO A NATURAL LANGUAGE ANSWER. 
        Use a helpful, engaging and conversational tone and ensure your answer sounds natural.
        Append '({source} Answer)'{ 'and (type: '+ type if type else ""})'"""
        response = self.transform_llm.invoke([{"role": "user", "content": prompt}])
        return response.content

if __name__ == "__main__":
    query_transformer = Transformer(vector_store=VectorStore())
    query = ('When was "The Godfather" released?')
    result = query_transformer.extract_text_entities(query)
    print(result)
