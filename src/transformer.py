import re

from langchain_ollama import ChatOllama

from src.vector_store.vector_store import VectorStore


class Transformer:
    def __init__(self, vector_store: VectorStore):
        self.transform_llm = ChatOllama(model="gemma3:4b", temperature=0.7)
        self.vector_store = vector_store


    def transform_query(self, text_query: str, retry=0) -> str:
        entity_search_query = text_query
        relation_search_query = text_query
        if retry == 0:
            _, entity_search_query = self.extract_named_entities(text_query)
            if entity_search_query is None:
                entity_search_query = text_query

        entity_search_query = self.clean_text_query(entity_search_query)

        node_result = self.vector_store.find_similar_entity(entity_search_query, 3)
        print(entity_search_query, node_result[0]['metadata'])
        node = node_result[0]['metadata']['entity']
        pred_result = self.vector_store.find_similar_relation(relation_search_query, 3)
        print(relation_search_query, pred_result[0]['metadata'])
        pred = pred_result[0]['metadata']['entity']

        return f"""SELECT (COALESCE(?objLabel, STR(?obj)) AS ?result) WHERE {{
                    <{node}> <{pred}> ?obj .
                    OPTIONAL {{
                        ?obj rdfs:label ?objLabel .
                    }}
                }}
                LIMIT 1
                """

    @staticmethod
    def clean_text_query(text_query: str) -> str:
        # Remove ?, quotes and 'movie' keywords to optimize similarity search
        return (text_query
                .replace('"', '')
                .replace("'", '')
                .replace('the movie ', '')
                .replace('the film ', '')
                .replace('?', '')
                .strip())


    @staticmethod
    def extract_named_entities(question: str) -> tuple[str, str] | tuple[None, None]:
        print(f"Extracting named entities from question: {question}")
        factual_question_patterns = [
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
            # {
            #     "pattern": "what ([^\\s]+) is (.*)",
            #     "entity_group_index": 2,
            #     "relation_group_index": 1,
            # },
            {
                "pattern": "what is the ([^\\s]+) of (.*)",
                "entity_group_index": 2,
                "relation_group_index": 1,
            },
            {
                "pattern": "when was (.*) ([^\\s]+)",
                "entity_group_index": 1,
                "relation_group_index": 2,
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
        ]

        for pattern in factual_question_patterns:
            match = re.match(pattern["pattern"], question.rstrip("?"), re.IGNORECASE)

            if match:
                relation = match.group(pattern["relation_group_index"]).strip()
                entity = match.group(pattern["entity_group_index"]).strip()
                return relation, entity

        # Check if something in quotes is present (probably an entity)
        quote_match = re.search(r'"(.*?)"|\'(.*?)\'', question)
        if quote_match:
            return None, quote_match.group(1) or quote_match.group(2)

        return None, None


    def transform_answer(self, question:str, factual_answer: str) -> str:
        print(f"Transforming answer: {factual_answer}")
        prompt = f"""
        Given the question: "{question}" and the factual answer: "{factual_answer}", generate a concise and informative answer.
        DO NOT ADD ANY ADDITIONAL INFORMATION THAT IS NOT PRESENT IN THE FACTUAL ANSWER. 
        ONLY TRANSFORM IT TO A NATURAL LANGUAGE ANSWER. 
        Use a helpful, engaging and conversational tone and ensure your answer sounds natural.
        Append '(Factual Answer)'"""
        response = self.transform_llm.invoke([{"role": "user", "content": prompt}])
        return response.content

if __name__ == "__main__":
    query_transformer = Transformer(vector_store=VectorStore())
    query = ('When was "The Godfather" released?')
    result = query_transformer.transform_query(query)
    print(result)
