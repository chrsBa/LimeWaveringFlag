from langchain_ollama import ChatOllama

class QueryTransformer:
    def __init__(self):
        self.transform_llm = ChatOllama(model="qwen3:30b")

    def transform(self, text_query: str) -> str:
        messages=[
            {"role": "system",
             "content": "You are an assistant that converts natural language questions into SPARQL queries."
                        "Start each query with"
                        "'''"
                        "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"
                        "PREFIX wd: <http://www.wikidata.org/entity/>"
                        "PREFIX wdt: <http://www.wikidata.org/prop/direct/>"
                        "PREFIX schema: <http://schema.org/>"
                        "'''"
                        "Do not add any comments or explanations!"
                        "Do not add any quotes around the query!"
                        "Add normal new lines and indentation for better readability!"
                        "DO NOT ADD A SERVICE STATEMENT!"
                        "IMPORTANT: Return only valid SPARQL!"
             },
            {"role": "user", "content": text_query}
        ]

        print("Generating...")
        response = self.transform_llm.invoke(messages)
        print(response)
        return response.content.strip()
