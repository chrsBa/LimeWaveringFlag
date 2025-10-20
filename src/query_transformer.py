import httpx
from langchain_ollama import ChatOllama


class QueryTransformer:
    def __init__(self):
        self.transform_llm = ChatOllama(model="qwen3:32b")

    def transform(self, text_query: str) -> str:
        messages=[
            {"role": "system",
             "content": "You are an assistant that converts natural language questions into SPARQL queries."
                        "Start each query with"
                        "'''"
                        "PREFIX ddis: <http://ddis.ch/atai/> "
                        "PREFIX wd: <http://www.wikidata.org/entity/> "
                        "PREFIX wdt: <http://www.wikidata.org/prop/direct/> "
                        "'''"
                        "Always return the label of the entity using rdfs:label."
                        "Do not add any comments or explanations!"
                        "Do not add any quotes around the query!"
                        "Add new lines and indentation!"
                        "Use the tool 'search_wikidata' to find relevant entities and relations in Wikidata."
                        "DO NOT ADD entities or relations that you have not found using the tool!"
                        "DO NOT ADD '''SERVICE''' !"
                        "IMPORTANT: Return only valid SPARQL!"
             },
            {"role": "user", "content": text_query}
        ]
        print("Generating...")
        response = self.transform_llm.invoke(messages, tools=[{"name": "search_wikidata", "description": "Searches Wikidata for a given term and returns the results as JSON.", "parameters": {"search_term": "string"}}])
        return response.content.strip()

    def serach_wikidata(self, search_term: str) -> str:
        base_url = "https://www.wikidata.org/w/api.php"
        payload = {
            "action": "query",
            "list": "search",
            "srsearch": search_term,
            "format": "json",
            "origin": "*",
        }
        res = httpx.get(base_url, params=payload)
        print(res.json())
        return res.json()


if __name__ == "__main__":
    transformer = QueryTransformer()
    query = "Which movie, originally from the country 'South Korea', received the award 'Academy Award for Best Picture'?"
    sparql_query = transformer.transform(query)
    print(sparql_query)
