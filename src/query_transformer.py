import httpx
from langchain.agents import create_agent
from langchain.tools import tool
from langchain_ollama import ChatOllama

@tool
def search_wikidata(search_term: str) -> str:
    """Search Wikidata for entities and relations that match a search term."""
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
    return {
                "title": res.json()['query']['search'][0]['title'],
                "description": res.json()['query']['search'][0]['snippet']
            } if 'search' in res.json()['query'] else "No results found"


class QueryTransformer:
    def __init__(self):
        self.transform_llm = ChatOllama(model="qwen3:32b")

    def transform(self, text_query: str) -> str:
        agent = create_agent(
                                    self.transform_llm,
                                    tools=[search_wikidata],
                                )
        messages=[
            {"role": "system",
             "content": "You are an assistant that converts natural language questions into SPARQL queries."
                        "Start each query with"
                        "'''"
                        "PREFIX ddis: <http://ddis.ch/atai/> "
                        "PREFIX wd: <http://www.wikidata.org/entity/> "
                        "PREFIX wdt: <http://www.wikidata.org/prop/direct/> "
                        "'''"
                        "Add new lines and indentation!"
                        "Use the tool 'search_wikidata' to find relevant entities and relations in Wikidata!"
                        "DO NOT ADD entities or relations that you have not found using the tool!"
                        "Always return the label of the entity using rdfs:label."
             },
            {"role": "user", "content": text_query}
        ]
        print("Generating...")

        response = agent.invoke({"messages": messages})['messages'][-1].content
        # Remove service terms
        formated_response = str(response)
        service_statement_index = formated_response.find("SERVICE")
        if service_statement_index != -1:
            print("Removed SERVICE part from response.")
            formated_response = response[:service_statement_index] + "}"

        formated_response = formated_response.replace('```sparql', '').replace('```', '').strip()

        return formated_response


if __name__ == "__main__":
    transformer = QueryTransformer()
    query = "Which movie, originally from the country 'South Korea', received the award 'Academy Award for Best Picture'?"
    sparql_query = transformer.transform(query)
    print(sparql_query)
