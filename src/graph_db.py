import os
from rdflib import Graph, URIRef
from rdflib.plugins.sparql.processor import prepareQuery


class GraphDB:
    def __init__(self):
        self.graph = Graph()
        base_dir = os.path.dirname(__file__)
        graph_path = os.path.join(base_dir, "../data/graph.nt")
        print('Loading Graph...')
        self.graph.parse(graph_path, format="nt")
        print('Successfully loaded Graph.')

    def execute_query(self, query: str) -> str:
        """
        Process the sparql queries

        Args:
            query (str): The incoming sparql query to be processed.

        Returns:
            str: The DB response after processing the query.
        """

        prepared_query = prepareQuery(query)
        answer = ""

        for row in self.graph.query(prepared_query):
            answer += str(row[0]) + "\n"

        return answer

    def extract_relations(self):
        query = """
                    PREFIX ddis: <http://ddis.ch/atai/>
                    PREFIX wd: <http://www.wikidata.org/entity/>
                    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
                    PREFIX schema: <http://schema.org/> 

                    SELECT ?label WHERE {{
                        {} rdfs:label ?label .
                        FILTER(LANG(?label) = "en").
                    }}
                    LIMIT 1
                    """

        graph_entities = set(self.graph.subjects(unique=True)) | set(self.graph.objects(unique=True)) | set(self.graph.predicates(unique=True))

        for node in graph_entities:
            link = node.n3()
            if isinstance(node, URIRef):
                result = self.graph.query(query.format(link))
                for row in result:
                    print(link.split('/')[-1], row.label)


if __name__ == "__main__":
    graph_db = GraphDB()
    graph_db.extract_relations()