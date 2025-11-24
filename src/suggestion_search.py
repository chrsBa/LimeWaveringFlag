import datetime

import numpy as np
from rdflib import URIRef


class SuggestionSearch:
    def __init__(self, vector_store, graph_db):
        self.graph_db = graph_db
        self.vector_store = vector_store
        self.relevant_properties = ["instance_of",
                                    "genre",
                                    "award_received",
                                    "main_subject",
                                    "production_company",
                                    "after_a_work_by"
                                    ]



    def _average_publication_date_str(self, entity_properties: dict, entity_uris: list[str]) -> str | None:
        publication_dates = []
        for uri in entity_uris:
            try:
                pub_str = entity_properties[uri].get("publication_date")[0]
                if not pub_str:
                    continue
                parsed = int(pub_str.split('-')[0])
                publication_dates.append(parsed)
            except Exception:
                continue

        if not publication_dates:
            return None

        average_publication_year = int(np.mean(publication_dates))
        return str(average_publication_year)

    @staticmethod
    def _within_years(entity_properties: dict, year_range: int) -> bool:
        publication_years = []
        for uri in entity_properties.keys():
            try:
                pub_str = entity_properties[uri].get("publication_date")[0]
                if pub_str:
                        year = int(pub_str.split('-')[0])
                        publication_years.append(year)
            except Exception:
                continue
        return publication_years and (max(publication_years) - min(publication_years) <= year_range)

    def _get_relevant_property_values(self, entity_properties: dict, entity_uris: list[str], property_name: str) -> list[str]:
        # write the property to the string that occurs most frequently among the entities
        prop_values = []
        for uri in entity_uris:
            value = entity_properties[uri].get(property_name)
            if value:
                prop_values.append(value)

        # Check if a value in the prop_values list occurs more than once in the entity_properties
        prop_frequency = {}
        for item in prop_values:
            for uri, props in entity_properties.items():
                if props.get(property_name):
                    for val in props.get(property_name):
                        prop_frequency[val] = prop_frequency.get(val, 0) + 1

        # Find the most property values tha occur multiple times
        # If more than 5 values occur multiple times, take the top 5, but only those that occur more than once
        most_common_vals = sorted(
            [val for val, freq in prop_frequency.items() if freq > 1],
            key=lambda x: prop_frequency[x],
            reverse=True
        )
        if len(most_common_vals) > 3:
            most_common_vals = most_common_vals[:3]

        return most_common_vals


    def find_suggestions(self, extracted_entities_map: dict[str, str]) -> list[str]:
        entity_uris = list(extracted_entities_map.values())
        entity_labels = list(extracted_entities_map.keys())
        try:
            # find movies similar to the given entities
            entity_properties = {
                uri: self.graph_db.get_movie_properties(URIRef(uri))
                for uri in entity_uris
            }

            most_common_properties = []
            print(entity_properties.items())
            for prop in self.relevant_properties:
                most_common_vals = self._get_relevant_property_values(entity_properties, entity_uris, prop)
                if most_common_vals:
                        most_common_properties.append(', '.join(most_common_vals))

            # If all publication dates are within 10 years, take the average publication date

            if self._within_years(entity_properties, 10):
                avg_date_str = self._average_publication_date_str(entity_properties, entity_uris)
                if avg_date_str:
                    most_common_properties.append(avg_date_str)

            movie_properties_str = ', '.join(most_common_properties)
            print("Suggestion search properties: " + movie_properties_str)
            similar_movies = self.vector_store.find_similar_movies(movie_properties_str, entity_labels)
            return [movie['metadata']['label'] for movie in similar_movies]
        
        except Exception as e:
            print("Error in suggestion search: ", e)
            return []