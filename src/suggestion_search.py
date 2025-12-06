import datetime
import os

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
                                    "after_a_work_by",
                                    "director",
                                    "narrative_location",
                                    "fsk_rating",
                                    "composer",
                                    "producer",
                                    "screenwriter",
                                    "director_of_photography",
                                    "film_editor",
                                    "nominated_for",
                                    "sound_designer",
                                    "movement"
                                    ]
        # Load general properties from csv into a list
        self.general_properties = []
        self.load_general_properties()

    def load_general_properties(self):
        base_path = os.path.dirname(os.path.dirname(__file__))
        general_properties_file = os.path.join(base_path, "data", "movie_general_property_keywords.csv")
        try:
            with open(general_properties_file, 'r', encoding='utf-8') as f:
                for line in f:
                    property_name = line.strip()
                    self.general_properties.append(property_name)
        except Exception as e:
            print(f"Error loading general properties from {general_properties_file}: {e}")



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

    @staticmethod
    def _get_relevant_property_values(entity_properties: dict, property_name: str) -> tuple[list[str], int]:
        # write the property to the string that occurs most frequently among the entities
        prop_values = []
        for uri in entity_properties.keys():
            value = entity_properties[uri].get(property_name)
            if value:
                prop_values.append(value)

        # Check if a value in the prop_values list occurs more than once in the entity_properties
        prop_frequency = {}
        for uri, props in entity_properties.items():
            if props.get(property_name):
                for val in props.get(property_name):
                    prop_frequency[val] = prop_frequency.get(val, 0) + 1

        # Find the most property values tha occur multiple times
        # If more than 5 values occur multiple times, take the top 5, but only those that occur more than once
        most_common_vals = sorted(
            [val for val, freq in prop_frequency.items() if (freq > 1 or len(entity_properties.keys()) == 1)],
            key=lambda x: prop_frequency[x],
            reverse=True
        )
        if len(most_common_vals) > 3:
            most_common_vals = most_common_vals[:3]

        # Return the most common values and the highest frequency
        score = max([prop_frequency[val] for val in most_common_vals], default=0)
        return most_common_vals, score


    def find_suggestions(self, extracted_entities_map: dict[str, str], text_query) -> list[str]:
        entity_uris = list(extracted_entities_map.values())
        entity_labels = list(extracted_entities_map.keys())
        try:
            # find movies similar to the given entities
            entity_properties = {
                uri: self.graph_db.get_movie_properties(URIRef(uri))
                for uri in entity_uris
            }
            # remove trivial properties
            for uri in entity_uris:
                if 'instance_of' in entity_properties[uri] and 'film' in entity_properties[uri]['instance_of']:
                    entity_properties[uri]['instance_of'].remove('film')

            most_common_properties = {}
            print(entity_properties.items())
            for prop in self.relevant_properties:
                most_common_vals, score = self._get_relevant_property_values(entity_properties, prop)
                if most_common_vals:
                        most_common_properties[', '.join(most_common_vals)] = score

            # If all publication dates are within 10 years, take the average publication date

            if self._within_years(entity_properties, 10) and len(entity_properties.items()) > 1:
                avg_date_str = self._average_publication_date_str(entity_properties, entity_uris)
                if avg_date_str:
                    most_common_properties[str(avg_date_str)] = len(entity_properties.keys())

            # Sort the most common properties by their score and create a string of the top 5 properties
            sorted_common_properties = sorted(most_common_properties.items(), key=lambda x: x[1], reverse=True)
            movie_properties_str = ''
            for i, (prop, score) in enumerate(sorted_common_properties):
                if i >= 5:
                    break
                if len(movie_properties_str) > 0:
                    movie_properties_str += ', '
                movie_properties_str += prop
            for gen_prop in self.general_properties:
                if gen_prop.lower() in text_query.lower():
                    prop_to_append = gen_prop.strip()
                    if prop_to_append + ' film' in self.general_properties:
                        prop_to_append += ' film'
                    elif prop_to_append + " movies" in self.general_properties:
                        prop_to_append += ' movies'

                    if len(movie_properties_str) > 0:
                        movie_properties_str += ', '
                    movie_properties_str += prop_to_append

            print("Suggestion search properties: " + movie_properties_str)
            similar_movies = self.vector_store.find_similar_movies(movie_properties_str, entity_labels)
            return [movie['metadata']['label'] for movie in similar_movies]
        
        except Exception as e:
            print("Error in suggestion search: ", e)
            return []