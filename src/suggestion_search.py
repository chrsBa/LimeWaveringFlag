import datetime

import numpy as np
from rdflib import URIRef


class SuggestionSearch:
    def __init__(self, vector_store, graph_db):
        self.graph_db = graph_db
        self.vector_store = vector_store
        self.relevant_properties = ["instance_of","director","screenwriter","genre","award_received"]



    def _average_publication_date_str(self, entity_properties: dict, entity_uris: list[str]) -> str | None:
        """Parse publication_date for each entity and return an averaged date as a string.

        Rules:
        - Accept ISO dates like '1965-01-01' using '%Y-%m-%d'.
        - Fallback to date.fromisoformat.
        - Fallback to year-only strings like '1965' (convert to Jan 1).
        - Skip None/empty/invalid values.
        - If no valid dates found, return None.
        - If any input had a full date (year-month-day), return the average formatted as 'YYYY-MM-DD'.
          Otherwise return just the year 'YYYY'.
        """
        publication_dates = []
        full_date_present = False
        for uri in entity_uris:
            pub_str = entity_properties[uri].get("publication_date")
            if not pub_str:
                continue
            try:
                # Prefer explicit full-date parsing
                parsed = datetime.datetime.strptime(pub_str, "%Y-%m-%d").date()
                full_date_present = True
            except Exception:
                try:
                    parsed = datetime.date.fromisoformat(pub_str)
                    # If the original string contains a '-', treat it as a full date format
                    if '-' in pub_str:
                        full_date_present = True
                except Exception:
                    try:
                        year = int(pub_str)
                        parsed = datetime.date(year, 1, 1)
                    except Exception:
                        continue
            publication_dates.append(parsed)

        if not publication_dates:
            return None

        average_publication_date = np.mean([d.toordinal() for d in publication_dates])
        avg_date = datetime.date.fromordinal(int(round(average_publication_date)))

        if full_date_present:
            return avg_date.strftime("%Y-%m-%d")
        return str(avg_date.year)

    @staticmethod
    def _within_years(entity_properties: dict, year_range: int) -> bool:
        publication_years = []
        for uri in entity_properties.keys():
            pub_str = entity_properties[uri].get("publication_date")
            if pub_str:
                try:
                    year = datetime.datetime.strptime(pub_str, "%Y-%m-%d").date().year
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
        if len(most_common_vals) > 5:
            most_common_vals = most_common_vals[:5]

        return most_common_vals


    def find_suggestions(self, entity_uris: list[str]) -> list[str]:
        try:
            # find movies similar to the given entities
            entity_properties = {
                uri: self.graph_db.get_movie_properties(URIRef(uri))
                for uri in entity_uris
            }

            most_common_properties = []
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
            similar_movies = self.vector_store.find_similar_movies(movie_properties_str)
            return [movie['metadata']['label'] for movie in similar_movies]
        
        except Exception as e:
            print("Error in suggestion search: ", e)
            return []