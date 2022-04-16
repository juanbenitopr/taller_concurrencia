from typing import List

import requests
from requests import RequestException

from pokeapi.pokeapi_service.pokeapi_rest_service import PokeAPIRestService
from pokeapi.pokemon import Pokemon


class PokeAPISecuentialRestService(PokeAPIRestService):

    def get_by_names(self, names: List[str]) -> List[Pokemon]:
        pokemons = []
        for name in names:
            try:
                response = requests.get(self.BASE_API_URL + name)
                response.raise_for_status()
                pokemon_json = response.json()
            except RequestException:
                continue

            pokemons.append(Pokemon(id=pokemon_json['id'], name=pokemon_json['name'], height=pokemon_json['height'],
                                    weight=pokemon_json['weight']))

        return pokemons
