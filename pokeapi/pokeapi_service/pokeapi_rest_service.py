import abc
from abc import ABC
from typing import List

import requests

from pokeapi.pokemon import Pokemon


class PokeAPIRestService(ABC):
    BASE_API_URL = 'https://pokeapi.co/api/v2/pokemon/'

    def get_list_names(self, limit: int = 50):
        response = requests.get(self.BASE_API_URL, params={'limit': limit})
        return [pokemon['name'] for pokemon in response.json()['results']]

    @abc.abstractmethod
    def get_by_names(self, names: List[str]) -> List[Pokemon]:
        pass
