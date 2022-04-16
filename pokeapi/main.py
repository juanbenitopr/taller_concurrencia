# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
from dataclasses import asdict
from datetime import datetime

from pokeapi.pokeapi_service.pokeapi_rest_service import PokeAPIRestService
from pokeapi.pokeapi_service.pokeapi_secuential_rest_service import PokeAPISecuentialRestService

if __name__ == '__main__':
    start_time = datetime.now()

    pokeapi_service: PokeAPIRestService = PokeAPISecuentialRestService()
    pokemon_names = pokeapi_service.get_list_names(limit=300)
    pokemons = pokeapi_service.get_by_names(pokemon_names)

    end_time = datetime.now()

    print(str([asdict(pokemon) if pokemon else None for pokemon in pokemons]))
    print(f'Duration: {end_time - start_time}')
