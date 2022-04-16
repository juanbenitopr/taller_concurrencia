import dataclasses


@dataclasses.dataclass
class Pokemon:
    id: int
    name: str
    height: int
    weight: int
