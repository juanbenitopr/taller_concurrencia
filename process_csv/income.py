import dataclasses


@dataclasses.dataclass(frozen=True)
class Income:
    region: str
    year: int
    amount: float