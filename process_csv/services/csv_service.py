from typing import List

from process_csv.income import Income


class CSVService:

    def read_csv(self, path: str) -> List[Income]:
        with open(path) as file:
            rows: List[List[str]] = [row.split(';') for row in file.read().split('\n')[1:]]
            incomes = [Income(region=row[0], year=int(row[1]), amount=float(row[2])) for row in rows]

        return incomes
