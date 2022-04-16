import time
from collections import defaultdict
from typing import Dict, List

from process_csv.data_income import DataIncome
from process_csv.income import Income


class IncomeProcessorService:

    def average_per_region(self, incomes: List[Income]) -> Dict[str, DataIncome]:
        region_incomes = defaultdict(list)
        for income in incomes:
            region_incomes[income.region].append(income)

        region_amount = {}
        for region, incomes in region_incomes.items():
            region_amount[region] = self._calculate_average([income.amount for income in incomes])
            time.sleep(0.8)

        return region_amount

    def _calculate_average(self, amounts: List[float]) -> DataIncome:
        return DataIncome(
            average=sum(amounts) / len(amounts),
            min=min(amounts),
            max=max(amounts)
        )
