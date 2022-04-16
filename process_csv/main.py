from datetime import datetime

from process_csv.services.csv_service import CSVService
from process_csv.services.income_processor_service import IncomeProcessorService

if __name__ == '__main__':
    start_time = datetime.now()

    csv_service = CSVService()
    incomes = csv_service.read_csv('data.csv')

    income_processor = IncomeProcessorService()
    income_per_region = income_processor.average_per_region(incomes)

    end_time = datetime.now()

    print(income_per_region)
    print(f'Duration: {end_time - start_time}')
