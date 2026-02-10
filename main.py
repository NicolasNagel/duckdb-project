from src.data_source.csv_data_source import CSVDataSource
from src.controllers.controller import DataIngestor

data_source = CSVDataSource().start()
controller = DataIngestor().start()