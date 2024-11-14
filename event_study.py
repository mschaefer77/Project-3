from project_classes import DataCatalog, DataLake, DataWorkbench, BaseDataModel

data_lake = DataLake('files')
data_catalog = DataCatalog('files/catalog_data.json')
data_workbench = DataWorkbench(data_lake, data_catalog)

#Ingests historical intraday data for a given symbol, over a speicified time period, with a specified interval
data_lake.ingest_historical_data(symbol='TSLA',start_date='2024-10-01',end_date= '2024-11-13', interval='5m', data_catalog=data_catalog)
data_lake.fetch_news_data(company_name='Tesla', api_key='8577506bbc384cb09e4850b4de949783', from_date='2024-11-01', to_date='2024-11-13')

search_terms = {
    'symbol': 'TSLA',
    'interval': '5m'
}

data = data_catalog.search_datasets(search_terms=search_terms)

data_workbench.aggregate_data(data, )