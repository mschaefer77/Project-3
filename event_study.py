from project_classes import DataCatalog, DataLake, DataWorkbench, BaseDataModel, NewsDataSentiment, StockSentimentAnalyzer
import pandas as pd
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

data_lake = DataLake('files')
data_catalog = DataCatalog('files/catalog_data.json')
data_workbench = DataWorkbench(data_lake, data_catalog)

#Ingests historical intraday data for a given symbol, over a speicified time period, with a specified interval
data_lake.ingest_historical_data(symbol='TSLA',start_date='2024-11-01',end_date= '2024-11-13', interval='5m', data_catalog=data_catalog)
data_lake.fetch_news_data(company_name='Tesla', api_key='8577506bbc384cb09e4850b4de949783', from_date='2024-11-01', to_date='2024-11-13', data_catalog=data_catalog)

search_terms_intraday = {
    'symbol': 'TSLA',
    'interval': '5m'
}

tesla_intraday = data_lake.retrieve_raw_data(data_catalog.search_datasets(search_terms=search_terms_intraday)[0]['file_path'])
tesla_close= data_workbench.filter_data(tesla_intraday, columns=['datetime','close'])

search_terms_news = {
    "company": "TESLA",
    "data_type" : "news"
}

tesla_news_data= data_lake.retrieve_raw_data(data_catalog.search_datasets(search_terms=search_terms_news)[0]['file_path'])
tesla_scores = NewsDataSentiment(data=tesla_news_data)
scores = tesla_scores.analyze_sentiment()

analyzer = StockSentimentAnalyzer(stock_data=tesla_close, news_data=scores)
merged_data = analyzer.merge_news_and_stock_data()
merged_data_with_signals = analyzer.generate_signals(merged_data)
analyzer.plot_signals(merged_data_with_signals)
