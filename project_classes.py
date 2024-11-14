import os
import json
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd
import yfinance as yf
import newsapi
import requests

# Class for data lake
class DataLake:
    def __init__(self, base_path: str):
        # Bath to where data will be stored
        self.base_path = base_path
        self.raw_data_path = os.path.join(self.base_path, 'raw')
        self.processed_data_path = os.path.join(self.base_path, 'processed')
        os.makedirs(self.raw_data_path, exist_ok=True)
        os.makedirs(self.processed_data_path, exist_ok=True)
    
    # Stores raw data 
    def store_raw_data(self, dataset_name: str, data: pd.DataFrame):
        file_path = os.path.join(self.raw_data_path, f"{dataset_name}.csv")
        data.to_csv(file_path, index=False)
        print(f"Raw data stored at {file_path}")
    
    # Stores process data
    def store_processed_data(self, dataset_name: str, data: pd.DataFrame):
        file_path = os.path.join(self.processed_data_path, f"{dataset_name}_processed.csv")
        data.to_csv(file_path, index=False)
        print(f"Processed data stored at {file_path}")
    
    # Retrieves raw data
    def retrieve_raw_data(self, dataset_name: str):
        file_path = os.path.join(self.raw_data_path, f"{dataset_name}.csv")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Raw data for {dataset_name} not found.")
        data = pd.read_csv(file_path)
        print(f"Raw data retrieved from {file_path}")
        return data
    
    # Retrieved processed data
    def retrieve_processed_data(self, dataset_name: str):
        file_path = os.path.join(self.processed_data_path, f"{dataset_name}_processed.csv")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Processed data for {dataset_name} not found.")
        data = pd.read_csv(file_path)
        print(f"Processed data retrieved from {file_path}")
        return data
    
    def get_file_path(self, dataset_name: str, data_format: str) -> str:
        if data_format == 'csv':
            directory = self.processed_data_path if 'processed' in dataset_name else self.raw_data_path
            file_path = os.path.join(directory, f"{dataset_name}.csv")
        elif data_format == 'json':
            directory = self.processed_data_path if 'processed' in dataset_name else self.raw_data_path
            file_path = os.path.join(directory, f"{dataset_name}.json")
        elif data_format == 'parquet':
            directory = self.processed_data_path if 'processed' in dataset_name else self.raw_data_path
            file_path = os.path.join(directory, f"{dataset_name}.parquet")
        else:
            raise ValueError(f"Unsupported data format: {data_format}")
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File '{file_path}' does not exist.")
        
        return file_path

    def ingest_historical_data(self, symbol: str, start_date: str, end_date: str, interval: str = '1d', data_catalog = None):
        """
        Ingests historical stock data for a given symbol from yfinance.

        Parameters:
            symbol (str): Stock ticker symbol (e.g., 'TSLA').
            start_date (str): Start date in 'YYYY-MM-DD' format.
            end_date (str): End date in 'YYYY-MM-DD' format.
            interval (str): Data interval (e.g., '1d' for daily, '1h' for hourly).
        """
        print(f"Starting ingestion for {symbol} from {start_date} to {end_date} with interval {interval}.")
        
        # Fetch data using yfinance
        ticker = yf.Ticker(symbol)
        data = ticker.history(start=start_date, end=end_date, interval=interval)
        
        if data.empty:
            print(f"No data fetched for {symbol} with the given parameters.")
            return
        
        # Reset index to make 'Date' a column
        data.reset_index(inplace=True)
        
        # Rename columns to lower case for consistency
        data.columns = [col.lower() for col in data.columns]
        
        # Store raw data
        dataset_name = f"historical_{symbol.lower()}"
        self.store_raw_data(dataset_name, data)

        if data_catalog:
            metadata = {
                'data_type': 'historical_intraday',
                'symbol': symbol.upper(),
                'source': 'yfinance',
                'format': 'csv',
                'start_date': start_date,
                'end_date': end_date,
                'interval': interval,
                'description': f"Historical intraday trading data for {symbol.upper()} from yfinance"
            }
            data_catalog.add_dataset(dataset_name, metadata)
        
        print(f"Ingestion completed for {symbol}.")

        # data_platform.py

    
    def fetch_news_data(self, company_name: str, api_key: str, from_date: str = None, to_date: str = None, language: str = 'en', data_catalog: 'DataCatalog' = None) -> pd.DataFrame:
        """
        Fetches news articles for a given company using NewsAPI.org and stores the data in the Data Lake.
    
        Parameters:
            company_name (str): The name of the company to fetch news for (e.g., 'Tesla').
            api_key (str): Your NewsAPI.org API key.
            from_date (str, optional): Start date for fetching news in 'YYYY-MM-DD' format.
            to_date (str, optional): End date for fetching news in 'YYYY-MM-DD' format.
            language (str, optional): Language of the news articles (default is 'en' for English).
            data_catalog (DataCatalog, optional): Instance of DataCatalog to register the dataset.
    
        Returns:
            pd.DataFrame: A DataFrame containing the fetched news articles.
        """
        print(f"Starting news data fetch for {company_name}.")
    
        # Define the endpoint and parameters
        url = 'https://newsapi.org/v2/everything'
        params = {
            'q': company_name,
            'apiKey': api_key,
            'language': language,
            'sortBy': 'publishedAt',
            'pageSize': 100  # Maximum allowed per request
        }
    
        if from_date:
            params['from'] = from_date
        if to_date:
            params['to'] = to_date
        
        # Make the API request
        response = requests.get(url, params=params)
        
        # Handle response
        if response.status_code != 200:
            print(f"Failed to fetch news data: {response.status_code} - {response.text}")
            return pd.DataFrame()
        
        data = response.json()
    
        if data.get('status') != 'ok':
            print(f"Error in response: {data.get('message')}")
            return pd.DataFrame()
    
        articles = data.get('articles', [])
    
        if not articles:
            print(f"No news articles found for {company_name}.")
            return pd.DataFrame()
    
        # Process articles into DataFrame
        news_df = pd.DataFrame(articles)
    
        # Select relevant columns
        news_df = news_df[['source', 'author', 'title', 'description', 'url', 'publishedAt', 'content']]
    
        # Rename columns for consistency
        news_df.columns = ['source', 'author', 'headline', 'description', 'url', 'published_at', 'content']
    
        # Convert 'published_at' to datetime
        news_df['published_at'] = pd.to_datetime(news_df['published_at'])
    
        # Add company name as a column
        news_df['company'] = company_name.upper()
    
        # Store raw news data
        dataset_name = f"news_{company_name.lower()}"
        self.store_raw_data(dataset_name, news_df)
    
        print(f"News data for {company_name} stored at {os.path.join(self.raw_data_path, f'{dataset_name}.csv')}")
        
        # Register dataset in Data Catalog if provided
        if data_catalog:
            metadata = {
                'data_type': 'news',
                'company': company_name.upper(),
                'source': 'NewsAPI',
                'format': 'csv',
                'language': language,
                'from_date': from_date,
                'to_date': to_date,
                'description': f"News articles for {company_name.upper()} fetched from NewsAPI.org"
            }
            data_catalog.add_dataset(dataset_name, metadata)
            print(f"Dataset '{dataset_name}' registered in Data Catalog.")
        
        print(f"News data fetch completed for {company_name}.")
        
        return news_df



class DataCatalog:
    def __init__(self, catalog_path: str):
        self.catalog_path = catalog_path
        if os.path.exists(self.catalog_path):
            with open(self.catalog_path, 'r') as f:
                self.catalog = json.load(f)
        else:
            self.catalog = []
    
    def add_dataset(self, dataset_name: str, metadata: Dict[str, Any]):
        # Check if dataset already exists
        for entry in self.catalog:
            if entry['dataset_name'] == dataset_name:
                print(f"Dataset '{dataset_name}' already exists in catalog. Updating metadata.")
                entry['metadata'].update(metadata)
                self._save_catalog()
                return
        # Add new dataset
        dataset_entry = {
            'dataset_name': dataset_name,
            'metadata': metadata
        }
        self.catalog.append(dataset_entry)
        self._save_catalog()
        print(f"Dataset '{dataset_name}' added to catalog.")
    
    def search_datasets(self, search_terms: Dict[str, Any]) -> List[Dict[str, Any]]:
        results = []
        for entry in self.catalog:
            match = True
            for key, value in search_terms.items():
                if key not in entry['metadata'] or entry['metadata'][key] != value:
                    match = False
                    break
            if match:
                results.append(entry)
        print(f"Found {len(results)} datasets matching search terms.")
        return results
    
    def get_dataset_metadata(self, dataset_name: str) -> Dict[str, Any]:
        for entry in self.catalog:
            if entry['dataset_name'] == dataset_name:
                return entry['metadata']
        raise ValueError(f"Dataset '{dataset_name}' not found in catalog.")
    
    def _save_catalog(self):
        with open(self.catalog_path, 'w') as f:
            json.dump(self.catalog, f, indent=4)

# Class for data workbench
class DataWorkbench:
    def __init__(self, data_lake: DataLake, data_catalog: DataCatalog):
        self.data_lake = data_lake
        self.data_catalog = data_catalog
    

    # Loads a data set from the data lake
    def load_data(self, dataset_name: str, processed: bool = False):
        if processed:
            data = self.data_lake.retrieve_processed_data(dataset_name)
        else:
            data = self.data_lake.retrieve_raw_data(dataset_name)
        return data
    
    # saves a data set to the data lake
    def save_data(self, dataset_name: str, data: pd.DataFrame, processed: bool = True):
        if processed:
            self.data_lake.store_processed_data(dataset_name, data)
        else:
            self.data_lake.store_raw_data(dataset_name, data)
    
    # filter data set based on some query 
    def filter_data(self, data: pd.DataFrame, column: str, condition: str) -> pd.DataFrame:
        filtered_data = data.query(condition)
        print(f"Data filtered on '{column}' with condition '{condition}'.")
        return filtered_data

    def aggregate_data(
        self,
        data: pd.DataFrame,
        group_by: Optional[List[str]] = None,
        aggregations: Optional[Dict[str, Union[str, List[str], Callable]]] = None,
        resample_freq: Optional[str] = None,
        resample_agg: Optional[Dict[str, Union[str, List[str], Callable]]] = None,
        dropna: bool = True,
        as_index: bool = False,
        sort: bool = True,
        **kwargs
    ) -> pd.DataFrame:
        """
        Aggregates the DataFrame based on grouping columns and aggregation functions.
        Optionally performs resampling for time-series data.

        Parameters:
            data (pd.DataFrame): The input DataFrame to aggregate.
            group_by (List[str], optional): Columns to group by. If None, aggregation is applied to the entire DataFrame.
            aggregations (Dict[str, Union[str, List[str], Callable]], optional): 
                Aggregation functions for each column.
            resample_freq (str, optional): Resampling frequency string (e.g., '15T' for 15 minutes).
                Applicable only if the DataFrame has a DateTime index.
            resample_agg (Dict[str, Union[str, List[str], Callable]], optional): 
                Aggregation functions to apply during resampling.
            dropna (bool, default=True): Whether to drop rows with NaN values after aggregation.
            as_index (bool, default=False): Whether to set the grouping columns as the index.
            sort (bool, default=True): Whether to sort the grouped DataFrame.
            **kwargs: Additional keyword arguments for pandas methods.

        Returns:
            pd.DataFrame: The aggregated DataFrame.
        """
        try:
            # Handle resampling if specified
            if resample_freq:
                if not pd.api.types.is_datetime64_any_dtype(data.index):
                    # Attempt to convert a 'Date' or 'Datetime' column to datetime and set as index
                    if 'Date' in data.columns or 'Datetime' in data.columns:
                        date_col = 'Date' if 'Date' in data.columns else 'Datetime'
                        data[date_col] = pd.to_datetime(data[date_col])
                        data.set_index(date_col, inplace=True)
                        self.logger.info(f"Set '{date_col}' as DateTime index for resampling.")
                    else:
                        raise ValueError("DataFrame must have a DateTime index or a 'Date'/'Datetime' column for resampling.")

                # Perform resampling
                if resample_agg:
                    aggregated = data.resample(resample_freq).agg(resample_agg)
                    self.logger.info(f"Data resampled to '{resample_freq}' intervals with aggregations: {resample_agg}")
                elif aggregations:
                    aggregated = data.resample(resample_freq).agg(aggregations)
                    self.logger.info(f"Data resampled to '{resample_freq}' intervals with aggregations: {aggregations}")
                else:
                    raise ValueError("No aggregation functions provided for resampling.")
            else:
                aggregated = data.copy()

            # Perform grouping and aggregation if specified
            if group_by and aggregations:
                # Validate group_by columns
                missing_cols = [col for col in group_by if col not in aggregated.columns]
                if missing_cols:
                    raise KeyError(f"Grouping columns not found in DataFrame: {missing_cols}")
                
                aggregated = aggregated.groupby(group_by).agg(aggregations).reset_index(drop=not as_index)
                self.logger.info(f"Data grouped by {group_by} with aggregations {aggregations}.")

            # Drop NaN values if specified
            if dropna:
                before_drop = len(aggregated)
                aggregated.dropna(inplace=True)
                after_drop = len(aggregated)
                self.logger.info(f"Dropped {before_drop - after_drop} rows with NaN values after aggregation.")

            # Sort the DataFrame if specified and group_by is provided
            if sort and group_by:
                aggregated.sort_values(by=group_by, inplace=True)
                self.logger.info(f"Data sorted by {group_by}.")

            return aggregated

        except Exception as e:
            self.logger.error(f"Error during aggregation: {e}", exc_info=True)
            return pd.DataFrame()  # Return empty DataFrame on error

    # adds information about the process dataset to the catalog
    def register_processed_dataset(self, dataset_name: str, metadata: Dict[str, Any]):
        # Add information about processed data to the catalog
        processed_metadata = metadata.copy()
        processed_metadata['processed'] = True
        self.data_catalog.add_dataset(f"{dataset_name}_processed", processed_metadata)


# Base class for the quant data models
class BaseDataModel:
    def __init__(self, timestamp: datetime, symbol: str = None):
        self.timestamp = timestamp if isinstance(timestamp, datetime) else datetime.now()
        self.symbol = symbol  # Optional, for models where a symbol might be applicable
    
    def __repr__(self):
        return f"{self.__class__.__name__}(Timestamp: {self.timestamp}, Symbol: {self.symbol})"
    
    def is_recent(self, days: int = 7) -> bool:
        delta = datetime.now() - self.timestamp
        return delta.days <= days
    
    def is_above_threshold(self, value: float, threshold: float) -> bool:
        return value > threshold
