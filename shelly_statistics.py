import msgspec
import requests
import pandas as pd
from awattar.client import AwattarClient
from typing import Optional, Tuple
import concurrent.futures
from functools import partial
import time
from threading import Lock
import datetime
import seaborn as sns
import matplotlib.pyplot as plt
from tqdm import tqdm

class PowerConsumptionAnalyzer:
    def __init__(self, api_key: str, device_id: str):
        self.KEY = api_key
        self.ID = device_id
        self.URL = "https://shelly-88-eu.shelly.cloud/v2/statistics/power-consumption/em-3p?channel=%i&id=%s&auth_key=%s"
        self.client = AwattarClient('DE')
        self.df_lock = Lock()
        self.rate_limit_lock = Lock()
        self.last_request_time = {}

    def rate_limited_request(self, url: str, min_interval: float = 0.5) -> requests.Response:
        """Makes a rate-limited request with retries"""
        with self.rate_limit_lock:
            current_time = time.time()
            if url in self.last_request_time:
                time_since_last = current_time - self.last_request_time[url]
                if time_since_last < min_interval:
                    time.sleep(min_interval - time_since_last)
            self.last_request_time[url] = time.time()

        max_retries = 5
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                return response
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                print(f"Request failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
                time.sleep(retry_delay * (attempt + 1))

    def get_power_consumption(self, *, channel: Optional[int] = None, 
                            start: Optional[str] = None,
                            end: Optional[str] = None,
                            netto_cost: bool = False) -> pd.DataFrame:
        """
        Fetch power consumption data for a specific channel and return as pandas DataFrame.

        :param channel: The channel number (0, 1, or 2) or None (will return the sum).
        :param id: The device ID.
        :param key: The authentication key.
        :parameter start: Optional; the start date in ISO format for date only (e.g., '2023-01-01').
        :parameter end: Optional; the end date in ISO format for date only (e.g., '2023-01-31').
        :param netto_cost: If True, calculates the cost based on net consumption (consumption - reversed).
        :return: A pandas DataFrame containing the power consumption data with datetime index.
        """
        if start or end:
            if not start or not end:
                raise ValueError("Both start and end parameters must be provided together.")
            else: #check format
                try:
                    pd.to_datetime(start)
                    pd.to_datetime(end)
                except ValueError:
                    raise ValueError("Start and end dates must be in ISO format (YYYY-MM-DD).")
        
        req = self.URL % (0, self.ID, self.KEY)
        
        if start:
            req += f"&date_range=day&date_from={start}&date_to={end}"

        response = self.rate_limited_request(req)
        data = msgspec.json.decode(response.content)
        
        if channel:
            df = pd.DataFrame(data['history'][channel])
        else:
            df = pd.DataFrame(data['sum'])
        
        # Convert datetime string to proper datetime index
        if 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime']).dt.tz_localize(None)
            df.set_index('datetime', inplace=True)
        
        # Convert numeric columns to appropriate types
        numeric_columns = ['consumption', 'reversed', 'min_voltage', 'max_voltage', 'cost']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col])
        
        # Get Awattar prices and create price DataFrame based on df datetime index
        if not start or not end:
            start = df.index.min().strftime('%Y-%m-%d')
            end = df.index.max().strftime('%Y-%m-%d')
        prices = self.client.request(pd.to_datetime(start), pd.to_datetime(end))
        price_data = []
        for price_entry in prices:
            # Convert to UTC first, then remove timezone info
            price_datetime = pd.to_datetime(price_entry.start_datetime)
            if price_datetime.tzinfo is not None:
                price_datetime = price_datetime.tz_convert('UTC').tz_localize(None)
            price_data.append({
                'datetime': price_datetime,
                'price_per_kWh': price_entry.price_per_kWh
            })
        price_df = pd.DataFrame(price_data)
        price_df.set_index('datetime', inplace=True)
        
        # Resample price data to match consumption data frequency
        price_df = price_df.resample('h').ffill()
              # Add price column to main DataFrame
        df['price_per_kWh'] = price_df['price_per_kWh']
        
        # Calculate actual cost - using vectorized operations
        df['netto_consumption'] = (df['consumption'] - df['reversed']).clip(lower=0)
        if netto_cost:
            df['cost'] = df['netto_consumption'] / 1000 * df['price_per_kWh']
        else:
            df['cost'] = df['consumption'] / 1000 * df['price_per_kWh']

        return df

    def _fetch_daily_data(self, day: int, channel: Optional[int] = None, 
                         netto_cost: bool = False) -> pd.DataFrame:
        """Helper function to fetch data for a single day"""
        try:
            start_date = (datetime.datetime.now() - datetime.timedelta(days=day+1)).strftime('%Y-%m-%d')
            end_date = (datetime.datetime.now() - datetime.timedelta(days=day)).strftime('%Y-%m-%d')
            return self.get_power_consumption(channel=channel, start=start_date, 
                                           end=end_date, netto_cost=netto_cost)
        except Exception as e:
            print(f"Error fetching data for day {day}: {str(e)}")
            raise

    def get_data_from_cloud(self, days_to_fetch: int = 365, 
                           netto_cost: bool = False, 
                           workers: int = 3) -> pd.DataFrame:
        """
        Fetch power consumption data from the cloud for a specified number of days.
        
        Args:
            days_to_fetch: Number of days to fetch data for
            netto_cost: If True, calculates costs based on net consumption (consumption - reversed)
            workers: Number of parallel workers for fetching data
        
        Returns:
            DataFrame with power consumption data
        """
        df = pd.DataFrame()
        
        print(f"\nFetching power consumption data for the last {days_to_fetch} days...")
        
        if days_to_fetch <= 10:
            # Sequential processing for small number of days
            for days in tqdm(range(days_to_fetch), desc="Fetching data", unit="day"):
                try:
                    daily_df = self._fetch_daily_data(days, channel=0, netto_cost=netto_cost)
                    with self.df_lock:
                        df = pd.concat([daily_df, df])
                except Exception as e:
                    print(f"\nError on day {days}: {str(e)}")
                    continue
        else:
            # Parallel processing for larger number of days
            fetch_func = partial(self._fetch_daily_data, channel=0, netto_cost=netto_cost)
            failed_days = []
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {executor.submit(fetch_func, day): day for day in range(days_to_fetch)}
                
                for future in tqdm(concurrent.futures.as_completed(futures), 
                                 total=days_to_fetch, 
                                 desc="Fetching data", 
                                 unit="day"):
                    day = futures[future]
                    try:
                        daily_df = future.result()
                        with self.df_lock:
                            df = pd.concat([daily_df, df])
                    except Exception as e:
                        print(f"\nError on day {day}: {str(e)}")
                        failed_days.append(day)
            
            # Retry failed days sequentially
            if failed_days:
                print(f"\nRetrying {len(failed_days)} failed days sequentially...")
                for day in tqdm(failed_days, desc="Retrying failed days", unit="day"):
                    try:
                        daily_df = self._fetch_daily_data(day, channel=0, netto_cost=netto_cost)
                        with self.df_lock:
                            df = pd.concat([daily_df, df])
                    except Exception as e:
                        print(f"\nFinal error on day {day}: {str(e)}")

        if df.empty:
            raise ValueError("No data was fetched successfully")

        # Sort index after concatenation
        return df.sort_index()

    def get_data_from_csv(self, file_path: str) -> pd.DataFrame:
        """    Load power consumption data from a CSV file into a pandas DataFrame.
        :param file_path: Path to the CSV file.
        :return: A pandas DataFrame containing the power consumption data.
        """
        try:
            df = pd.read_csv(file_path, parse_dates=['datetime'], index_col='datetime')
            # Ensure numeric columns are properly typed
            numeric_columns = ['consumption', 'reversed', 'min_voltage', 'max_voltage', 'cost', 'price_per_kWh']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            return df
        except FileNotFoundError:
            print(f"File not found: {file_path}")
            return pd.DataFrame()
        

    def create_heatmap_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Create pivot tables for heatmaps"""
        df['hour'] = df.index.hour
        df['day'] = pd.Categorical(
            df.index.day_name(),
            categories=['Monday', 'Tuesday', 'Wednesday', 
                       'Thursday', 'Friday', 'Saturday', 'Sunday'],
            ordered=True
        )

        pivot_tables = {}
        for value in ['consumption', 'cost', 'price_per_kWh']:
            pivot_tables[value] = df.pivot_table(
                values=value,
                index='hour',
                columns='day',
                aggfunc='mean'
            )

        return pivot_tables['consumption'], pivot_tables['cost'], pivot_tables['price_per_kWh']

    def plot_heatmaps(self, df: pd.DataFrame, save_path: str = 'power_consumption_heatmaps.png'):
        """Create and save heatmaps for consumption, cost, and price"""
        consumption_pivot, cost_pivot, price_pivot = self.create_heatmap_data(df)
        
        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 24))
        
        # Consumption heatmap
        sns.heatmap(
            consumption_pivot,
            cmap='YlOrRd',
            annot=True,
            fmt='.2f',
            cbar_kws={'label': 'Average Consumption (kW)'},
            ax=ax1
        )
        ax1.set_title('Daily Power Consumption Patterns')
        ax1.set_xlabel('Day of Week')
        ax1.set_ylabel('Hour of Day')
        ax1.tick_params(axis='x', rotation=45)

        # Cost heatmap
        sns.heatmap(
            cost_pivot,
            cmap='YlOrRd',
            annot=True,
            fmt='.3f',
            cbar_kws={'label': 'Average Cost (€)'},
            ax=ax2
        )
        ax2.set_title('Daily Cost Patterns')
        ax2.set_xlabel('Day of Week')
        ax2.set_ylabel('Hour of Day')
        ax2.tick_params(axis='x', rotation=45)

        # Price heatmap
        sns.heatmap(
            price_pivot,
            cmap='YlOrRd',
            annot=True,
            fmt='.3f',
            cbar_kws={'label': 'Price per kWh (€)'},
            ax=ax3
        )
        ax3.set_title('Daily Price Patterns')
        ax3.set_xlabel('Day of Week')
        ax3.set_ylabel('Hour of Day')
        ax3.tick_params(axis='x', rotation=45)

        plt.tight_layout()
        plt.savefig(save_path)
        print(f"\nHeatmaps have been saved as '{save_path}'")

    def analyze_power_consumption(self, days_to_fetch: int = 365, 
                                netto_cost: bool = True,
                                save_csv: bool = True,
                                save_plots: bool = True) -> pd.DataFrame:
        """Main analysis function that orchestrates the entire process"""
        df = self.get_data_from_cloud(days_to_fetch=days_to_fetch, netto_cost=netto_cost)
        
        print("\nFirst 5 rows of the DataFrame:")
        print(df.head())
        print("\nBasic statistics:")
        print(df.describe())
        print(f"\nWeighted average cost per kWh: {(df['cost'].sum() / df['netto_consumption'].sum()*100000):0.2f} cent")

        if save_csv:
            csv_path = f'power_consumption_data_{days_to_fetch}.csv'
            print(f"\nSaving DataFrame to '{csv_path}'...")
            df.to_csv(csv_path)

        if save_plots:
            self.plot_heatmaps(df, f'power_consumption_heatmaps_{days_to_fetch}.png')

        return df


if __name__ == "__main__":
    analyzer = PowerConsumptionAnalyzer(
        api_key="MWY1MzdmdWlk1C69C7373446071BB2B1AFB2E08DCBAEADE4E0BC177A71ACEDA8F5151F20B08163CD77E4D638C020",
        device_id="485519dbee6d"
    )
    
    # Analyze data for the last year
    # df = analyzer.analyze_power_consumption(days_to_fetch=365, netto_cost=True)
    
    # Alternative: Load from CSV
    df = analyzer.get_data_from_csv('power_consumption_data_365.csv')
    analyzer.plot_heatmaps(df, 'power_consumption_heatmaps_from_csv.png')


