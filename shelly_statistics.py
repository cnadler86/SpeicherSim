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
        self.BASE_URL = "https://shelly-88-eu.shelly.cloud/v2/statistics/power-consumption/em-3p"
        self.client = AwattarClient('DE')
        self.df_lock = Lock()
        self.rate_limit_lock = Lock()
        self.last_request_time = {}

    def rate_limited_request(self, url: str, min_interval: float = 0.5) -> requests.Response|None:
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
        
        # Construct parameters dictionary
        params = {
            'channel': 0,
            'id': self.ID,
            'auth_key': self.KEY
        }
        
        if start:
            params.update({
                'date_range': 'day',
                'date_from': start,
                'date_to': end
            })

        # Use requests' built-in parameter handling
        response = requests.get(self.BASE_URL, params=params, timeout=10)
        data = msgspec.json.decode(response.content) if response else None
        
        if data:
            if channel and channel in data['history']:
                df = pd.DataFrame(data['history'][channel])
            else:
                df = pd.DataFrame(data['sum'])
        else:
            raise ValueError("No data returned from the API. Check your parameters or connection.")
        
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
        df = self.calculate_costs(df, netto_cost=netto_cost)

        return df

    def calculate_costs(self, df: pd.DataFrame, netto_cost: bool = False) -> pd.DataFrame:
        """
        Calculate costs based on the DataFrame and return it with a new 'cost' column.
        
        Args:
            df: DataFrame containing power consumption data
            netto_cost: If True, calculates costs based on net consumption (consumption - reversed)
        
        Returns:
            DataFrame with an additional 'cost' column
        """
        if df.empty:
            raise ValueError("DataFrame is empty. Please fetch data first.")
        
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
            # Lade CSV und konvertiere datetime
            df = pd.read_csv(file_path, parse_dates=['datetime'])
            
            # Behandle Duplikate bei Zeitumstellung - Mittelwert der Messungen verwenden
            if df.duplicated(subset=['datetime']).any():
                print("Hinweis: Doppelte Zeitstempel gefunden (wahrscheinlich Zeitumstellung). Verwende Mittelwerte.")
                df = df.groupby('datetime').agg({
                    'consumption': 'mean',
                    'reversed': 'mean',
                    'min_voltage': 'mean',
                    'max_voltage': 'mean',
                    'cost': 'mean',
                    'price_per_kWh': 'first',  # Preis sollte gleich sein
                    'purpose': 'first',
                    'tariff_id': 'first',
                    'netto_consumption': 'mean'
                }).reset_index()
            
            df.set_index('datetime', inplace=True)
            
            # Entferne Zeilen mit fehlenden Werten
            df = df.dropna(subset=['consumption', 'reversed', 'price_per_kWh'])
            
            # Stelle sicher, dass numerische Spalten den korrekten Typ haben
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
        if df.empty:
            raise ValueError("DataFrame is empty. Please fetch data first.")
        # Ensure the DataFrame has a datetime index
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("DataFrame index must be a DatetimeIndex.")
        
        df['hour'] = df.index.hour
        df['day'] = pd.Categorical(
            df.index.day_name(),
            categories=['Monday', 'Tuesday', 'Wednesday', 
                       'Thursday', 'Friday', 'Saturday', 'Sunday'],
            ordered=True
        )

        pivot_tables = {}
        for value in ['netto_consumption', 'cost', 'price_per_kWh']:
            pivot_tables[value] = df.pivot_table(
                values=value,
                index='hour',
                columns='day',
                aggfunc='mean',
                observed=True
            )

        return pivot_tables['netto_consumption'], pivot_tables['cost'], pivot_tables['price_per_kWh']

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

    def simulate_battery_storage(self, df: pd.DataFrame, capacity_wh: float, 
                            efficiency: float, charging_power_w: int) -> pd.DataFrame:
        """
        Simulate battery storage operation with dynamic electricity tariffs.
        
        Args:
            df: Input DataFrame with consumption and price data
            capacity_wh: Battery capacity in Wh
            efficiency: Battery round-trip efficiency (0-1)
            charging_power_w: Maximum charging power in Watts
        
        Returns:
            DataFrame with additional columns for adjusted consumption and costs
        """
        # Create copy of DataFrame to avoid modifying original
        result_df = df.copy()
        result_df['battery_energy'] = 0.0
        # Calculate real netto consumption without clipping to 0
        result_df['netto_consumption'] = df['consumption'] - df['reversed']
        result_df['adjusted_consumption'] = result_df['netto_consumption'].copy()
        
        # Process day by day
        if df.empty:
            raise ValueError("DataFrame is empty. Please fetch data first.")
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("DataFrame index must be a DatetimeIndex.")
        
        for day in pd.unique(df.index.date):
            day_mask = df.index.date == day
            day_df = result_df[day_mask].copy()
            
            # Initialize battery state for the day
            battery_energy = 0.0
            
            # First, store excess energy (negative consumption)
            excess_mask = day_df['netto_consumption'] < 0
            for idx in day_df[excess_mask].index:
                excess = abs(day_df.loc[idx, 'netto_consumption'])
                chargeable = min(
                    excess * efficiency,  # Berücksichtige Verluste beim Laden
                    capacity_wh - battery_energy,
                    charging_power_w  # Maximale Ladeleistung pro Stunde
                )
                battery_energy += chargeable
                # Reduziere den negativen Verbrauch (Einspeisung) um die geladene Energie
                result_df.loc[idx, 'adjusted_consumption'] = -(excess - chargeable/efficiency)
                result_df.loc[idx, 'battery_energy'] = battery_energy
                
            # Charge remaining capacity in cheapest hours
            remaining_capacity = capacity_wh - battery_energy
            if remaining_capacity > 0:
                # Sort hours by price (ascending) for charging strategy
                day_prices = day_df['price_per_kWh'].sort_values()
                
                # Try to charge in each hour, starting with the cheapest
                for hour in day_prices.index:
                    if remaining_capacity <= 0:
                        break
                        
                    charge_amount = min(
                        remaining_capacity,
                        charging_power_w  # Maximale Ladeleistung pro Stunde
                    )
                    
                    # Add charging energy to consumption (considering efficiency)
                    current_consumption = pd.to_numeric(result_df.at[hour, 'adjusted_consumption'])
                    result_df.at[hour, 'adjusted_consumption'] = pd.to_numeric(current_consumption + charge_amount/efficiency)
                    battery_energy += charge_amount
                    remaining_capacity = capacity_wh - battery_energy
                    result_df.loc[hour, 'battery_energy'] = battery_energy
                    
            # Sort hours by price for discharge strategy
            day_prices = day_df['price_per_kWh'].sort_values(ascending=False)
            
            # Define discharge percentages for different price levels
            discharge_levels = [
                (day_prices.index[:1], 0.4),   # Most expensive hour: 40%
                (day_prices.index[1:2], 0.7),  # Second most expensive: 70%
                (day_prices.index[2:], 1.0)    # Remaining expensive hours: 100%
            ]
            
            # Apply discharge strategy
            for hours, percentage in discharge_levels:
                for hour in hours:
                    if battery_energy <= 0:
                        break
                        
                    consumption = pd.to_numeric(result_df.at[hour, 'adjusted_consumption'])
                    if consumption <= 0:
                        continue
                        
                    target_discharge = consumption * percentage
                    possible_discharge = min(
                        target_discharge,
                        battery_energy * efficiency
                    )
                    
                    result_df.loc[hour, 'adjusted_consumption'] = consumption - possible_discharge
                    battery_energy -= possible_discharge/efficiency
                    result_df.loc[hour, 'battery_energy'] = battery_energy
            
        # Calculate adjusted costs
        # Kosten werden in Euro berechnet (Verbrauch in Wh * Preis pro kWh / 1000 für Umrechnung Wh -> kWh)
        result_df['cost'] = result_df['adjusted_consumption'] * result_df['price_per_kWh'] / 1000
        
        return result_df


    def correct_energy_price(self, df: pd.DataFrame, netto_static_cost:float, tax:float=0.19, netto_cost:bool=True) -> pd.DataFrame:
        if df.empty:
            raise ValueError("DataFrame is empty. Please fetch data first.")
        if tax < 0 or tax > 1:
            raise ValueError("Tax must be between 0 and 1 (e.g., 0.19 for 19%).")
        if netto_static_cost < 0:
            raise ValueError("Netto static cost must be non negative.")
        if netto_static_cost > 1:
            print(Warning("Netto static cost seems to be too high, check your value."))
        
        df['price_per_kWh'] = df['price_per_kWh']* (1 + tax) + netto_static_cost
        
        return self.calculate_costs(df, netto_cost=netto_cost)

if __name__ == "__main__":
    import shellyKeys
    analyzer = PowerConsumptionAnalyzer(api_key=shellyKeys.API_KEY, device_id=shellyKeys.DEVICE_ID)
    
    # Analyze data for the last year
    # df = analyzer.analyze_power_consumption(days_to_fetch=360, netto_cost=True)
    
    # Alternative: Load from CSV
    df = analyzer.get_data_from_csv('power_consumption_data_360.csv')
    df = analyzer.correct_energy_price(df, netto_static_cost=0.1767, tax=0.19, netto_cost=True)
    # analyzer.plot_heatmaps(df, 'power_consumption_heatmaps_from_csv.png')
    
    # Simulate battery storage
    battery_capacity_wh = 2110*0.9
    battery_efficiency = 0.9
    charging_power_w = 1200 
    df_with_battery = analyzer.simulate_battery_storage(
        df, 
        capacity_wh=battery_capacity_wh, 
        efficiency=battery_efficiency, 
        charging_power_w=charging_power_w
    )

    # analyzer.plot_heatmaps(df_with_battery, 'power_consumption_heatmaps_df_with_battery.png')
    print("\nWithout battery:")
    print(f"Total cost: {df['cost'].sum():0.2f} €.")
    print(f"Weighted average cost per kWh ({df['netto_consumption'].sum()/1000:0.1f} kWh): \t{(df['cost'].sum() / df['netto_consumption'].sum()*100000):0.2f} cent")
    print("\nWith battery:")
    print(f"Total cost: {df_with_battery['cost'].sum():0.2f} €. ")
    print(f"Weighted average cost per kWh ({df_with_battery['netto_consumption'].sum()/1000:0.1f} kWh): \t{(df_with_battery['cost'].sum() / df_with_battery['netto_consumption'].sum()*100000):0.2f} cent")

