import pandas as pd
import psycopg2
from backtesting import Backtest, Strategy
from backtesting.lib import crossover
import talib

# Database connection parameters
conn_params = {
    'dbname': 'algo_trading',
    'user': 'Archit',
    'password': 'Archit@1',
    'host': 'localhost',
    'port': '5432'
}

# Fetch 15-minute data from TimescaleDB
def fetch_15min_data(symbols, start, end):
    conn = psycopg2.connect(**conn_params)
    query = """
        SELECT timestamp, symbol, open, high, low, close, volume
        FROM alpaca_minute_data
        WHERE timestamp >= %s AND timestamp <= %s
        AND symbol = ANY(%s)
        ORDER BY timestamp;
    """
    df_list = []
    for symbol in symbols:
        df = pd.read_sql_query(query, conn, params=(start, end, [symbol]))
        df.set_index('timestamp', inplace=True)
        df.index = pd.to_datetime(df.index)
        df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'}, inplace=True)
        df['symbol'] = symbol  # Ensure the symbol column is added
        df = df.resample('15min').agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum',
            'symbol': 'last'  # Retain the symbol after resampling
        }).dropna()
        df_list.append(df)
    conn.close()
    return pd.concat(df_list)

# Define trend-following strategy using EMA and ADX
class EMADXStrategy(Strategy):
    ema_window = 20
    adx_window = 14
    adx_threshold = 25

    def init(self):
        self.ema = self.I(talib.EMA, self.data.Close, self.ema_window)
        self.adx = self.I(talib.ADX, self.data.High, self.data.Low, self.data.Close, timeperiod=self.adx_window)

    def next(self):
        if self.adx[-1] > self.adx_threshold and crossover(self.data.Close, self.ema):
            self.buy()
        elif crossover(self.ema, self.data.Close):
            self.sell()

# Define mean-reversion strategy using Bollinger Bands
class BollingerBandsStrategy(Strategy):
    window = 20
    num_std_dev = 2

    def init(self):
        upper, middle, lower = self.I(talib.BBANDS, self.data.Close, timeperiod=self.window, nbdevup=self.num_std_dev, nbdevdn=self.num_std_dev, matype=0)
        self.upper_band = upper
        self.lower_band = lower

    def next(self):
        if self.data.Close[-1] < self.lower_band[-1]:
            self.buy()
        elif self.data.Close[-1] > self.upper_band[-1]:
            self.sell()

# Function to optimize EMADXStrategy
def optimize_emadx_strategy(df):
    best_result = None
    best_params = None
    for ema_window in range(10, 50, 5):
        for adx_window in range(10, 30, 5):
            for adx_threshold in range(20, 40, 5):
                EMADXStrategy.ema_window = ema_window
                EMADXStrategy.adx_window = adx_window
                EMADXStrategy.adx_threshold = adx_threshold
                bt = Backtest(df, EMADXStrategy, cash=10000, commission=.002)
                stats = bt.run()
                if best_result is None or stats['Return [%]'] > best_result['Return [%]']:
                    best_result = stats
                    best_params = {'ema_window': ema_window, 'adx_window': adx_window, 'adx_threshold': adx_threshold}
    return best_result, best_params

# Function to optimize BollingerBandsStrategy
def optimize_bollinger_strategy(df):
    best_result = None
    best_params = None
    for window in range(10, 50, 5):
        for num_std_dev in range(1, 4):
            BollingerBandsStrategy.window = window
            BollingerBandsStrategy.num_std_dev = num_std_dev
            bt = Backtest(df, BollingerBandsStrategy, cash=10000, commission=.002)
            stats = bt.run()
            if best_result is None or stats['Return [%]'] > best_result['Return [%]']:
                best_result = stats
                best_params = {'window': window, 'num_std_dev': num_std_dev}
    return best_result, best_params

# Backtest and optimize each strategy for each stock
def run_backtests_and_optimization(symbols, start, end):
    data = fetch_15min_data(symbols, start, end)
    results = {}

    for symbol in symbols:
        df = data[data['symbol'] == symbol].drop(columns='symbol')
        
        # Optimize EMADX Strategy
        optimized_emadx, params_emadx = optimize_emadx_strategy(df)
        
        # Optimize Bollinger Bands Strategy
        optimized_bollinger, params_bollinger = optimize_bollinger_strategy(df)
        
        results[symbol] = {
            'emadx': {'result': optimized_emadx, 'params': params_emadx},
            'bollinger': {'result': optimized_bollinger, 'params': params_bollinger}
        }
    
    return results

# Parameters
symbols = ['MCD', 'PEP', 'KO']
start = '2023-05-15'
end = '2023-08-23'

# Run backtests and optimization
results = run_backtests_and_optimization(symbols, start, end)

# Print results
for symbol, strategies in results.items():
    print(f"\nResults for {symbol}:")
    print("Optimized EMA-ADX Strategy:")
    print(f"Parameters: {strategies['emadx']['params']}")
    print(strategies['emadx']['result'])
    
    print("Optimized Bollinger Bands Strategy:")
    print(f"Parameters: {strategies['bollinger']['params']}")
    print(strategies['bollinger']['result'])
