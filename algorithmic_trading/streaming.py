import asyncio
import logging
from alpaca_trade_api.stream import Stream
from alpaca_trade_api.rest import REST, TimeFrame
from decouple import config
import talib

# Configure logging
logging.basicConfig(level=logging.INFO)

# Alpaca API credentials
API_KEY = config('ALPACA_KEY')
SECRET_KEY = config('ALPACA_SECRET')
BASE_URL = 'https://paper-api.alpaca.markets'

# Initialize Alpaca API clients
rest_api = REST(API_KEY, SECRET_KEY, base_url=BASE_URL)
stream = Stream(API_KEY, SECRET_KEY, base_url='https://stream.data.alpaca.markets/v2/sip')

# Strategy parameters for EMA-ADX and Bollinger Bands
STRATEGY_PARAMS = {
    'MCD': {'strategy': 'ema_adx', 'ema_window': 40, 'adx_window': 10, 'adx_threshold': 25},
    'KO': {'strategy': 'ema_adx', 'ema_window': 25, 'adx_window': 20, 'adx_threshold': 35},
    'PEP': {'strategy': 'bollinger_bands', 'window': 15, 'num_std_dev': 3}
}

# Store historical bars for strategy calculation
historical_data = {
    'MCD': [],
    'KO': [],
    'PEP': []
}

# Function to place an order
def place_order(symbol, qty, side):
    try:
        rest_api.submit_order(
            symbol=symbol,
            qty=qty,
            side=side,
            type='market',
            time_in_force='gtc'
        )
        logging.info(f"Order placed: {side} {qty} shares of {symbol}")
    except Exception as e:
        logging.error(f"Error placing order for {symbol}: {e}")

# EMA-ADX strategy execution
def execute_ema_adx(symbol, latest_price):
    params = STRATEGY_PARAMS[symbol]
    if len(historical_data[symbol]) < max(params['ema_window'], params['adx_window']):
        return  # Not enough data to calculate indicators

    close_prices = [bar['c'] for bar in historical_data[symbol]]
    high_prices = [bar['h'] for bar in historical_data[symbol]]
    low_prices = [bar['l'] for bar in historical_data[symbol]]

    ema = talib.EMA(close_prices, timeperiod=params['ema_window'])[-1]
    adx = talib.ADX(high_prices, low_prices, close_prices, timeperiod=params['adx_window'])[-1]

    if adx > params['adx_threshold']:
        if latest_price > ema:
            place_order(symbol, 1, 'buy')
        elif latest_price < ema:
            place_order(symbol, 1, 'sell')

# Bollinger Bands strategy execution
def execute_bollinger_bands(symbol, latest_price):
    params = STRATEGY_PARAMS[symbol]
    if len(historical_data[symbol]) < params['window']:
        return  # Not enough data to calculate indicators

    close_prices = [bar['c'] for bar in historical_data[symbol]]

    upper_band, middle_band, lower_band = talib.BBANDS(
        close_prices, timeperiod=params['window'], nbdevup=params['num_std_dev'], nbdevdn=params['num_std_dev'], matype=0)

    if latest_price < lower_band[-1]:
        place_order(symbol, 1, 'buy')
    elif latest_price > upper_band[-1]:
        place_order(symbol, 1, 'sell')

# Callback for trade updates
async def trade_callback(data):
    symbol = data['S']
    latest_price = data['p']
    logging.info(f"Received trade update for {symbol} at price {latest_price}")

    # Store the latest trade data
    bar = {'c': latest_price, 'h': latest_price, 'l': latest_price, 't': data['t']}
    if len(historical_data[symbol]) >= STRATEGY_PARAMS[symbol]['window']:
        historical_data[symbol].pop(0)
    historical_data[symbol].append(bar)

    # Execute the appropriate strategy
    if STRATEGY_PARAMS[symbol]['strategy'] == 'ema_adx':
        execute_ema_adx(symbol, latest_price)
    elif STRATEGY_PARAMS[symbol]['strategy'] == 'bollinger_bands':
        execute_bollinger_bands(symbol, latest_price)

# Main function to start the stream
async def main():
    try:
        for symbol in STRATEGY_PARAMS.keys():
            logging.info(f"Subscribing to trade updates for {symbol}")
            stream.subscribe_trades(trade_callback, symbol)
        
        # Run the stream without asyncio.run() to avoid conflicts with the existing event loop
        await stream._run_forever()
    except Exception as e:
        logging.error(f"Error during stream execution: {e}")
    finally:
        await stream.close()

if __name__ == '__main__':
    # Run the event loop manually to avoid issues with asyncio.run()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
