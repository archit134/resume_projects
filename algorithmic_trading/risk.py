import asyncio
import logging
from alpaca_trade_api.stream import Stream
from alpaca_trade_api.rest import REST, APIError
from decouple import config
import talib
import numpy as np

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

# Risk management parameters
RISK_MANAGEMENT_PARAMS = {
    'max_position_size': 10000,  # Maximum dollars to risk in any one position
    'stop_loss_pct': 0.02,  # Stop loss at 2% below the entry price
    'take_profit_pct': 0.05,  # Take profit at 5% above the entry price
    'var_confidence_level': 0.95  # Confidence level for VaR calculation
}

# Store historical bars for strategy calculation
historical_data = {symbol: [] for symbol in STRATEGY_PARAMS.keys()}

# Track positions and orders
open_positions = {}
active_orders = {}

# Function to calculate Historical VaR at a specified confidence level
def calculate_historical_var(symbol, confidence_level=0.95):
    if len(historical_data[symbol]) < 100:  # Ensure enough data points for VaR calculation
        return None
    
    close_prices = [bar['c'] for bar in historical_data[symbol]]
    returns = np.diff(np.log(close_prices))  # Calculate logarithmic returns
    var = np.percentile(returns, (1 - confidence_level) * 100)
    
    # Convert VaR back to a monetary value
    last_price = close_prices[-1]
    var_value = last_price * abs(var)
    return var_value

# Function to place an order with risk management and event handling, including VaR check
def place_order_with_var(symbol, qty, side, entry_price):
    # Calculate VaR for the asset
    var_value = calculate_historical_var(symbol, RISK_MANAGEMENT_PARAMS['var_confidence_level'])
    if var_value is None:
        logging.warning(f"Not enough data to calculate VaR for {symbol}. Order not placed.")
        return

    # Check if the calculated VaR exceeds a risk threshold
    if var_value > RISK_MANAGEMENT_PARAMS['max_position_size']:
        logging.warning(f"VaR for {symbol} exceeds risk threshold. Order not placed.")
        return

    # Check existing position size
    position_size = open_positions.get(symbol, 0)
    if position_size + (qty * entry_price) > RISK_MANAGEMENT_PARAMS['max_position_size']:
        logging.warning(f"Position size for {symbol} exceeds maximum allowed. Order not placed.")
        return

    # Place the order if VaR is within acceptable limits
    place_order(symbol, qty, side, entry_price)

# Function to place an order (simplified)
def place_order(symbol, qty, side, entry_price):
    try:
        order = rest_api.submit_order(
            symbol=symbol,
            qty=qty,
            side=side,
            type='market',
            time_in_force='gtc'
        )
        active_orders[order.id] = order
        logging.info(f"Order placed: {side} {qty} shares of {symbol} at {entry_price}")

        # Reconfirm trade after order is filled
        asyncio.run(reconfirm_trade(order, symbol, entry_price, side, qty))

    except APIError as e:
        logging.error(f"API Error placing order for {symbol}: {e}")
    except Exception as e:
        logging.error(f"Error placing order for {symbol}: {e}")

# Function to reconfirm a trade and track positions
async def reconfirm_trade(order, symbol, entry_price, side, qty):
    try:
        while True:
            # Check the order status
            updated_order = rest_api.get_order(order.id)
            if updated_order.filled_qty == updated_order.qty:
                logging.info(f"Order fully filled for {symbol}.")
                update_positions(symbol, side, qty, entry_price)
                break
            elif updated_order.status == 'canceled':
                logging.warning(f"Order for {symbol} was canceled.")
                break
            await asyncio.sleep(1)  # Retry every second
    except APIError as e:
        logging.error(f"API Error reconfirming trade for {symbol}: {e}")

# Update the open positions based on the order filled
def update_positions(symbol, side, qty, entry_price):
    if side == 'buy':
        open_positions[symbol] = open_positions.get(symbol, 0) + (qty * entry_price)
    elif side == 'sell':
        open_positions[symbol] = max(0, open_positions.get(symbol, 0) - (qty * entry_price))
    logging.info(f"Updated positions for {symbol}: {open_positions[symbol]}")

# Basic market data validation
def validate_market_data(latest_price, symbol):
    if latest_price <= 0 or np.isnan(latest_price):
        logging.warning(f"Invalid price data for {symbol}: {latest_price}")
        return False
    return True

# EMA-ADX strategy execution
def execute_ema_adx(symbol, latest_price):
    if not validate_market_data(latest_price, symbol):
        return

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
            place_order_with_var(symbol, 1, 'buy', latest_price)
        elif latest_price < ema:
            place_order_with_var(symbol, 1, 'sell', latest_price)

# Bollinger Bands strategy execution
def execute_bollinger_bands(symbol, latest_price):
    if not validate_market_data(latest_price, symbol):
        return

    params = STRATEGY_PARAMS[symbol]
    if len(historical_data[symbol]) < params['window']:
        return  # Not enough data to calculate indicators

    close_prices = [bar['c'] for bar in historical_data[symbol]]

    upper_band, middle_band, lower_band = talib.BBANDS(
        close_prices, timeperiod=params['window'], nbdevup=params['num_std_dev'], nbdevdn=params['num_std_dev'], matype=0)

    if latest_price < lower_band[-1]:
        place_order_with_var(symbol, 1, 'buy', latest_price)
    elif latest_price > upper_band[-1]:
        place_order_with_var(symbol, 1, 'sell', latest_price)

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
