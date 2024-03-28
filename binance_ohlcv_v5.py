import websocket
import json
import pandas as pd
import signal
import sys

# Initializing global variables for balances and trade parameters.
initial_usd_balance = 10000  # Starting balance in USD
initial_btc_balance = 1  # Starting balance in BTC
buy_trade_percentage = 0.1  # Percentage of portfolio to buy
sell_trade_percentage = 0.1  # Percentage of portfolio to sell
leverage = 1  # Leverage used in trades
last_trade_signal = 0  # Tracks the last signal that triggered a trade

# Initializing an empty DataFrame to store incoming data with initial balances.
df = pd.DataFrame(columns=['timestamp', 'pair', 'c', 'v'])

def calculate_moving_averages(df, periods):
    for period in periods:
        df[f'ma{period}'] = (
            df['c'].astype(float)
            .rolling(window=period, min_periods=period)
            .mean()
            .round(3)
        )

def calculate_crossover(df, short_period, long_period):
    calculate_moving_averages(df, [short_period, long_period])
    
    short_ma = f'ma{short_period}'
    long_ma = f'ma{long_period}'
    
    prev_short_ma = df[short_ma].shift(1)
    prev_long_ma = df[long_ma].shift(1)
    
    df['cross'] = 0
    df.loc[(df[short_ma] > df[long_ma]) & (prev_short_ma <= prev_long_ma), 'cross'] = 1
    df.loc[(df[short_ma] < df[long_ma]) & (prev_short_ma >= prev_long_ma), 'cross'] = -1
    
    df['cross'] = df['cross'].astype(int)

def calculate_portfolio_balance(row):
    global initial_usd_balance, initial_btc_balance, last_trade_signal
    current_signal = row['cross']

    if current_signal in [1, -1] and current_signal != last_trade_signal:
        if current_signal == 1:  # Buy signal
            buy_amount_usd = initial_usd_balance * buy_trade_percentage
            btc_bought = (buy_amount_usd / row['c']) * leverage
            initial_usd_balance -= buy_amount_usd
            initial_btc_balance += btc_bought
        elif current_signal == -1:  # Sell signal
            btc_sold = initial_btc_balance * sell_trade_percentage
            usd_earned = (btc_sold * row['c']) * leverage
            initial_usd_balance += usd_earned
            initial_btc_balance -= btc_sold
        last_trade_signal = current_signal  # Update the last trade signal

    return initial_usd_balance, initial_btc_balance, initial_usd_balance + (initial_btc_balance * row['c'])

def on_message(ws, message):
    global df, last_trade_signal
    message_data = json.loads(message)
    data = {
        'timestamp': message_data['k']['t'],
        'pair': message_data['s'],
        'c': float(message_data['k']['c']),
        'v': message_data['k']['v'],
    }
    new_row = pd.DataFrame([data])
    df = pd.concat([df, new_row], ignore_index=True)

    calculate_crossover(df, ma_periods[0], ma_periods[1])

    df[['usd_balance', 'btc_balance', 'total_portfolio_value']] = df.apply(calculate_portfolio_balance, axis=1, result_type='expand')
    
    if not df.empty:
        print(df.iloc[-1:].to_string(index=False, header=df.index[-1]==0))

def on_error(ws, error):
    print(error)

def on_close(ws, *args, **kwargs):
    global df
    print("Connection closed")
    csv_file_path = "C:/Users/Reilly Decker/Desktop/websocket_test.csv"
    df.to_csv(csv_file_path, index=False)
    print(f"DataFrame saved to {csv_file_path}")

def on_open(ws):
    global last_trade_signal
    last_trade_signal = 0  # Reset last_trade_signal when the websocket connection opens
    print("Connection opened")

def subscribe_to_stream(symbol, interval):
    ws_url = f"wss://stream.binancefuture.com/ws/{symbol.lower()}@kline_{interval}"
    ws = websocket.WebSocketApp(ws_url, on_message=on_message, on_error=on_error, on_close=on_close)
    ws.on_open = on_open
    return ws

def signal_handler(sig, frame):
    print('Interrupt received, stopping…')
    ws.close()
    sys.exit(0)

if __name__ == "__main__":
    symbol = "BTCUSDT"
    interval = "1m"
    ma_periods = [5, 10]  # Specify moving average periods

    print(f"Subscribing to {symbol} with interval {interval}")
    ws = subscribe_to_stream(symbol, interval)
    signal.signal(signal.SIGINT, signal_handler)
    ws.run_forever()
