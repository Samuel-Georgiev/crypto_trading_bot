import websocket
import json
import pandas as pd
import signal
import sys

# Initializing an empty DataFrame to store incoming data.
df = pd.DataFrame(columns=['timestamp', 'pair', 'o', 'h', 'l', 'c', 'v'])

def calculate_moving_averages(df, periods):
    """Calculates moving averages for specified periods and adds them as new columns."""
    for period in periods:
        # Calculate the moving average, set min_periods=period to ensure calculation only with enough data,
        # and round to 3 decimal places.
        df[f'MA_{period}'] = (
            df['c'].astype(float)
            .rolling(window=period, min_periods=period)
            .mean()
            .round(3)  # Round the result to 3 decimal places
        )

def calculate_ma_crossover(df, short_period, long_period):
    """Calculates moving average crossover and adds a 'ma_cross' column to the DataFrame."""
    # Ensure moving averages are calculated and present in the DataFrame
    calculate_moving_averages(df, [short_period, long_period])
    
    short_ma = f'MA_{short_period}'
    long_ma = f'MA_{long_period}'
    
    # Shift the MAs to align with previous row for comparison
    prev_short_ma = df[short_ma].shift(1)
    prev_long_ma = df[long_ma].shift(1)
    
    # Calculate crossover: 1 for short MA crossing above long MA, -1 for crossing below, 0 otherwise
    df['ma_cross'] = 0  # Default to 0
    df.loc[(df[short_ma] > df[long_ma]) & (prev_short_ma <= prev_long_ma), 'ma_cross'] = 1
    df.loc[(df[short_ma] < df[long_ma]) & (prev_short_ma >= prev_long_ma), 'ma_cross'] = -1
    
    # Ensure the 'ma_cross' column is of integer type
    df['ma_cross'] = df['ma_cross'].astype(int)

def on_message(ws, message):
    """Handles incoming WebSocket messages by appending them to the DataFrame."""
    global df
    message_data = json.loads(message)
    data = {
        'timestamp': message_data['k']['t'],
        'pair': message_data['s'],
        'o': message_data['k']['o'],
        'h': message_data['k']['h'],
        'l': message_data['k']['l'],
        'c': float(message_data['k']['c']),  # Convert close price to float
        'v': message_data['k']['v'],
    }
    new_row = pd.DataFrame([data])
    df = pd.concat([df, new_row], ignore_index=True)

    # Calculate moving averages and crossover after adding the new row.
    short_ma_period, long_ma_period = ma_periods  # Assuming ma_periods is sorted: [short, long]
    calculate_ma_crossover(df, short_ma_period, long_ma_period)
    
    # Print the latest row in the desired format, including MAs and ma_cross
    if not df.empty:
        print(df.iloc[-1:].to_string(index=False, header=df.index[-1]==0))

def on_error(ws, error):
    print(error)

def on_close(ws, *args, **kwargs):
    global df
    print("Connection closed")
    csv_file_path = "C:/Users/Reilly Decker/Desktop/websocket_test.xlsx"
    df.to_excel(csv_file_path, index=False)
    print(f"DataFrame saved to {csv_file_path}")

def on_open(ws):
    print("Connection opened")

def subscribe_to_stream(symbol, interval):
    ws_url = f"wss://stream.binancefuture.com/ws/{symbol.lower()}@kline_{interval}"
    ws = websocket.WebSocketApp(ws_url,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    return ws

def signal_handler(sig, frame):
    print('Interrupt received, stopping…')
    ws.close()
    sys.exit(0)


# All inputs after this statement 
if __name__ == "__main__":

    symbol = "BTCUSDT"
    interval = "1m"
    # Define periods for moving averages calculation. The first is considered the short MA, the second the long MA.
    ma_periods = [5, 10]  # Example periods for MAs
    
    print(f"Subscribing to {symbol} with interval {interval}")
    ws = subscribe_to_stream(symbol, interval)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    ws.run_forever()
