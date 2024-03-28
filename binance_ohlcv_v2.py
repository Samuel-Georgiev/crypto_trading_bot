import websocket
import json
import pandas as pd
import signal
import sys

# Initializing an empty DataFrame to store incoming data.
df = pd.DataFrame(columns=['timestamp', 'pair', 'o', 'h', 'l', 'c', 'v'])


#
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
    # Convert data to DataFrame and append
    new_row = pd.DataFrame([data])
    df = pd.concat([df, new_row], ignore_index=True)

    # Ensure 'c' is float for moving average calculations
    df['c'] = df['c'].astype(float)

    # Calculate moving averages after adding the new row.
    calculate_moving_averages(df, ma_periods)
    
    # Print the latest row in the desired format, including MAs
    if not df.empty:
        print(df.iloc[-1:].to_string(index=False, header=df.index[-1]==0))

def on_error(ws, error):
    """Handles errors by printing them."""
    print(error)

def on_close(ws, *args, **kwargs):
    """Handles the closing of the WebSocket connection."""
    global df
    print("Connection closed")
    csv_file_path = r"C:/Users/Reilly Decker/Desktop/websocket_test.csv"
    df.to_csv(csv_file_path, index=False)
    print(f"DataFrame saved to {csv_file_path}")

def on_open(ws):
    """Notifies when the WebSocket connection is successfully opened."""
    print("Connection opened")

def subscribe_to_stream(symbol, interval):
    """Subscribes to a specific Binance kline stream based on symbol and interval."""
    ws_url = f"wss://stream.binancefuture.com/ws/{symbol.lower()}@kline_{interval}"
    ws = websocket.WebSocketApp(ws_url,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    return ws

def signal_handler(sig, frame):
    """Handles keyboard interrupt for graceful termination."""
    print('Interrupt received, stopping…')
    ws.close()
    sys.exit(0)

if __name__ == "__main__":
    # Define the cryptocurrency pair symbol and candle interval.
    symbol = "BTCUSDT"
    interval = "1m"
    # Define periods for moving averages calculation.
    ma_periods = [5, 10]  # Example periods for MAs
    
    print(f"Subscribing to {symbol} with interval {interval}")
    ws = subscribe_to_stream(symbol, interval)
    
    # Setup a signal handler for graceful termination with Ctrl+C.
    signal.signal(signal.SIGINT, signal_handler)
    
    ws.run_forever()
