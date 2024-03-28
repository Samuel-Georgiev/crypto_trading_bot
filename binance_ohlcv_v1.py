import websocket
import json
import pandas as pd
import signal
import sys

# Initializing an empty DataFrame to store incoming data.
df = pd.DataFrame(columns=['timestamp', 'pair', 'o', 'h', 'l', 'c', 'v'])


# Handles incoming WebSocket messages by appending them to the DataFrame.
def on_message(ws, message):
    global df
    message_data = json.loads(message)

    data = {
        'timestamp': [message_data['k']['t']],
        'pair': [message_data['s']],
        'o': [message_data['k']['o']],
        'h': [message_data['k']['h']],
        'l': [message_data['k']['l']],
        'c': [message_data['k']['c']],
        'v': [message_data['k']['v']],
    }
    new_row = pd.DataFrame(data)
    if df.empty:
        # If the DataFrame is empty, print column names with the first row.
        print(new_row.to_string(index=False, header=True))
    else:
        # For subsequent rows, print without column names or an index.
        print(new_row.to_string(index=False, header=False))
    df = pd.concat([df, new_row], ignore_index=True)

# Handles errors by printing them.
def on_error(ws, error):
    print(error)


# Handles the closing of the WebSocket connection.
def on_close(ws, *args, **kwargs):
    global df
    print("Connection closed")
    csv_file_path = r"C:/Users/Reilly Decker/Desktop/websocket_test_v1.csv"
    df.to_csv(csv_file_path, index=False)
    print(f"DataFrame saved to {csv_file_path}")


# Notifies when the WebSocket connection is successfully opened.
def on_open(ws):
    print("Connection opened")


# Subscribes to a specific Binance kline stream based on symbol and interval.
def subscribe_to_stream(symbol, interval):
    ws_url = f"wss://stream.binancefuture.com/ws/{symbol.lower()}@kline_{interval}"
    ws = websocket.WebSocketApp(ws_url,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    return ws


# Handles keyboard interrupt for graceful termination.
def signal_handler(sig, frame):
    print('Interrupt received, stopping…')
    # Gracefully closing the WebSocket connection before exiting.
    ws.close()
    sys.exit(0)

if __name__ == "__main__":
    # Define the cryptocurrency pair symbol and candle interval.
    symbol = "BTCUSDT"
    interval = "1m"
    print(f"Subscribing to {symbol} with interval {interval}")
    
    ws = subscribe_to_stream(symbol, interval)
    
    # Setup a signal handler for graceful termination with Ctrl+C.
    # This allows for the subscription to be ended manually,
    # saving the accumulated DataFrame to a CSV file.
    signal.signal(signal.SIGINT, signal_handler)
    
    ws.run_forever()
