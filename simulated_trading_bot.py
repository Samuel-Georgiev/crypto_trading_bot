#!/usr/bin/env python
# ====================================================================
# Script Name    : simulated_trading_bot.py
# Script Version : 01
# Created        : 03/23/2024
# Modified       : 03/24/2024
# Author         : Samuel Georgiev
# Email          : samgeo.us@gmail.com
# Description    : Analyzes financial data using moving average crossover strategy.
# Notes          : Requires pandas and numpy. Assumes data in 'csv' format with 'datetime' and 'c' columns.
# Usage          : See below
#                  python simulated_trading_bot_.py your_data_file.csv
# ====================================================================

import pandas as pd
import numpy as np
import sys
import datetime
import os

# --------------------------------------------------------------------
# Reads historical price data from a CSV file
# def getHistoricalData(filePath):
# --------------------------------------------------------------------
def getData(filePath):
    if not os.path.exists(filePath):
        print(f"[ERROR] : File '{filePath}' does not exist.")
        sys.exit(1)
    else:
        print(f"[OK   ] : File '{filePath}' exist.")

    df = pd.read_csv(filePath)
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df

# --------------------------------------------------------------------


# --------------------------------------------------------------------
# Calculates and adds moving averages to the DataFrame
# def calculateMovingAverages(dataFrame, shortWindow, longWindow):
# --------------------------------------------------------------------
def calculateMovingAverages(dataFrame, shortWindow, longWindow):
    dataFrame["short_mavg"] = dataFrame["c"].rolling(window=shortWindow, min_periods=1).mean()
    dataFrame["long_mavg"] = dataFrame["c"].rolling(window=longWindow, min_periods=1).mean()

# --------------------------------------------------------------------


# --------------------------------------------------------------------
# Adds 'signal' and 'position' columns based on moving averages
# def identifySignals(dataFrame):
# --------------------------------------------------------------------
def identifySignals(dataFrame):
    dataFrame["signal"] = np.where(dataFrame["short_mavg"] > dataFrame["long_mavg"], 1, 0)
    dataFrame["position"] = dataFrame["signal"].diff()

# --------------------------------------------------------------------


# --------------------------------------------------------------------
# Starts trading based on moving average crossover signals
# def Trade(dataFrame, startingCapital):
# --------------------------------------------------------------------
def Trade(dataFrame, startingCapital):
    cash = startingCapital
    stock = 0
    dataFrame["portfolio_value"] = startingCapital

    for index, row in dataFrame.iterrows():
        if row["position"] == 1:  # Buy signal
            if cash > 0:
                stock = cash / row["c"]
                cash = 0
        elif row["position"] == -1:  # Sell signal
            if stock > 0:
                cash = stock * row["c"]
                stock = 0
        dataFrame.at[index, "portfolio_value"] = cash + stock * row["c"]

    trade_executed = dataFrame[dataFrame["position"].isin([1, -1])]
    return trade_executed

# --------------------------------------------------------------------


# --------------------------------------------------------------------
# Retrieves current date and time
# --------------------------------------------------------------------
def retrieveDateandTime():
    currentDatetime = datetime.datetime.now()
    formattedDatetime = currentDatetime.strftime("%Y-%m-%d %H:%M:%S")
    return formattedDatetime

# --------------------------------------------------------------------


# --------------------------------------------------------------------
# MAIN
# --------------------------------------------------------------------
def main():
    if len(sys.argv) != 2:
        print("[ERROR] : Please provide the path to your data file.")
        sys.exit(1)

    print("-----------------------------------------------")
    print(f"S T A R T - {retrieveDateandTime()}")
    print("-----------------------------------------------")

    dataFilePath = sys.argv[1]
    df = getData(dataFilePath)

    try:
        startingCapital = float(input("Enter the starting capital (in USD): "))
    except ValueError:
        print("[ERROR] : Invalid input for capital. Please enter a numeric value.")
        sys.exit(1)

    calculateMovingAverages(df, 13, 48)
    identifySignals(df)
    df = Trade(df, startingCapital)

    print(
        df[
            [
                "datetime",
                "c",
                "short_mavg",
                "long_mavg",
                "signal",
                "position",
                "portfolio_value",
            ]
        ]
    )

    print("-----------------------------------------------")
    print(f"D O N E - {retrieveDateandTime()}")
    print("-----------------------------------------------")

# --------------------------------------------------------------------

if __name__ == "__main__":
    main()
