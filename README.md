# Stock Data Aggregator

A Go application that processes stock market data and generates time-based aggregations.

## Overview

This application reads stock market data from CSV files and aggregates the data into specified time intervals (1-minute and 5-minute periods). For each interval, it calculates:

- Opening price
- Closing price
- Highest price
- Lowest price
- Total volume

## Usage

1. Place your stock data CSV file in the ./stock_data directory
2. The input CSV should have the following format:
   - Symbol
   - Timestamp (Format like: 2006-01-02T15:04:05Z)
   - Price
   - Volume

3. Run the application:
   ```
   go run ./main/main.go
   ```

4. The application will generate output files with aggregated data:
   - *_1_Min.csv for 1-minute aggregations
   - *_5_Min.csv for 5-minute aggregations

## Features

- Concurrent processing using goroutines
- Efficient data aggregation
- Support for multiple time intervals
- Error handling and reporting
- CSV input/output

## Project Structure

- /main - Contains the main application logic
- /aggregator - Handles data aggregation logic
- /models - Defines data structures
- /stock_data - Directory for input data files

## Requirements

- Go 1.x
- CSV formatted stock data file
