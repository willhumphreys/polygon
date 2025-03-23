import argparse
import os
import subprocess
import time
from datetime import datetime

import boto3
import pandas as pd
from dotenv import load_dotenv
from polygon import RESTClient

# Load environment variables from .env file
load_dotenv()

output_dir = "output"
# Create the directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Retrieve the API key from the environment variables
api_key = os.getenv("POLYGON_API_KEY")
if not api_key:
    raise ValueError("Please set the POLYGON_API_KEY in your .env file.")

client = RESTClient(api_key)

# Initialize S3 client
s3_client = boto3.client('s3')


def get_historical_data(ticker, from_date, to_date, multiplier=1, timespan='minute'):
    """
    Fetches historical data from Polygon.io in one-month chunks.

    Args:
        ticker: The ticker symbol
        from_date: Start date in YYYY-MM-DD format
        to_date: End date in YYYY-MM-DD format
        multiplier: Time multiplier for the timespan
        timespan: Time span for the aggregation (minute, hour, day, etc.)

    Returns:
        Path to the CSV file containing the data
    """
    output_filename = os.path.join(output_dir, f"{ticker}_{timespan}_historical.csv")
    total_rows = 0

    # Convert string dates to datetime for easier manipulation
    start_dt = datetime.strptime(from_date, '%Y-%m-%d')
    end_dt = datetime.strptime(to_date, '%Y-%m-%d')

    # Create an empty CSV file
    with open(output_filename, 'w') as f:
        pass

    first_chunk = True

    # Process one month at a time
    current_start = start_dt
    while current_start < end_dt:
        # Calculate end of current chunk (one month later or end_dt, whichever comes first)
        if current_start.month == 12:
            current_end = min(datetime(current_start.year + 1, 1, current_start.day), end_dt)
        else:
            current_end = min(datetime(current_start.year, current_start.month + 1, current_start.day), end_dt)

        # Format dates for API call
        current_start_str = current_start.strftime('%Y-%m-%d')
        current_end_str = current_end.strftime('%Y-%m-%d')

        endpoint = f"aggs/{ticker}/range/{multiplier}/{timespan}/{current_start_str}/{current_end_str}"
        chunk_rows = 0
        log_interval = 5000

        try:
            print(f"Fetching {ticker} data from {current_start_str} to {current_end_str}")

            # Collect data for this chunk
            chunk_data = []

            for a in client.list_aggs(
                    ticker,
                    multiplier,
                    timespan,
                    current_start_str,
                    current_end_str,
                    limit=50000,
            ):
                chunk_data.append(a)
                chunk_rows += 1
                total_rows += 1

                # Log progress
                if total_rows % log_interval == 0:
                    print(f"Processing {ticker}: {total_rows} records retrieved...")

            # Write chunk to CSV
            if chunk_data:
                df = pd.DataFrame(chunk_data)
                df.to_csv(output_filename, mode='a', header=first_chunk, index=False)
                first_chunk = False

            print(f"Retrieved {chunk_rows} records for {ticker} from {current_start_str} to {current_end_str}")

            # Move to next month
            current_start = current_end

            # Add a small delay to avoid hitting rate limits
            time.sleep(0.5)

        except Exception as e:
            error_str = str(e)
            if "429" in error_str:
                print(f"Rate limit hit for {ticker} at endpoint {endpoint}.")
                raise RuntimeError(f"Rate limit hit for {ticker}. Please wait before retrying.")
            else:
                print(f"Error fetching data for {ticker} at endpoint {endpoint}: {e}")
                raise RuntimeError(f"Failed to fetch data for {ticker}: {e}")

    if total_rows > 0:
        print(f"Retrieved and saved a total of {total_rows} results for {ticker} from {from_date} to {to_date}")
        return output_filename
    else:
        print(f"No data returned for {ticker}.")
        return None


def compress_and_upload_to_s3(file_path, ticker, metadata, s3_key, source='polygon', timeframe='1min',
                              quality='raw'):
    """
    Compress a CSV file using lzop and upload it to S3 with appropriate metadata tags
    """
    try:
        # Get the base filename without path
        file_name = os.path.basename(file_path)

        # Create the compressed file name
        compressed_file_path = f"{file_path}.lzo"

        # Compress the file using lzop
        print(f"Compressing {file_path}...")
        compression_result = subprocess.run(['lzop', '-f', file_path, '-o', compressed_file_path], capture_output=True,
                                            text=True)

        if compression_result.returncode != 0:
            print(f"Error compressing file: {compression_result.stderr}")
            return

        print(f"File compressed successfully to {compressed_file_path}")

        s3_bucket = os.getenv("OUTPUT_BUCKET_NAME", "mochi-prod-raw-historical-data")

        # Build a comprehensive tag string from metadata
        tag_parts = [f"symbol={ticker}", f"source={source}", f"timeframe={timeframe}", f"quality={quality}"]

        # Add optional name tag if available
        if 'name' in metadata:
            tag_parts.append(f"name={metadata['name']}")

        # Join all tags
        tags = "&".join(tag_parts)

        # Upload file to S3 with tags
        print(f"Uploading {compressed_file_path} to S3 bucket {s3_bucket} at {s3_key}...")
        s3_client.upload_file(compressed_file_path, s3_bucket, s3_key, ExtraArgs={'Tagging': tags})
        print(f"File uploaded successfully to S3: s3://{s3_bucket}/{s3_key}")

        # Remove the compressed file after upload
        os.remove(compressed_file_path)
        print(f"Removed temporary compressed file {compressed_file_path}")

    except Exception as e:
        print(f"Error in compress_and_upload_to_s3: {e}")


def get_tickers_from_args():
    """
    Parse command line arguments for tickers.
    Returns a list of tickers if provided, otherwise None.
    """
    parser = argparse.ArgumentParser(description='Fetch, compress, and upload stock data to S3.')
    parser.add_argument('--tickers', nargs='+', help='List of ticker symbols to process')
    parser.add_argument('--s3_key_min', required=True, help='Path in S3 where files will be uploaded')
    parser.add_argument('--s3_key_hour', required=True, help='Path in S3 where files will be uploaded')
    parser.add_argument('--s3_key_day', required=True, help='Path in S3 where files will be uploaded')
    parser.add_argument('--from_date', required=True, help='Start date in format YYYY-MM-DD')
    parser.add_argument('--to_date', required=True, help='End date in format YYYY-MM-DD')
    args = parser.parse_args()
    return args.tickers, args.s3_key_min, args.s3_key_hour, args.s3_key_day, args.from_date, args.to_date


def main():
    # Check for command line arguments first
    cmd_tickers, s3_key_1min, s3_key_1hour, s3_key_1day, from_date, to_date = get_tickers_from_args()

    if cmd_tickers:
        print(f"Using tickers from command line arguments: {cmd_tickers}")
        tickers = cmd_tickers
    else:
        # If no command line tickers, load from CSV file
        print("No tickers provided via command line, loading from tickers.csv...")
        try:
            tickers_df = pd.read_csv("tickers.csv")
            tickers = tickers_df['ticker'].tolist()
            print(f"Loaded {len(tickers)} tickers from CSV file")
        except Exception as e:
            print(f"Error loading tickers from CSV: {e}")
            return

    for ticker in tickers:
        ticker_metadata = get_ticker_metadata(ticker)

        fetch_from_polygon(from_date, s3_key_1min, ticker, ticker_metadata, to_date, 1, 'minute')
        fetch_from_polygon(from_date, s3_key_1hour, ticker, ticker_metadata, to_date, 1, 'hour')
        fetch_from_polygon(from_date, s3_key_1day, ticker, ticker_metadata, to_date, 1, 'day')


def fetch_from_polygon(from_date, s3_key, ticker, ticker_metadata, to_date, multiplier, timespan='minute'):
    output_filename = get_historical_data(ticker, from_date, to_date, multiplier, timespan)
    if output_filename:
        # Compress and upload the file to S3
        compress_and_upload_to_s3(output_filename, ticker, metadata=ticker_metadata, source='polygon', s3_key=s3_key)
    else:
        print(f"No data returned for {ticker}.")


def get_ticker_metadata(ticker):
    """
    Fetch ticker metadata from Polygon API with comprehensive handling for different asset types
    """

    # Base metadata with defaults
    metadata = {'asset_type': 'stocks', 'exchange': 'NASDAQ', 'currency': 'USD', 'name': ticker, 'market': 'stocks'}

    try:
        # Call the reference/tickers endpoint
        ticker_details = client.get_ticker_details(ticker)

        # Extract type (asset class)
        if hasattr(ticker_details, 'type'):
            asset_type = ticker_details.type.lower()
            if asset_type in ['cs', 'common_stock']:
                metadata['asset_type'] = 'stocks'
            elif asset_type in ['etf']:
                metadata['asset_type'] = 'etfs'
            elif asset_type in ['crypto']:
                metadata['asset_type'] = 'crypto'
            elif asset_type in ['fx']:
                metadata['asset_type'] = 'forex'
            else:
                metadata['asset_type'] = asset_type

        # Extract exchange
        if hasattr(ticker_details, 'primary_exchange'):
            metadata['exchange'] = ticker_details.primary_exchange

        # Extract currency
        if hasattr(ticker_details, 'currency_name'):
            metadata['currency'] = ticker_details.currency_name

        # Extract name
        if hasattr(ticker_details, 'name'):
            metadata['name'] = ticker_details.name

        # Extract market
        if hasattr(ticker_details, 'market'):
            metadata['market'] = ticker_details.market

        return metadata
    except Exception as e:
        print(f"Error fetching metadata for {ticker}: {e}")
        # Return default values if API call fails
        return metadata


if __name__ == "__main__":
    main()
