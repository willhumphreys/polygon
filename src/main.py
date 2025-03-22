import argparse
import os
import subprocess
import time

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


def get_historical_data(ticker, from_date, to_date, multiplier=1, timespan='minute', max_retries=3):
    attempts = 0
    endpoint = f"aggs/{ticker}/range/{multiplier}/{timespan}/{from_date}/{to_date}"  # Log the endpoint pattern
    # endpoint = f"v2/aggs/ticker/AAPL/range/1/minute/2023-03-15/2023-03-20"  # Log the endpoint pattern

    while attempts < max_retries:
        try:
            print(f"Calling endpoint: {endpoint}")  # Log the endpoint being called
            response = client.get_aggs(ticker, multiplier, timespan, from_date, to_date)

            # If the response is a list, return it directly.
            if isinstance(response, list):
                return response

            # If the response is a dictionary with a "results" key, return the contents.
            elif isinstance(response, dict) and "results" in response:
                return response["results"]

            else:
                print(f"No data returned for {ticker}. Response: {response}")
                return None

        except Exception as e:
            error_str = str(e)
            if "429" in error_str:
                wait_time = 60  # wait time in seconds
                print(
                    f"Rate limit hit for {ticker} at endpoint {endpoint}. Waiting {wait_time} seconds before retrying...")
                time.sleep(wait_time)
                attempts += 1
            else:
                print(f"Error fetching data for {ticker} at endpoint {endpoint}: {e}")
                # Throw an error by raising an exception
                raise RuntimeError(f"Failed to fetch data for {ticker}: {e}")

    print(f"Exceeded maximum retries for {ticker} at endpoint {endpoint}.")
    raise RuntimeError(f"Exceeded maximum retries ({max_retries}) for {ticker}")


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
    data = get_historical_data(ticker, from_date, to_date, multiplier, timespan)
    if data:
        # Convert the list of Agg objects or dict results to a pandas DataFrame.
        df = pd.DataFrame(data)
        output_filename = os.path.join(output_dir, f"{ticker}_{timespan}_historical.csv")
        df.to_csv(output_filename, index=False)
        print(f"Saved data for {ticker} to {output_filename}")

        # Compress and upload the file to S3
        compress_and_upload_to_s3(output_filename, ticker, metadata=ticker_metadata, source='polygon', s3_key=s3_key)
    else:
        throws = f"No data returned for {ticker}."

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
