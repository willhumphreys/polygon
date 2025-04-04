import argparse
import os
import subprocess
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


import time
import random
import datetime
import sys
import logging

# Configure logging for synchronized output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('polygon_data_fetcher')


def get_historical_data(ticker, from_date, to_date, multiplier=1, timespan='minute'):
    endpoint = f"aggs/{ticker}/range/{multiplier}/{timespan}/{from_date}/{to_date}"
    output_filename = os.path.join(output_dir, f"{ticker}_{timespan}_historical.csv")
    row_count = 0
    log_interval = 5000  # Log every 5000 records

    try:
        logger.info(f"Calling endpoint: {endpoint}")

        # Create the CSV file
        with open(output_filename, 'w') as f:
            pass

        # Process data and write directly to CSV as we receive it
        first_record = True

        # Parameters for exponential backoff
        max_retries = 5
        base_wait_time = 15  # Start with 15 seconds

        retry_count = 0
        data_fetch_started = False

        while True:
            try:
                for a in client.list_aggs(
                        ticker,
                        multiplier,
                        timespan,
                        from_date,
                        to_date,
                        limit=50000,  # Max results per page
                ):
                    # Mark that we've started fetching data
                    if not data_fetch_started:
                        data_fetch_started = True

                    # Convert the single aggregation to a DataFrame and write immediately
                    df = pd.DataFrame([a])
                    df.to_csv(output_filename, mode='a', header=first_record, index=False)

                    if first_record:
                        first_record = False

                    row_count += 1

                    # Log progress at intervals
                    if row_count % log_interval == 0:
                        logger.info(f"Processing {ticker}: {row_count} records retrieved...")

                # If we got here without exception, we're done
                break

            except Exception as e:
                error_str = str(e)
                if "429" in error_str and retry_count < max_retries:
                    retry_count += 1
                    # Calculate wait time with exponential backoff and jitter
                    wait_time = base_wait_time * (2 ** (retry_count - 1)) * (1 + random.random() * 0.2)

                    logger.info(f"Rate limit hit (429 error). Retry attempt {retry_count}/{max_retries}.")
                    logger.info(f"Backing off for {wait_time:.2f} seconds...")

                    # Force flush all handlers to ensure logs are written
                    for handler in logger.handlers:
                        handler.flush()

                    # Actually sleep for the calculated time
                    time.sleep(wait_time)

                    # Log the resumption with a status message
                    if data_fetch_started:
                        resume_message = f"Resuming data fetch for {ticker} at record {row_count}"
                    else:
                        resume_message = f"Attempting to start data fetch for {ticker} again"

                    logger.info(f"{resume_message} after {wait_time:.2f} seconds backoff")
                else:
                    # Re-raise if it's not a 429 error or we've exceeded max retries
                    raise

        if row_count > 0:
            logger.info(f"Retrieved and saved {row_count} results for {ticker} from {from_date} to {to_date}")
            return output_filename  # Return the filename instead of the data
        else:
            logger.info(f"No data returned for {ticker}.")
            raise RuntimeError(f"No data returned for {ticker}.")

    except Exception as e:
        error_str = str(e)
        if "429" in error_str:
            logger.error(f"Rate limit hit for {ticker} at endpoint {endpoint} after {max_retries} retries.")
            raise RuntimeError(f"Rate limit hit for {ticker}. Please wait before retrying.")
        else:
            logger.error(f"Error fetching data for {ticker} at endpoint {endpoint}: {e}")
            raise RuntimeError(f"Failed to fetch data for {ticker}: {e}")


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
            raise RuntimeError(f"Error compressing file: {compression_result.stderr}")

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
        print(f"Uploading {compressed_file_path} to S3 bucket {s3_bucket} at {s3_key} with tags: {tags}")
        s3_client.upload_file(compressed_file_path, s3_bucket, s3_key)
        print(f"File uploaded successfully to S3: s3://{s3_bucket}/{s3_key}")

        # Remove the compressed file after upload
        os.remove(compressed_file_path)
        print(f"Removed temporary compressed file {compressed_file_path}")

    except Exception as e:
        raise RuntimeError(f"Error in compress_and_upload_to_s3: {e}")


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

        fetch_from_polygon(from_date, s3_key_1hour, ticker, ticker_metadata, to_date, 1, 'hour')
        fetch_from_polygon(from_date, s3_key_1day, ticker, ticker_metadata, to_date, 1, 'day')
        fetch_from_polygon(from_date, s3_key_1min, ticker, ticker_metadata, to_date, 1, 'minute')


def fetch_from_polygon(from_date, s3_key, ticker, ticker_metadata, to_date, multiplier, timespan='minute'):
    output_filename = get_historical_data(ticker, from_date, to_date, multiplier, timespan)
    if output_filename:
        compress_and_upload_to_s3(output_filename, ticker, metadata=ticker_metadata,
                                  source='polygon', s3_key=s3_key)
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
