import os

import pandas as pd
import time
import random
import boto3
import requests
import argparse
import logging
import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load the Polygon API key from environmental variable
polygon_api_key = os.environ.get('POLYGON_API_KEY')
if not polygon_api_key:
    raise ValueError("POLYGON_API_KEY environmental variable is not set")

# Get S3 bucket name from environment
bucket_name = os.environ.get('OUTPUT_BUCKET_NAME')
if not bucket_name:
    logger.warning("OUTPUT_BUCKET_NAME environment variable is not set, S3 upload will be skipped")

# Set the output directory for CSV files
output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
# Create the output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Get API key from environment
api_key = os.environ.get('POLYGON_API_KEY')
if not api_key:
    raise ValueError("Please set the POLYGON_API_KEY environment variable")

# Initialize S3 client
s3_client = boto3.client('s3')

def get_ticker_info(ticker, api_key):
    """
    Fetches information about a ticker from the Polygon.io reference endpoint.

    Args:
        ticker (str): The ticker symbol to fetch information for
        api_key (str): Polygon.io API key

    Returns:
        dict: Information about the ticker or None if the request fails
    """
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={api_key}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            logger.info(f"Successfully fetched info for ticker {ticker}")
            return data['results']
        else:
            logger.error(f"Failed to fetch info for ticker {ticker}: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error fetching ticker info: {str(e)}")
        return None


def compress_and_upload_to_s3(file_path, bucket_name, object_key=None, metadata=None):
    """
    Compresses a file using LZO compression and uploads it to an S3 bucket with metadata tags.

    Args:
        file_path (str): Path to the local file to compress and upload
        bucket_name (str): Name of the S3 bucket
        object_key (str, optional): S3 object key. If not provided, the file name will be used
        metadata (dict, optional): Metadata to attach to the S3 object as tags

    Returns:
        bool: True if upload was successful, False otherwise
    """
    # Check if file exists
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return False

    # If object_key is not provided, use the file name with .lzo extension
    if object_key is None:
        object_key = os.path.basename(file_path) + '.lzo'

    try:
        # Create a temporary file for the compressed data
        temp_lzo_file = file_path + '.lzo'

        # Compress the file using lzop command line tool
        compress_command = f"lzop -o {temp_lzo_file} {file_path}"
        compression_result = os.system(compress_command)

        if compression_result != 0:
            logger.error(f"Failed to compress file using lzop: {file_path}")
            return False

        # Prepare extra args for S3 upload if metadata is provided
        extra_args = {}
        if metadata:
            # Convert metadata to S3 metadata format
            # S3 metadata keys must be prefixed with 'x-amz-meta-'
            s3_metadata = {}
            for key, value in metadata.items():
                if isinstance(value, (str, int, float, bool)):
                    s3_metadata[str(key)] = str(value)

            if s3_metadata:
                extra_args['Metadata'] = s3_metadata

        # Upload the compressed file to S3
        if extra_args:
            s3_client.upload_file(
                temp_lzo_file,
                bucket_name,
                object_key,
                ExtraArgs=extra_args
            )
        else:
            s3_client.upload_file(
                temp_lzo_file,
                bucket_name,
                object_key
            )

        # Clean up the temporary file
        os.remove(temp_lzo_file)

        logger.info(f"Successfully compressed and uploaded {file_path} to {bucket_name}/{object_key}")
        return True

    except Exception as e:
        logger.error(f"Error uploading file to S3: {str(e)}")
        return False


def get_tickers_from_args():
    """
    Parse command-line arguments to get ticker symbols, date range, and S3 keys.

    Returns:
        tuple: (list of tickers, from_date, to_date, s3_key_min, s3_key_hour, s3_key_day)
    """
    parser = argparse.ArgumentParser(description='Fetch and process historical market data.')

    # Add arguments
    parser.add_argument('--tickers', '-t', required=False, help='Comma-separated list of ticker symbols')
    parser.add_argument('--file', '-f', required=False, help='Path to a file containing ticker symbols (one per line)')
    parser.add_argument('--from_date', required=False, help='Start date in YYYY-MM-DD format')
    parser.add_argument('--to_date', required=False, help='End date in YYYY-MM-DD format')
    parser.add_argument('--s3_key_min', required=False, help='S3 key for minute data')
    parser.add_argument('--s3_key_hour', required=False, help='S3 key for hour data')
    parser.add_argument('--s3_key_day', required=False, help='S3 key for day data')

    args = parser.parse_args()

    # Make sure either --tickers or --file is provided
    if not args.tickers and not args.file:
        parser.error("Either --tickers or --file must be provided")

    # Parse tickers
    tickers = []
    if args.tickers:
        tickers = [ticker.strip().upper() for ticker in args.tickers.split(',')]
    elif args.file:
        try:
            with open(args.file, 'r') as f:
                tickers = [line.strip().upper() for line in f if line.strip()]
        except FileNotFoundError:
            parser.error(f"File not found: {args.file}")

    # Handle dates
    from_date = args.from_date or (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
    to_date = args.to_date or datetime.datetime.now().strftime('%Y-%m-%d')

    return tickers, from_date, to_date, args.s3_key_min, args.s3_key_hour, args.s3_key_day


def main():
    """
    Main function to orchestrate data fetching and processing.
    """
    tickers, from_date, to_date, s3_key_min, s3_key_hour, s3_key_day = get_tickers_from_args()

    # Get S3 bucket name from environment
    bucket_name = os.environ.get('OUTPUT_BUCKET_NAME')
    if not bucket_name:
        raise ValueError("OUTPUT_BUCKET_NAME environment variable is not set. Cannot proceed without S3 bucket name.")

    logger.info(f"Processing {len(tickers)} tickers from {from_date} to {to_date}")

    # Process each ticker
    for ticker in tickers:
        logger.info(f"Processing ticker: {ticker}")

        try:
            ticker_info = get_ticker_info(ticker, api_key)

            if not ticker_info:
                logger.error(f"Could not get ticker information for {ticker}. Continuing with data fetch anyway.")

            # Determine market type
            market_type = ticker_info.get('market', '').lower() if ticker_info else None

            # Create metadata dictionary from ticker info
            metadata = None
            if ticker_info:
                metadata = {
                    'ticker': ticker,
                    'name': ticker_info.get('name', ''),
                    'market': ticker_info.get('market', ''),
                    'locale': ticker_info.get('locale', ''),
                    'primary_exchange': ticker_info.get('primary_exchange', ''),
                    'type': ticker_info.get('type', ''),
                    'currency': ticker_info.get('currency_name', '')
                }

            # Fetch data for each timeframe with market type
            hour_file = fetch_data_with_key(ticker, from_date, to_date, 1, 'hour', market_type)
            day_file = fetch_data_with_key(ticker, from_date, to_date, 1, 'day', market_type)
            minute_file = fetch_data_with_key(ticker, from_date, to_date, 1, 'minute', market_type)

            # Create simplified S3 object keys without date components
            hour_key = s3_key_hour
            day_key = s3_key_min
            minute_key = s3_key_day

            # Upload to S3 with simplified keys
            if hour_file and os.path.exists(hour_file):
                compress_and_upload_to_s3(hour_file, bucket_name, hour_key, metadata)

            if day_file and os.path.exists(day_file):
                compress_and_upload_to_s3(day_file, bucket_name, day_key, metadata)

            if minute_file and os.path.exists(minute_file):
                compress_and_upload_to_s3(minute_file, bucket_name, minute_key, metadata)

        except Exception as e:
            logger.error(f"Error processing ticker {ticker}: {str(e)}")

    logger.info("Data processing complete")


def fetch_data_with_key(ticker, from_date, to_date, multiplier, timespan, market_type=None):
    """
    Fetches data from Polygon API and formats decimal precision based on market type.

    Args:
        ticker (str): Ticker symbol
        from_date (str): Start date
        to_date (str): End date
        multiplier (int): Time multiplier
        timespan (str): Time span (minute, hour, day)
        market_type (str, optional): Market type (e.g., 'stocks', 'crypto'). Used for decimal precision.

    Returns:
        str: Path to saved CSV file or None if no data
    """
    output_filename = os.path.join(output_dir, f"{ticker}_{timespan}_historical.csv")
    base_url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from_date}/{to_date}"
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 50000,
        "apiKey": polygon_api_key
    }

    # Create empty CSV file
    with open(output_filename, 'w') as f:
        pass

    row_count = 0
    first_record = True
    current_url = base_url

    # Parameters for exponential backoff
    max_retries = 5
    base_wait_time = 15  # Start with 15 seconds

    while current_url:
        retry_count = 0
        success = False

        while not success and retry_count <= max_retries:
            try:
                # For first request use params, for subsequent requests append the API key
                if current_url == base_url:
                    response = requests.get(current_url, params=params)
                else:
                    # Ensure the API key is added to the next_url
                    if '?' in current_url:
                        modified_url = f"{current_url}&apiKey={polygon_api_key}"
                    else:
                        modified_url = f"{current_url}?apiKey={polygon_api_key}"
                    response = requests.get(modified_url)

                response.raise_for_status()
                data = response.json()
                success = True

            except requests.exceptions.RequestException as e:
                error_str = str(e)
                if "429" in error_str and retry_count < max_retries:
                    retry_count += 1
                    # Calculate wait time with exponential backoff and jitter
                    wait_time = base_wait_time * (2 ** (retry_count - 1)) * (1 + random.random() * 0.2)

                    print(f"Rate limit hit (429 error). Retry attempt {retry_count}/{max_retries}.")
                    print(f"Backing off for {wait_time:.2f} seconds...")

                    time.sleep(wait_time)

                    print(f"Resuming data fetch for {ticker} after {wait_time:.2f} seconds backoff")
                else:
                    # Re-raise if it's not a 429 error or we've exceeded max retries
                    print(f"Error fetching data: {e}")
                    return None

        if not success:
            print(f"Failed to fetch data after {max_retries} retries")
            return None

        # Process results and append to CSV
        if 'results' in data and data['results']:
            # Convert the results to a DataFrame
            df = pd.DataFrame(data['results'])

            # For stocks, set precision to 2 decimal places for price columns
            if market_type == 'stocks':
                price_columns = ['o', 'h', 'l', 'c', 'vw']
                for col in price_columns:
                    if col in df.columns:
                        df[col] = df[col].round(2)

            # Write to CSV
            df.to_csv(output_filename, mode='a', header=first_record, index=False)

            if first_record:
                first_record = False

            batch_count = len(data['results'])
            row_count += batch_count

            # Log progress
            print(f"Processing {ticker}: {row_count} records retrieved...")

            # Check if there's a next page
            current_url = data.get('next_url')

            # Add a small delay between requests to avoid rate limiting
            if current_url:
                time.sleep(0.5)
        else:
            # No more results
            current_url = None

    if row_count > 0:
        print(f"Retrieved and saved {row_count} results for {ticker} from {from_date} to {to_date}")
        return output_filename
    else:
        print(f"No data returned for {ticker}.")
        return None


if __name__ == "__main__":
    main()
