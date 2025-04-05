import os
import pandas as pd
import time
import random
import boto3
import requests
import argparse
import logging
import datetime
from io import BytesIO

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


def compress_and_upload_to_s3(file_path, bucket_name, object_key=None):
    """
    Compresses a file using LZO compression and uploads it to an S3 bucket.

    Args:
        file_path (str): Path to the local file to compress and upload
        bucket_name (str): Name of the S3 bucket
        object_key (str, optional): S3 object key. If not provided, the file name will be used

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

        # Upload the compressed file to S3
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
            # Fetch data for each timeframe
            hour_file = fetch_data_with_key(ticker, from_date, to_date, 1, 'hour')
            day_file = fetch_data_with_key(ticker, from_date, to_date, 1, 'day')
            minute_file = fetch_data_with_key(ticker, from_date, to_date, 1, 'minute')

            # Upload to S3 if bucket name is provided
            if hour_file and os.path.exists(hour_file) and s3_key_hour:
                compress_and_upload_to_s3(hour_file, bucket_name, s3_key_hour)

            if day_file and os.path.exists(day_file) and s3_key_day:
                compress_and_upload_to_s3(day_file, bucket_name, s3_key_day)

            if minute_file and os.path.exists(minute_file) and s3_key_min:
                compress_and_upload_to_s3(minute_file, bucket_name, s3_key_min)

        except Exception as e:
            logger.error(f"Error processing ticker {ticker}: {str(e)}")

    logger.info("Data processing complete")


def fetch_data_with_key(ticker, from_date, to_date, multiplier, timespan):
    """
    Fixed version of fetch_all_polygon_data that properly handles the API key during pagination
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
            # Convert the results to a DataFrame and write to CSV
            df = pd.DataFrame(data['results'])
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
