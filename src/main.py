import os
import time
import pandas as pd
import boto3
import subprocess
import argparse
from datetime import datetime
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
    """
    Fetch historical aggregates (bars) data for the given ticker between from_date and to_date.
    Handles responses that might be either a dict (with a "results" key) or a list of Agg objects.
    If a 429 error (rate limit) is encountered, the function will wait before retrying.
    """
    attempts = 0
    while attempts < max_retries:
        try:
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
                print(f"Rate limit hit for {ticker}. Waiting {wait_time} seconds before retrying...")
                time.sleep(wait_time)
                attempts += 1
            else:
                print(f"Error fetching data for {ticker}: {e}")
                return None
    print(f"Exceeded maximum retries for {ticker}.")
    return None


def compress_and_upload_to_s3(file_path, ticker, source='polygon', asset_type='stocks', exchange='NASDAQ',
                              currency='USD', timeframe='1min', quality='raw'):
    """
    Compress the CSV file using LZO (via lzop command-line tool) and upload it to S3
    with proper naming convention and tags.
    """
    try:
        # Create compressed file path
        compressed_file_path = file_path + '.lzo'

        # Compress using lzop command-line tool
        print(f"Compressing {file_path} to {compressed_file_path}...")
        subprocess.run(["lzop", "-o", compressed_file_path, file_path], check=True)

        # Get current date and time for file path construction
        now = datetime.now()
        date_str = now.strftime('%Y-%m-%d')
        year = now.strftime('%Y')
        month = now.strftime('%m')
        day = now.strftime('%d')
        hour = now.strftime('%H')
        datetime_str = now.strftime('%Y%m%d%H%M')

        # Construct the S3 key (path)
        s3_key = f"{asset_type}/{ticker}/{source}/{year}/{month}/{day}/{hour}/{ticker}_{source}_{datetime_str}.csv.lzo"

        # Upload the file to S3
        print(f"Uploading {compressed_file_path} to S3...")
        s3_bucket = 'mochi-tickdata-historical'
        s3_client.upload_file(
            compressed_file_path,
            s3_bucket,
            s3_key,
            ExtraArgs={
                'Tagging': f"asset_type={asset_type}&symbol={ticker}&source={source}&exchange={exchange}&"
                           f"currency={currency}&timeframe={timeframe}&date={date_str}&quality={quality}"
            }
        )

        print(f"Successfully uploaded {ticker} data to s3://{s3_bucket}/{s3_key}")

        # Clean up temporary compressed file
        os.remove(compressed_file_path)

    except subprocess.CalledProcessError as e:
        print(f"Error compressing file {file_path}: {e}")
    except Exception as e:
        print(f"Error uploading {ticker} data to S3: {e}")


def get_tickers_from_args():
    """
    Parse command line arguments for tickers.
    Returns a list of tickers if provided, otherwise None.
    """
    parser = argparse.ArgumentParser(description='Fetch, compress, and upload stock data to S3.')
    parser.add_argument('--tickers', nargs='+', help='List of ticker symbols to process')
    args = parser.parse_args()
    return args.tickers


def main():
    # Check for command line arguments first
    cmd_tickers = get_tickers_from_args()

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

    # Define the historical date range you want to query.
    from_date = "2023-03-15"
    to_date = "2023-03-20"

    for ticker in tickers:
        print(f"Fetching minute data for {ticker}...")
        data = get_historical_data(ticker, from_date, to_date)

        if data:
            # Convert the list of Agg objects or dict results to a pandas DataFrame.
            df = pd.DataFrame(data)
            output_filename = os.path.join(output_dir, f"{ticker}_historical.csv")
            df.to_csv(output_filename, index=False)
            print(f"Saved data for {ticker} to {output_filename}")

            # Compress and upload the file to S3
            compress_and_upload_to_s3(
                output_filename,
                ticker,
                source='polygon',
                asset_type='stocks',
                exchange='NASDAQ',  # You might want to get this from your data
                currency='USD',
                timeframe='1min',
                quality='raw'
            )
        else:
            print(f"No data returned for {ticker}.")


if __name__ == "__main__":
    main()
