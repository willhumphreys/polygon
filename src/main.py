import os
import time
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

def main():
    # Load the CSV file containing the tickers.
    tickers_df = pd.read_csv("tickers.csv")
    tickers = tickers_df['ticker'].tolist()

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
        else:
            print(f"No data returned for {ticker}.")

if __name__ == "__main__":
    main()
