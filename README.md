# Polygon Historical Data Downloader

This script downloads historical stock data from the Polygon.io API, compresses it using LZO compression, and uploads it to an S3 bucket with appropriate naming conventions and tagging.

## Features

- Downloads minute-level historical stock data for specified tickers
- Handles API rate limiting with automatic retry logic
- Compresses data using LZO compression
- Uploads compressed data to S3 with structured paths and metadata tags
- Supports both batch processing (via CSV) and command line ticker specification

## Prerequisites

### Required Software

- Python 3.6 or higher
- `lzop` command-line tool:
    - Ubuntu/Debian: `sudo apt-get install lzop`
    - macOS: `brew install lzop`
    - Windows: Use WSL or download from appropriate source

### Required Python Packages

```shell script
pip install pandas polygon-api-client boto3 python-dotenv
```

### API Keys and Credentials

You'll need to create a `.env` file in the same directory as the script with the following variables:

```
POLYGON_API_KEY=your_polygon_api_key
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
```

### CSV File Format (Optional)

If you want to process multiple tickers from a CSV file, create a file named `tickers.csv` with the following format:

```
ticker
AAPL
MSFT
GOOGL
...
```

## Usage

### Process Tickers from CSV File

```shell script
python main.py
```

### Process Specific Tickers via Command Line

```shell script
python main.py --tickers AAPL MSFT GOOGL
```

## Output

The script creates two types of output:

1. **Local CSV files**: Stored in the `output/` directory with the naming format `{ticker}_historical.csv`
2. **S3 uploads**: Compressed files uploaded to the S3 bucket with the following path structure:
```
s3://mochi-tickdata-historical/{asset_type}/{ticker}/{source}/{YYYY}/{MM}/{DD}/{HH}/{ticker}_{source}_{YYYYMMDDHHmm}.csv.lzo
```

## S3 Metadata

Each file uploaded to S3 includes the following tags:

- `asset_type`: Type of asset (e.g., 'stocks')
- `symbol`: The ticker symbol
- `source`: Data source (e.g., 'polygon')
- `exchange`: Stock exchange (e.g., 'NASDAQ')
- `currency`: Currency of the price data (e.g., 'USD')
- `timeframe`: Time interval of the data (e.g., '1min')
- `date`: Date of data collection (YYYY-MM-DD format)
- `quality`: Data quality indicator (e.g., 'raw')

## Data Cleanup

The script automatically removes temporary compressed files after they've been successfully uploaded to S3.

## Customization

You can customize the date range by modifying the `from_date` and `to_date` variables in the `main()` function.

## Error Handling

The script includes robust error handling for:
- API rate limiting
- API fetch failures
- Compression errors
- S3 upload issues

## License

GNU General Public License v3 (GPL-3)
