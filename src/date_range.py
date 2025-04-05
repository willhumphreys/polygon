import pandas as pd
import subprocess
import os
import tempfile


def decompress_lzo(lzo_file, output_dir):
    """Decompress an LZO file using the lzop command line tool"""
    # Create the output filename
    base_name = os.path.basename(lzo_file).replace('.lzo', '')
    output_file = os.path.join(output_dir, base_name)

    try:
        # Make sure the output directory exists
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        # Run lzop to decompress the file
        cmd = ['lzop', '-d', '-f', '-o', output_file, lzo_file]
        print(f"Running command: {' '.join(cmd)}")
        subprocess.run(cmd, check=True)

        print(f"Successfully decompressed {lzo_file} to {output_file}")
        return output_file
    except subprocess.CalledProcessError as e:
        print(f"Failed to decompress {lzo_file}: {e}")
        return None


def analyze_date_range(csv_file):
    """Analyze a CSV file to determine the date range of the data"""
    try:
        # Read the CSV file
        print(f"Reading file: {csv_file}")
        df = pd.read_csv(csv_file)

        # Check if the timestamp column exists
        if 'timestamp' not in df.columns:
            print(f"Error: 'timestamp' column not found in {csv_file}")
            print(f"Available columns: {df.columns.tolist()}")
            return None

        # Convert the timestamp (milliseconds since epoch) to datetime
        df['date'] = pd.to_datetime(df['timestamp'], unit='ms')

        # Get the min and max dates
        min_date = df['date'].min()
        max_date = df['date'].max()

        # Count the number of rows
        row_count = len(df)

        # Get the frequency (daily, hourly, minute) based on the file name
        if 'day' in csv_file:
            freq_type = "Daily"
        elif 'hour' in csv_file:
            freq_type = "Hourly"
        elif 'min' in csv_file:
            freq_type = "Minute"
        else:
            freq_type = "Unknown"

        # Return the analysis results
        return {
            'file': csv_file,
            'min_date': min_date,
            'max_date': max_date,
            'row_count': row_count,
            'frequency': freq_type
        }

    except Exception as e:
        print(f"Error analyzing {csv_file}: {e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    # List of LZO files to process
    lzo_files = [
        "MSFT/MSFT_polygon_day.csv.lzo",
        "MSFT/MSFT_polygon_hour.csv.lzo",
        "MSFT/MSFT_polygon_min.csv.lzo"
    ]

    # Get current directory
    current_dir = os.getcwd()
    print(f"Current directory: {current_dir}")

    # Create a temporary directory for the decompressed files
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Temporary directory: {temp_dir}")
        results = []

        for lzo_file in lzo_files:
            # Check if the file exists
            full_path = os.path.join(current_dir, lzo_file)
            if not os.path.exists(full_path):
                print(f"Warning: File {full_path} does not exist. Skipping.")
                continue

            print(f"Processing: {full_path}")

            # Decompress the file
            decompressed_file = decompress_lzo(full_path, temp_dir)

            if decompressed_file:
                # Analyze the decompressed file
                analysis = analyze_date_range(decompressed_file)
                if analysis:
                    results.append(analysis)

        # Print the results in a formatted table
        if results:
            print("\n" + "=" * 100)
            print(f"{'File':<25} {'Frequency':<10} {'Start Date':<25} {'End Date':<25} {'# Records':<10}")
            print("-" * 100)

            for result in results:
                print(f"{os.path.basename(result['file']):<25} "
                      f"{result['frequency']:<10} "
                      f"{result['min_date'].strftime('%Y-%m-%d %H:%M:%S'):<25} "
                      f"{result['max_date'].strftime('%Y-%m-%d %H:%M:%S'):<25} "
                      f"{result['row_count']:<10}")

            print("=" * 100)
        else:
            print("No files were successfully analyzed.")


if __name__ == "__main__":
    main()
