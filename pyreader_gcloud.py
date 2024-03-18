import os
import pandas as pd
import time
from google.cloud import storage

# Define Google Cloud Storage variables
BUCKET_NAME = "bvd_info_1103"
FOLDER_PATH = "MahdiMethod"

def read_excel_files(folder_path=FOLDER_PATH, sheet="Results"):
    start_time_main = time.time()
    files_processed = 0
    df_main = pd.DataFrame()
    error_log = []

    # Initialize Google Cloud Storage client
    storage_client = storage.Client()

    # Get bucket object
    bucket = storage_client.bucket(BUCKET_NAME)

    # Get list of blob objects in the folder
    blobs = bucket.list_blobs(prefix=folder_path)

    for blob in blobs:
        if blob.name.endswith('.xlsx'):
            try:
                start_time = time.time()
                # Download the file content
                file_content = blob.download_as_string()
                # Read Excel file from the content
                df = pd.read_excel(file_content, sheet_name=sheet, engine="calamine")
                end_time = time.time()
                execution_time = end_time - start_time
                print(f"Processed {files_processed} Excel files in {execution_time:.2f} seconds.")
                df_main = pd.concat([df_main, df])

                if files_processed % 10 == 0:
                    # Save dataframe to Parquet format
                    df_main.to_parquet(f'batch_{files_processed}.parquet')
                    df_main = pd.DataFrame()

                files_processed += 1

            except Exception as e:
                error_message = f"Error reading file {blob.name}: {str(e)}"
                print(error_message)
                error_log.append({'file_name': blob.name, 'error_message': error_message})
                # Writing error log to CSV after each iteration
                error_df = pd.DataFrame(error_log)
                error_log_path = os.path.join(folder_path, 'error_log.csv')
                error_df.to_csv(error_log_path, index=False)
                print(f"Error log updated: {error_log_path}")
                continue

    return df_main


def to_parquet(df):
    start_time = time.time()
    df.to_parquet('output.parquet')
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Converted to parquet in {execution_time:.2f} seconds.")
    results = {'method:': 'parquet', 'execution time': f'{execution_time:.2f}'}
    return results

def store_excel_files(df):
    method_results = [
        to_parquet(df),
    ]
    df_results = pd.DataFrame(method_results)
    df_results.to_csv('conversion_stats.csv')
    print(df_results)

if __name__ == "__main__":
    df = read_excel_files()
    store_excel_files(df)
