import os
from typing import Optional

from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd


def get_bq_client(
    project_id: Optional[str] = None,
    service_account_json: Optional[str] = None,
) -> bigquery.Client:
    """Create a BigQuery client.

    Preferred auth methods (in order):
    1) service_account_json path provided
    2) GOOGLE_APPLICATION_CREDENTIALS env var
    3) Application Default Credentials (gcloud auth application-default login)
    """
    if service_account_json and os.path.exists(service_account_json):
        credentials = service_account.Credentials.from_service_account_file(service_account_json)
        return bigquery.Client(project=project_id or credentials.project_id, credentials=credentials)

    # If no explicit path passed, rely on ADC (env var or gcloud)
    return bigquery.Client(project=project_id)


def run_query(client: bigquery.Client, sql: str, job_location: str = "US") -> pd.DataFrame:
    """Run a SQL query and return results as a pandas DataFrame.

    job_location should match the dataset/warehouse location (e.g., 'US', 'EU').
    """
    job = client.query(sql, location=job_location)
    result = job.result()  # Wait for job to complete
    return result.to_dataframe(create_bqstorage_client=True)


def main():
    # CONFIGURE THESE VARIABLES
    # Option A: Use a service account key file
    service_account_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")  # path/to/key.json

    # Option B: Use ADC (no key file). Make sure you ran:
    #   gcloud auth application-default login
    # and set project below (or have it set as default in your gcloud config)
    project_id = os.getenv("GCP_PROJECT_ID", "your-gcp-project-id")

    # Example: public dataset query (safe to run)
    sql = """
    SELECT name, SUM(number) AS total_babies
    FROM `bigquery-public-data.usa_names.usa_1910_2013`
    WHERE state = 'CA'
    GROUP BY name
    ORDER BY total_babies DESC
    LIMIT 10
    """

    client = get_bq_client(project_id=project_id, service_account_json=service_account_json)

    print("Running query...")
    df = run_query(client, sql, job_location="US")

    print("Top 5 rows:")
    print(df.head())

    # Access values programmatically
    for _, row in df.iterrows():
        print(f"Name={row['name']}, Total={row['total_babies']}")


if __name__ == "__main__":
    main()
