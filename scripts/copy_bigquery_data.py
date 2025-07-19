import sys
from typing import Union
import pandas as pd

def get_project_id():
    import urllib.request
    # method 1
    url = "http://metadata.google.internal/computeMetadata/v1/project/project-id"
    req = urllib.request.Request(url)
    req.add_header("Metadata-Flavor", "Google")
    project_id = urllib.request.urlopen(req).read().decode()
    if not project_id:
        try:
            # method 2 - try to retrieve PROJECT ID from config.json in gs://{PROJECT}/datagen/config.json
            config = get_local_config()
            project_id = config["project_id"]
        except:   
            try:
                # method 3
                import subprocess
                # Using subprocess here to spawn new thread to run processes - connect to their input/output/error pipes, and obtain their return codes.
                project_id=subprocess.check_output(["gcloud config get-value project"], shell=True, stderr=subprocess.STDOUT).decode("utf-8").replace("\n","") 
            except:
                raise ValueError(f"Could not get a value for PROJECT_ID: error seems to be - {project_id}")

    return project_id


def run_bq_query(sql: str) -> Union[str, pd.DataFrame]:
    """
    Executes a SQL query in BigQuery.
    Returns:
        - A pandas DataFrame if the query succeeds.
        - A string describing the error if it fails.
    """

    from google.cloud import bigquery
    from google.api_core.exceptions import BadRequest, GoogleAPIError

    try:
        bq_client = bigquery.Client()

        # Step 1: Dry run to validate the query without running it
        try:
            job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
            bq_client.query(sql, job_config=job_config)
        except BadRequest as e:
            return f"Query validation failed (dry run error): {e.message if hasattr(e, 'message') else str(e)}"

        job_config = bigquery.QueryJobConfig()
        client_result = bq_client.query(sql, job_config=job_config)

        df = client_result.result().to_arrow().to_pandas()
        return df

    except BadRequest as e:
        return f"BadRequest error during query execution: {e.message if hasattr(e, 'message') else str(e)}"
    except GoogleAPIError as e:
        return f"Google API error: {e.message if hasattr(e, 'message') else str(e)}"
    except Exception as e:
        return f"Unexpected error: {str(e)}"


def copy_blob(
    bucket_name, blob_name, destination_bucket_name, destination_blob_name
):
    """Copies a blob from one bucket to another with a new name."""

    from google.cloud import storage
    from google.api_core.exceptions import NotFound, Forbidden, GoogleAPIError

    try:
        storage_client = storage.Client()

        source_bucket = storage_client.bucket(bucket_name)
        source_blob = source_bucket.blob(blob_name)
        destination_bucket = storage_client.bucket(destination_bucket_name)

        if not source_blob.exists():
            raise NotFound(f"Source blob '{blob_name}' not found in bucket '{bucket_name}'")

        # Copy the blob
        blob_copy = source_bucket.copy_blob(
            source_blob, destination_bucket, destination_blob_name
        )

        # Optionally make public
        if destination_bucket_name == "cymbal-fraudfinder":
            try:
                blob_copy.make_public()
            except GoogleAPIError as e:
                print(f"Warning: Failed to make blob public: {e}")

        print(f"File copied from gs://{source_bucket.name}/{source_blob.name} \n\t\t to gs://{destination_bucket.name}/{blob_copy.name}")

    except NotFound as e:
        print(f"Error: Not found - {e}")
    except Forbidden as e:
        print(f"Error: Permission denied - {e}")
    except GoogleAPIError as e:
        print(f"Google API Error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")



def get_batch_data_gcs(BUCKET_NAME):
    '''
    Copy necessary files for datagen streaming
    '''
    copy_blob(
        bucket_name="cymbal-fraudfinder",
        blob_name="datagen/hacked_customers_history.txt",
        destination_bucket_name=BUCKET_NAME,
        destination_blob_name="datagen/hacked_customers_history.txt"
    )

    copy_blob(
        bucket_name="cymbal-fraudfinder",
        blob_name="datagen/hacked_terminals_history.txt",
        destination_bucket_name=BUCKET_NAME,
        destination_blob_name="datagen/hacked_terminals_history.txt"
    )
    copy_blob(
        bucket_name="cymbal-fraudfinder",
        blob_name="datagen/demographics/customer_profiles.csv",
        destination_bucket_name=BUCKET_NAME,
        destination_blob_name="datagen/demographics/customer_profiles.csv"
    )

    copy_blob(
        bucket_name="cymbal-fraudfinder",
        blob_name="datagen/demographics/terminal_profiles.csv",
        destination_bucket_name=BUCKET_NAME,
        destination_blob_name="datagen/demographics/terminal_profiles.csv"
    )

    copy_blob(
        bucket_name="cymbal-fraudfinder",
        blob_name="datagen/demographics/customer_with_terminal_profiles.csv",
        destination_bucket_name=BUCKET_NAME,
        destination_blob_name="datagen/demographics/customer_with_terminal_profiles.csv"
    )

    return "Done get_batch_data_gcs"

def get_batch_data_bq(PROJECT):
    '''
    Creates the following tables in your project by copying from public tables:

    {YOUR PROJECT}
    |-`tx` (dataset)
    |-`tx` (table: transactions without labels)
    |-`txlabels` (table: transactions with fraud labels (1 or 0))
    |-demographics
    |-`customers` (table: profiles of customers)
    |-`terminals` (table: profiles of terminals)
    |-`customersterminals` (table: profiles of customers and terminals within their radius)
    '''

    run_bq_query(f"CREATE SCHEMA IF NOT EXISTS `{PROJECT}`.tx OPTIONS(location='us-central1');")
    run_bq_query(f"CREATE SCHEMA IF NOT EXISTS `{PROJECT}`.demographics OPTIONS(location='us-central1');")

    # Creating partitions on timestamp - this helps in optimizing when running queries on certain parts of the data
    # partition is done on 2025-07-18 and then 2025-07-19, so when we use the WHERE tx_ts = '2025-07-19', bigquery reads on partitioned data, reducing time and cost
    run_bq_query(f"""
    CREATE OR REPLACE TABLE `{PROJECT}`.tx.tx 
    PARTITION BY
    DATE(TX_TS)
    AS (
        SELECT
        TX_ID,
        TX_TS,
        CUSTOMER_ID,
        TERMINAL_ID,
        TX_AMOUNT
        FROM
        `cymbal-fraudfinder`.txbackup.all
    );
    """)
    print(f"BigQuery table created: `{PROJECT}`.tx.tx")

    run_bq_query(f"""
    CREATE OR REPLACE TABLE `{PROJECT}`.tx.txlabels
    AS (
        SELECT
        TX_ID,
        TX_FRAUD
        FROM
        `cymbal-fraudfinder`.txbackup.all
    );
    """)
    print(f"BigQuery table created: `{PROJECT}`.tx.txlabels")
    
    run_bq_query(f"""
    CREATE OR REPLACE TABLE `{PROJECT}`.demographics.customers
    AS (
        SELECT
        *
        FROM
        `cymbal-fraudfinder`.demographics.customers
    );
    """)
    print(f"BigQuery table created: `{PROJECT}`.demographics.customers")
    
    run_bq_query(f"""
    CREATE OR REPLACE TABLE `{PROJECT}`.demographics.terminals
    AS (
        SELECT
        *
        FROM
        `cymbal-fraudfinder`.demographics.terminals
    );
    """)
    print(f"BigQuery table created: `{PROJECT}`.demographics.terminals")
    
    run_bq_query(f"""
    CREATE OR REPLACE TABLE `{PROJECT}`.demographics.customersterminals
    AS (
        SELECT
        *
        FROM
        `cymbal-fraudfinder`.demographics.customersterminals
    );
    """)
    print(f"BigQuery table created: `{PROJECT}`.demographics.customersterminals")
    
    return "Done get_batch_data_bq"


def read_from_sub(project_id, subscription_name, messages=10):
    """
    Read messages from a Pub/Sub subscription
    Args:
        project_id: project ID
        subscription_name: the name of a Pub/Sub subscription in your project
        messages: number of messages to read
    Returns:
        msg_data: list of messages in your Pub/Sub subscription as a Python dictionary
    """
    
    import ast

    from google.api_core import retry
    from google.cloud import pubsub_v1

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_name)

    # Wrap the subscriber in a 'with' block to automatically call close() to
    # close the underlying gRPC channel when done.
    with subscriber:
        # The subscriber pulls a specific number of messages. The actual
        # number of messages pulled may be smaller than max_messages.
        response = subscriber.pull(
            subscription=subscription_path,
            max_messages=messages,
            retry=retry.Retry(deadline=300),
        )

        if len(response.received_messages) == 0:
            print("no messages")
            return

        ack_ids = []
        msg_data = []
        for received_message in response.received_messages:
            msg = ast.literal_eval(received_message.message.data.decode("utf-8"))
            msg_data.append(msg)
            ack_ids.append(received_message.ack_id)

        # Acknowledges the received messages so they will not be sent again.
        subscriber.acknowledge(subscription=subscription_path, ack_ids=ack_ids)

        print(
            f"Received and acknowledged {len(response.received_messages)} messages from {subscription_path}."
        )

        return msg_data


if __name__ == "__main__":
    PROJECT = get_project_id()
    BUCKET_NAME = sys.argv[1]
    get_batch_data_gcs(BUCKET_NAME)
    get_batch_data_bq(PROJECT)