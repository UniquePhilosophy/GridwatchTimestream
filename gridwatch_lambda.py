import boto3
import pandas as pd
import io
from datetime import datetime, timedelta
import os

S3_BUCKET = os.environ.get("S3_BUCKET")
S3_KEY = os.environ.get("S3_KEY")
DATABASE_NAME = os.environ.get("TST_DATABASE")
TABLE_NAME = os.environ.get("TST_TABLE")
REGION_NAME = os.environ.get("AWS_REGION", "eu-west-2")

BATCH_SIZE = 100

timestream_client = boto3.client("timestream-write", region_name=REGION_NAME)
s3_client = boto3.client("s3", region_name=REGION_NAME)

# --- HELPER FUNCTIONS ---
def convert_settlement_period_to_time(settlement_period):
    hour = (settlement_period - 1) // 2
    minute = 30 if settlement_period % 2 == 0 else 0
    return f"{hour:02d}:{minute:02d}:00"

def create_timestream_records(df):
    records = []
    for _, row in df.iterrows():
        timestamp = f"{row['SETTLEMENT_DATE']}T{row['TIME']}Z"  # ISO format
        # Only ingest ND (National Demand) for demo; extend fields as needed
        record = {
            'Dimensions': [
                {'Name': 'region', 'Value': 'UK'}
            ],
            'MeasureName': 'national_demand',
            'MeasureValue': str(row['ND']),
            'MeasureValueType': 'DOUBLE',
            'Time': str(int(pd.Timestamp(timestamp).timestamp() * 1000))
        }
        records.append(record)
    return records

def write_records_in_batches(records):
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i+BATCH_SIZE]
        try:
            response = timestream_client.write_records(
                DatabaseName=DATABASE_NAME,
                TableName=TABLE_NAME,
                Records=batch
            )
            print(f"Written batch {i//BATCH_SIZE + 1}, response: {response}")
        except timestream_client.exceptions.RejectedRecordsException as e:
            print("Some records were rejected:", e)
        except Exception as e:
            print("Error writing batch:", e)

# --- LAMBDA HANDLER ---
def lambda_handler(event, context):
    csv_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
    body = csv_obj['Body'].read()
    df = pd.read_csv(io.BytesIO(body))
    print(f"CSV loaded, {len(df)} rows")

    df.fillna(0, inplace=True)

    df['TIME'] = df['SETTLEMENT_PERIOD'].apply(convert_settlement_period_to_time)

    df = df[['SETTLEMENT_DATE', 'TIME', 'ND']]

    records = create_timestream_records(df)
    print(f"Prepared {len(records)} Timestream records")

    write_records_in_batches(records)
    print("All records written successfully")

    return {
        'statusCode': 200,
        'body': f'Successfully ingested {len(records)} records to Timestream table {TABLE_NAME}'
    }
