import boto3
import pandas as pd
import io
import os
from datetime import datetime
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision

load_dotenv()

S3_BUCKET = os.environ.get("S3_BUCKET")
S3_KEY = os.environ.get("S3_KEY")
REGION_NAME = os.environ.get("AWS_REGION", "eu-west-2")

INFLUX_URL = os.environ.get("INFLUX_URL")
INFLUX_TOKEN = os.environ.get("INFLUX_TOKEN")
INFLUX_ORG = os.environ.get("INFLUX_ORG")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET")

s3_client = boto3.client("s3", region_name=REGION_NAME)

# --- HELPER FUNCTIONS ---
def convert_settlement_period_to_time(settlement_period):
    hour = (settlement_period - 1) // 2
    minute = 30 if settlement_period % 2 == 0 else 0
    return f"{hour:02d}:{minute:02d}:00"

def create_influx_points(df):
    points = []
    for _, row in df.iterrows():
        timestamp = f"{row['SETTLEMENT_DATE']}T{row['TIME']}Z"

        point = (
            Point("gridwatch")
            .tag("source_region", "UK")
            .field("national_demand", float(row["ND"]))
            .field("system_demand", float(row["TSD"]))
            .field("viking_flow", float(row["VIKING_FLOW"]))
            .time(pd.Timestamp(timestamp).to_pydatetime(), WritePrecision.NS)
        )
        points.append(point)
    return points

def write_points_to_influx(points):
    with InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG) as client:
        write_api = client.write_api()
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
        print(f"Wrote {len(points)} points to InfluxDB")

# --- LAMBDA HANDLER ---
def lambda_handler(event=None, context=None):
    csv_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
    body = csv_obj["Body"].read()
    df = pd.read_csv(io.BytesIO(body))
    print(f"CSV loaded, {len(df)} rows")

    df.fillna(df.mean(numeric_only=True), inplace=True)

    df["TIME"] = df["SETTLEMENT_PERIOD"].apply(convert_settlement_period_to_time)

    df = df[["SETTLEMENT_DATE", "TIME", "ND", "TSD", "VIKING_FLOW"]]

    points = create_influx_points(df)
    print(f"Prepared {len(points)} InfluxDB points")

    write_points_to_influx(points)
    print("All points written successfully")

    return {
        "statusCode": 200,
        "body": f"Successfully ingested {len(points)} points to InfluxDB bucket {INFLUX_BUCKET}"
    }

if __name__ == "__main__":
    lambda_handler()
