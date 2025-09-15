import boto3
import gridwatch_lambda as lf
import importlib
import io
from moto import mock_aws
import os
import pandas as pd
import pytest

# --- TEST CONFIG ---
TEST_BUCKET = "test-bucket"
TEST_KEY = "demanddata_2025_test.csv"
DATABASE_NAME = "TestDB"
TABLE_NAME = "TestTable"
REGION = "us-east-1"

CSV_CONTENT = """SETTLEMENT_DATE,SETTLEMENT_PERIOD,ND,TSD
2025-01-01,1,21036,26215
2025-01-01,2,21222,26063
2025-01-01,3,21385,25734
"""

@pytest.fixture(scope="function", autouse=True)
def aws_setup():
    """One shared moto context for all AWS services."""
    with mock_aws():
        # Mock credentials
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_REGION"] = REGION

        # Env vars for lambda
        os.environ["S3_BUCKET"] = TEST_BUCKET
        os.environ["S3_KEY"] = TEST_KEY
        os.environ["TST_DATABASE"] = DATABASE_NAME
        os.environ["TST_TABLE"] = TABLE_NAME

        # Create S3
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket=TEST_BUCKET)
        s3.put_object(Bucket=TEST_BUCKET, Key=TEST_KEY, Body=CSV_CONTENT)

        # Create Timestream
        ts = boto3.client("timestream-write", region_name=REGION)
        ts.create_database(DatabaseName=DATABASE_NAME)
        ts.create_table(DatabaseName=DATABASE_NAME, TableName=TABLE_NAME)

        # Reload lambda so it picks up env vars
        import gridwatch_lambda
        importlib.reload(gridwatch_lambda)

        yield  # let the tests run


def test_convert_settlement_period_to_time():
    assert lf.convert_settlement_period_to_time(1) == "00:00:00"
    assert lf.convert_settlement_period_to_time(2) == "00:30:00"
    assert lf.convert_settlement_period_to_time(48) == "23:30:00"
    with pytest.raises(TypeError):
        lf.convert_settlement_period_to_time("a")


def test_lambda_handler_s3_timestream():
    response = lf.lambda_handler({}, {})
    body = response["body"]
    assert "Successfully ingested" in body


def test_dataframe_cleaning():
    s3_client = boto3.client("s3", region_name=REGION)
    csv_obj = s3_client.get_object(Bucket=TEST_BUCKET, Key=TEST_KEY)
    df = pd.read_csv(io.BytesIO(csv_obj["Body"].read()))
    df.loc[0, "ND"] = None
    df.fillna(0, inplace=True)
    assert df.loc[0, "ND"] == 0


def test_create_timestream_records_structure():
    df = pd.DataFrame({
        "SETTLEMENT_DATE": ["2025-01-01"],
        "TIME": ["00:00:00"],
        "ND": [21036],
    })
    records = lf.create_timestream_records(df)
    assert len(records) == 1
    record = records[0]
    assert record["MeasureName"] == "national_demand"
    assert record["MeasureValue"] == "21036"
    assert record["MeasureValueType"] == "DOUBLE"
    assert int(record["Time"]) > 0


def test_write_records_in_batches():
    records = []
    for i in range(250):
        records.append({
            "Dimensions": [{"Name": "region", "Value": "UK"}],
            "MeasureName": "national_demand",
            "MeasureValue": str(20000 + i),
            "MeasureValueType": "DOUBLE",
            "Time": str(1672531200000 + i * 1800000),
        })
    lf.write_records_in_batches(records)


def test_lambda_handler_with_empty_csv():
    s3_client = boto3.client("s3", region_name=REGION)
    s3_client.put_object(
        Bucket=TEST_BUCKET,
        Key=TEST_KEY,
        Body="SETTLEMENT_DATE,SETTLEMENT_PERIOD,ND,TSD\n",  # empty except header
    )
    response = lf.lambda_handler({}, {})
    body = response["body"]
    assert "0 records" in body
