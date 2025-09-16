import boto3
import gridwatch_lambda as lf
import importlib
import io
from moto import mock_aws
import os
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

# --- TEST CONFIG ---
TEST_BUCKET = "test-bucket"
TEST_KEY = "demanddata_2025_test.csv"
REGION = "us-east-1"

CSV_CONTENT = """SETTLEMENT_DATE,SETTLEMENT_PERIOD,ND,TSD,VIKING_FLOW
2025-01-01,1,21036,26215,100
2025-01-01,2,21222,26063,200
2025-01-01,3,21385,25734,300
"""

@pytest.fixture(scope="function", autouse=True)
def aws_setup():
    with mock_aws():
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_REGION"] = REGION

        os.environ["S3_BUCKET"] = TEST_BUCKET
        os.environ["S3_KEY"] = TEST_KEY

        os.environ["INFLUX_URL"] = "http://localhost:8086"
        os.environ["INFLUX_TOKEN"] = "test-token"
        os.environ["INFLUX_ORG"] = "test-org"
        os.environ["INFLUX_BUCKET"] = "test-bucket"

        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket=TEST_BUCKET)
        s3.put_object(Bucket=TEST_BUCKET, Key=TEST_KEY, Body=CSV_CONTENT)

        import gridwatch_lambda
        importlib.reload(gridwatch_lambda)

        yield


def test_convert_settlement_period_to_time():
    assert lf.convert_settlement_period_to_time(1) == "00:00:00"
    assert lf.convert_settlement_period_to_time(2) == "00:30:00"
    assert lf.convert_settlement_period_to_time(48) == "23:30:00"
    with pytest.raises(TypeError):
        lf.convert_settlement_period_to_time("a")


@patch("gridwatch_lambda.InfluxDBClient")
def test_lambda_handler_s3_influx(mock_influx):
    mock_write_api = MagicMock()
    mock_influx.return_value.write_api.return_value = mock_write_api

    response = lf.lambda_handler({}, {})
    body = response["body"]

    assert "Successfully ingested" in body
    mock_write_api.write.assert_called_once()
    args, kwargs = mock_write_api.write.call_args
    assert len(kwargs["record"]) > 0


def test_dataframe_mean_imputation():
    s3_client = boto3.client("s3", region_name=REGION)
    csv_obj = s3_client.get_object(Bucket=TEST_BUCKET, Key=TEST_KEY)
    df = pd.read_csv(io.BytesIO(csv_obj["Body"].read()))
    df.loc[0, "ND"] = None

    mean_val = df["ND"].mean(skipna=True)
    df.fillna(df.mean(numeric_only=True), inplace=True)

    assert df.loc[0, "ND"] == pytest.approx(mean_val)


def test_create_influx_points_structure():
    df = pd.DataFrame({
        "SETTLEMENT_DATE": ["2025-01-01"],
        "TIME": ["00:00:00"],
        "ND": [21036],
        "TSD": [26215],
        "VIKING_FLOW": [123],
    })
    points = lf.create_influx_points(df)
    assert len(points) == 1
    point = points[0]
    lp = point.to_line_protocol()
    assert "gridwatch" in lp
    assert "national_demand" in lp
    assert "system_demand" in lp
    assert "viking_flow" in lp


@patch("gridwatch_lambda.InfluxDBClient")
def test_lambda_handler_with_empty_csv(mock_influx):
    s3_client = boto3.client("s3", region_name=REGION)
    s3_client.put_object(
        Bucket=TEST_BUCKET,
        Key=TEST_KEY,
        Body="SETTLEMENT_DATE,SETTLEMENT_PERIOD,ND,TSD,VIKING_FLOW\n",
    )

    mock_write_api = MagicMock()
    mock_influx.return_value.write_api.return_value = mock_write_api

    response = lf.lambda_handler({}, {})
    body = response["body"]

    assert "0 points" in body
