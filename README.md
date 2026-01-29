# Delta S3 Proxy

## Overview

`delta-s3-proxy` is a reverse proxy for S3, specifically designed for providing controlled access to Delta Lake tables. It emulates a subset of the S3 API to allow S3-compatible deltalke clients to interact with Delta Lakes while enforcing partition-based authorization policies.

IT IS CURRENTLY IN A DEVELOPMENT STAGE.
 - VERIFICATION OF VALID USER IS NOT YET FULLY IMPLEMENTED


THE PROJECT SO FAR HAS BEEN 95% VIBE-CODED AND NEEDS MORE REVIEWS AND VALID.

Things that are implemented is basic whitelist for files.


It is built with Rust using `warp`, `tokio`, and the `deltalake` and `aws-sdk-s3` crates.

## Features

*   **S3 API Emulation**: Emulates S3 API endpoints for listing buckets and objects, and for getting objects.
*   **Table Aliasing**: Maps user-friendly table aliases to their actual S3 paths.
*   **Partition-Based Access Control**: Restricts access to data files based on partition values defined in the configuration.
*   **Caching**: In-memory caching for Delta Table metadata, file lists, and authentication details to improve performance.
*   **Flexible Get Modes**: Supports two modes for handling `GET` requests for data files:
    *   `proxy`: The server streams the file content directly from S3 to the client.
    *   `presignedUrl`: The server redirects the client to a temporary, presigned S3 URL. When used with the local Python proxy, the proxy handles stripping extraneous headers (like `Authorization`) during the redirect to prevent S3 errors.
*   **Read-Only Mode**: Can be configured to prevent any write operations (`PUT`, `POST`, `DELETE`).
*   **Authentication Forwarding**: Can be configured to forward authentication details to the underlying S3 bucket.

## Configuration

The application is configured via a `config.json` file in the working directory, or through environment variables with the prefix `PROXY_`.

**Example `config.json`:**

```json
{
  "tableMapping": {
    "my_table": "s3://my-bucket/path/to/table"
  },
  "readOnly": true,
  "proxyPartial": false,
  "defaultAuthMode": "iam",
  "getMode": "presignedUrl",
  "allowedPartitions": {
    "my_table": [
      { "MIC": "XLON" },
      { "MIC": "XETR" }
    ]
  },
  "port": 18080
}
```

**Configuration Options:**

*   `tableMapping` (required): A map of table aliases to their S3 URIs.
*   `readOnly`: If `true`, disables `PUT`, `POST`, and `DELETE` requests. Defaults to `true`.
*   `proxyPartial`: If `true`, forces proxying for partial/ranged `GET` requests even in `presignedUrl` mode. Defaults to `false`.
*   `defaultAuthMode`: The default authentication mode. Can be `iam` or `forward`. Defaults to `iam`.
*   `getMode`: The mode for handling `GET` requests. Can be `proxy` or `presignedUrl`. Defaults to `presignedUrl`.
*   `allowedPartitions`: A map where keys are table aliases and values are lists of allowed partition key-value pairs.
*   `port`: The port on which the proxy will listen. Defaults to `18080`.
*   `metricsPort`: An optional port for a separate Prometheus metrics endpoint. If specified, a metrics server will run on this port (e.g., `9090`). If omitted, metrics are not exposed on a separate port.

Configuration can be set by env variable
```export PROXY_PORT=28080 ```  can be a way to change the port.

## Prometheus Metrics

The `delta-s3-proxy` exposes a Prometheus-compatible metrics endpoint to monitor its operational statistics. This endpoint can be configured to run on a separate port.

**Endpoint:** `/metrics`

**Configuration:**
The `metricsPort` field in `config.json` (or `PROXY_METRICS_PORT` environment variable) determines the port for the metrics server. If `metricsPort` is not specified, a metrics server will not be started on a separate port.

**Collected Metrics (Aggregated per minute):**

*   `queries_served_total`: Counter for the total number of queries served by the proxy.
*   `queries_proxied_total`: Counter for the total number of queries directly proxied to S3.
*   `unique_users_last_minute`: Gauge indicating the number of unique user IDs observed in the last minute.
*   `backend_reply_latency_seconds_avg`: Gauge showing the average latency (in seconds) of backend S3 replies in the last minute.
*   `average_message_size_bytes`: Gauge representing the average size (in bytes) of messages processed in the last minute.

## Building

To build the project, you need to have Rust and Cargo installed.

```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > /tmp/rup.sh; sh /tmp/rup.sh -y; rm /tmp/rup.sh; . "$HOME/.cargo/env"

# Build for release
cargo build --release
```

## Running

Before running, ensure your environment is configured with AWS credentials that have access to the underlying S3 buckets.

```bash

```

The proxy will start on the port specified in the configuration (default `18080`).

## Testing

To run the test suite:

```bash
cargo test
```


## Validation

Run remote proxy

```
import os

os.system("pip install deltalake")
os.environ["RUST_LOG"]="INFO"# os.environ["HTTP_PROXY"]="http://127.0.0.1:28080"
from pyspark.sql import SparkSession
import boto3
import os


print("Stopping SPARK")
# Stop any running Spark session
try:
    spark.stop()
except Exception as e:
    print(e)
    pass
print("Reconfiguring SPARK")

# Get temporary credentials from boto3
session = boto3.Session()
creds = session.get_credentials().get_frozen_credentials()

os.environ["AWS_ACCESS_KEY_ID"] = creds.access_key
os.environ["AWS_SECRET_ACCESS_KEY"] = creds.secret_key
os.environ["AWS_SESSION_TOKEN"] = creds.token
# os.environ["HTTP_PROXY"] = "http://127.0.0.1:28080/"
# os.environ["HTTPS_PROXY"] = "http://127.0.0.1:28080/"
print("Starting SPARK")

spark = (SparkSession.builder \
    .master("local[*]") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", creds.access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", creds.secret_key) \
    .config("spark.hadoop.fs.s3a.session.token", creds.token) \
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:18080") \
    .config("fs.s3a.endpoint", "http://localhost:18080") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.proxy.host", "127.0.0.1") \
    .config("spark.hadoop.fs.s3a.proxy.port", "28080") \
    .config("fs.s3a.proxy.host", "127.0.0.1") \
    .config("fs.s3a.proxy.port", "28080") \
    .config("fs.s3a.proxy.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.proxy.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider") \
     .config( "spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension") \
    .config("com.amazonaws.sdk.disableCertChecking","true") \
    .config("spark.hadoop.fs.s3a.ssl.channel.mode","insecure")
    .config ("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
        )
print("Loading data")
df=spark.read.format("delta").load("s3a://datalake/trades_plus/")
#df = spark.read.parquet("s3a://bmll-prod-lakehouse-market/__unitystorage/schemas/f476ced5-fdfb-47c8-8a92-35604c399648/tables/d4d07a86-b88b-444c-985a-bb7659caacd6/02/part-00027-69cf6606-bd2e-44dc-8edc-602ee9776132.c000.zstd.parquet")


try:
    dfq=df.where("MIC = 'XMIL'").limit(10)
    print(pd.DataFrame(dfq.toLocalIterator(), columns =dfq.columns))[["Ticker","MIC","TradeTimestamp","Price","Size"]]
except Exception as e:
    print("XMIL",e)


try:
    dfq=df.where("MIC = 'XLON'").limit(10)
    print(pd.DataFrame(dfq.toLocalIterator(), columns =dfq.columns))[["Ticker","MIC","TradeTimestamp","Price","Size"]]
except Exception as e:
    print("XLON",e)

    
try:
    dfq=df.where("MIC = 'XPAR'").limit(10)
    print(pd.DataFrame(dfq.toLocalIterator(), columns =dfq.columns))[["Ticker","MIC","TradeTimestamp","Price","Size"]]
except Exception as e:
    print("XPAR",e)


import json
import polars as pl

from deltalake import DeltaTable
import json
import time
import boto3

def get_mapping_and_files(table_uri, partition_filter=None, storage_options=None): 
    # 1. Initialize the table (S3 requires storage_options) 
    dt = DeltaTable(table_uri, storage_options=storage_options) 
    # 2. Get the Schema Mapping # We look into the table's internal metadata for the column mapping 
    metadata = dt.metadata() 
    print(dir(metadata._metadata))
    print(metadata._metadata)
    schema_json = json.loads(metadata.schema_json) 
    rename_map = {} 
    for field in schema_json.get("fields", []): 
        logical_name = field["name"] 
        # Extract physical name from field metadata 
        physical_name = field.get("metadata", {}).get("delta.columnMapping.physicalName") 
        if physical_name: 
            rename_map[physical_name] = logical_name
     # 3. Get specific files filtered by partition # Example partition_filter: [("year", "=", "2023"), ("month", "=", "12")] 
    active_files = dt.file_uris(partition_filters=partition_filter) 
    return rename_map, active_files

_creds = None
_creds_time = 0

def get_creds(): 
    global _creds, _creds_time 
    if time.time() - _creds_time > 1800:
        session = boto3.Session() 
        _creds = session.get_credentials().get_frozen_credentials() 
        _creds_time=time.time() 
    return _creds

def test_load_data( table_path = "s3:/datalake/trade_plus/",filters = [("TradeDate", "=", "2024-01-25")]):
     # AWS Configuration 
     creds=get_creds()
     if table_path.startswith("s3://datalake"):
         s3_options = { "aws_access_key_id": creds.access_key, "aws_secret_access_key": creds.secret_key, "aws_session_token": creds.token, "aws_region": "us-east-1", "aws_endpoint_url": "http://localhost:18080", "aws_s3_force_path_style": "true", "aws_allow_http": "true",
         }
     else:
         s3_options = { "aws_access_key_id": creds.access_key, "aws_secret_access_key": creds.secret_key, "aws_session_token": creds.token, "aws_region": "us-east-1"}
         

     # Define your partition filter 
     # This ensures we only fetch files from the specific S3 'folder' # 1. Get the translation map and the S3 URIs mapping, 
     file_uris = get_mapping_and_files( table_path, partition_filter=
                                        
                                        filters, storage_options=s3_options )
     # 2. Load into Polars # Polars read_parquet handles S3 URIs natively if you pass storage_options 
     df = pl.scan_parquet(file_uris, storage_options=s3_options)
     # 3. Apply the mapping to fix the UUID column names 
     df = df.rename({k: v for k, v in mapping.items() if k in df.columns})
     return df

def spawn_proxy():
    creds=get_creds()
    os.system(f"""ps fauxw | grep delta_s3_proxy | grep -v grep | awk '{{print $2}}' | xargs kill; cd /home/bmll/user/delta-lake-proxy;  AWS_REGION=us-east-1 AWS_ACCESS_KEY_ID="{creds.access_key}" AWS_SECRET_ACCESS_KEY="{creds.secret_key}" AWS_SESSION_TOKEN="{creds.token}"  nohup  /home/bmll/user/delta-lake-proxy/target/debug/delta_s3_proxy &""")

spawn_proxy()

```

Run local proxy

```
python utils/local_proxy.py 
```


Run this script to verify feature

```
from pyspark.sql import SparkSession
import boto3
import os


print("Stopping SPARK")
# Stop any running Spark session
try:
    spark.stop()
except Exception as e:
    print(e)
    pass
print("Reconfiguring SPARK")

# Get temporary credentials from boto3
session = boto3.Session()
creds = session.get_credentials().get_frozen_credentials()

os.environ["AWS_ACCESS_KEY_ID"] = creds.access_key
os.environ["AWS_SECRET_ACCESS_KEY"] = creds.secret_key
os.environ["AWS_SESSION_TOKEN"] = creds.token
print("Starting SPARK")

spark = (SparkSession.builder \
    .master("local[*]") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", creds.access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", creds.secret_key) \
    .config("spark.hadoop.fs.s3a.session.token", creds.token) \
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:18080") \
    .config("fs.s3a.endpoint", "http://localhost:18080") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.proxy.host", "127.0.0.1") \
    .config("spark.hadoop.fs.s3a.proxy.port", "28080") \
    .config("fs.s3a.proxy.host", "127.0.0.1") \
    .config("fs.s3a.proxy.port", "28080") \
    .config("fs.s3a.proxy.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.proxy.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider") \
    .config( "spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension") \
    .config("com.amazonaws.sdk.disableCertChecking","true") \
    .config("spark.hadoop.fs.s3a.ssl.channel.mode","insecure")
    .config ("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
        )
print("Loading data")
df=spark.read.format("delta").load("s3a://datalake/trades_plus/")
#df = spark.read.parquet("s3a://bmll-prod-lakehouse-market/__unitystorage/schemas/f476ced5-fdfb-47c8-8a92-35604c399648/tables/d4d07a86-b88b-444c-985a-bb7659caacd6/02/part-00027-69cf6606-bd2e-44dc-8edc-602ee9776132.c000.zstd.parquet")


try:
    dfq=df.where("MIC = 'XMIL'").limit(10)
    print(pd.DataFrame(dfq.toLocalIterator(), columns =dfq.columns))[["Ticker","MIC","TradeTimestamp","Price","Size"]]
except Exception as e:
    print("XMIL",e)


try:
    dfq=df.where("MIC = 'XLON'").limit(10)
    print(pd.DataFrame(dfq.toLocalIterator(), columns =dfq.columns))[["Ticker","MIC","TradeTimestamp","Price","Size"]]
except Exception as e:
    print("XLON",e)

    
try:
    dfq=df.where("MIC = 'XPAR'").limit(10)
    print(pd.DataFrame(dfq.toLocalIterator(), columns =dfq.columns))[["Ticker","MIC","TradeTimestamp","Price","Size"]]
except Exception as e:
    print("XPAR",e)


```
