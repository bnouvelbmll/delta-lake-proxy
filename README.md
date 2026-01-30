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
%pip install deltalake
import polars as pl

from deltalake import DeltaTable
import json
import time
import boto3


_creds = None
_creds_time = 0

def get_creds(): 
    global _creds, _creds_time 
    if time.time() - _creds_time > 1800:
        session = boto3.Session() 
        _creds = session.get_credentials().get_frozen_credentials() 
        _creds_time=time.time() 
    return _creds

    

def spawn_proxy():
    creds=get_creds()
    os.system(f"""ps fauxw | grep delta_s3_proxy | grep -v grep | awk '{{print $2}}' | xargs kill; cd /home/bmll/user/delta-lake-proxy;  AWS_REGION=us-east-1 AWS_ACCESS_KEY_ID="{creds.access_key}" AWS_SECRET_ACCESS_KEY="{creds.secret_key}" AWS_SESSION_TOKEN="{creds.token}"  nohup  /home/bmll/user/delta-lake-proxy/target/debug/delta_s3_proxy &""")

spawn_proxy()

```

Run local proxy

```
python utils/local_proxy.py 
```


Install pyspark with deltas 

```
%pip install delta-spark
!(cd  /home/bmll/.spark/jars/; wget -nd  https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar)
!(cd  /home/bmll/.spark/jars/; wget -nd  https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar)
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

### Note that column mapping and iceberg compat is not working currently on databricks

```
import os
os.system("pip install -U deltalake")
import polars as pl

pl.scan_delta("s3://bmll-prod-lakehouse-market/__unitystorage/schemas/f476ced5-fdfb-47c8-8a92-35604c399648/tables/d4d07a86-b88b-444c-985a-bb7659caacd6/").limit(10).collect()

```
Results in 
```
File ~/.conda/envs/py311-stable/lib/python3.11/site-packages/polars/io/delta.py:376, in scan_delta(***failed resolving arguments***)
    374     if len(missing_features) > 0:
    375         msg = f"The table has set these reader features: {missing_features} but these are not yet supported by the polars delta scanner."
--> 376         raise DeltaProtocolError(msg)
    378 delta_schema = dl_tbl.schema()
    379 polars_schema = Schema(delta_schema)

DeltaProtocolError: The table has set these reader features: {'columnMapping'} but these are not yet supported by the polars delta scanner.

```


```
import os
os.system("pip install -U pyiceberg")
import polars as pl

pl.scan_iceberg("s3://bmll-prod-lakehouse-market/__unitystorage/schemas/f476ced5-fdfb-47c8-8a92-35604c399648/tables/d4d07a86-b88b-444c-985a-bb7659caacd6/")

```

Will result in 

```
...
File ~/.conda/envs/py311-stable/lib/python3.11/site-packages/pyarrow/_fs.pyx:815, in pyarrow._fs.FileSystem.open_input_file()

File ~/.conda/envs/py311-stable/lib/python3.11/site-packages/pyarrow/error.pxi:155, in pyarrow.lib.pyarrow_internal_check_status()

File ~/.conda/envs/py311-stable/lib/python3.11/site-packages/pyarrow/error.pxi:92, in pyarrow.lib.check_status()

FileNotFoundError: [Errno 2] Path does not exist 'bmll-prod-lakehouse-market/__unitystorage/schemas/f476ced5-fdfb-47c8-8a92-35604c399648/metadata/version-hint.text'. Detail: [errno 2] No such file or directory
<LazyFrame at 0x7FEA5C93E250>
```
