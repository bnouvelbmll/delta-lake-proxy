# Delta S3 Proxy

## Overview

`delta-s3-proxy` is a reverse proxy for S3, specifically designed for providing controlled access to Delta Lake tables. It emulates a subset of the S3 API to allow S3-compatible deltalke clients to interact with Delta Lakes while enforcing partition-based authorization policies.

IT IS CURRENTLY IN A DEVELOPMENT STAGE.
 - VERIFICATION OF VALID USER IS NOT YET FULLY IMPLEMENTED
 - PERMISSION RETURNED CURRENTLY ARE STATIC AND NOT PER USER


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
    *   `presignedUrl`: The server redirects the client to a temporary, presigned S3 URL.
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

Configuration can be set by env variable
```export PROXY_PORT=28080 ```  can be a way to change the port.

## Building

To build the project, you need to have Rust and Cargo installed.

```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Build for release
cargo build --release
```

## Running

Before running, ensure your environment is configured with AWS credentials that have access to the underlying S3 buckets.

```bash
./target/release/delta_s3_proxy
```

The proxy will start on the port specified in the configuration (default `18080`).

## Testing

To run the test suite:

```bash
cargo test
```

## Packaging

For the moment it is packaged like so but we need to put it in git

```bash
tar -czf delta-lake-proxy.tgz delta-lake-proxy/src/* delta-lake-proxy/Cargo.toml delta-lake-proxy/README.md delta-lake-proxy/*.json
```