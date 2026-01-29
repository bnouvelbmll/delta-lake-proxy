use aws_sdk_s3::{Client as S3Client, primitives::ByteStream};
use aws_sdk_s3::presigning::PresigningConfig;
use bytes::Buf;
use deltalake::DeltaTable;
use deltalake::PartitionFilter;
use futures_util::{Stream, TryStreamExt};
use moka::sync::Cache;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};
use url::Url;
use warp::http::{HeaderMap, Uri};
use aws_smithy_types::date_time::Format as DateTimeFormat;
use warp::hyper::{Body, Method, Response};
use warp::{Filter, Reply, http::StatusCode};

mod config;
use config::*;
mod database;
use database::DBPool;
mod permission;
mod metrics;
use metrics::{AppMetrics, metrics_route};

// ---------------------- Custom Error ----------------------
#[derive(Debug)]
struct CustomError(String);

impl warp::reject::Reject for CustomError {}

fn custom_rejection(msg: &str) -> warp::Rejection {
    warp::reject::custom(CustomError(msg.to_string()))
}

// ---------------------- Shared state ----------------------
struct AppState {
    s3_client: S3Client,
    http_client: reqwest::Client,
    table_mapping: HashMap<String, (String, String)>, // alias -> (bucket, prefix)
    delta_cache: Cache<String, Arc<DeltaTable>>, // Cache for DeltaTable instances
    auth_cache: Cache<String, bool>,             // Cache for validated AWS credentials
    file_list_cache: Cache<String, Arc<Vec<String>>>, 
    config: Config,
    db_pool: Option<DBPool>,
    metrics: Arc<AppMetrics>, // Add metrics field
}

impl AppState {
    async fn new(metrics: Arc<AppMetrics>) -> Self {
        let aws_config = aws_config::load_from_env().await;
        let s3_client = S3Client::new(&aws_config);
        let http_client = reqwest::Client::new();

        let settings = ::config::Config::builder()
            .add_source(::config::File::with_name("config").required(false))
            .add_source(::config::Environment::with_prefix("PROXY"))
            .build()
            .unwrap();

        let config: Config = settings.try_deserialize().unwrap();

        let db_pool = if config.database_enabled {
            Some(DBPool::new(&config).await.unwrap())
        } else {
            None
        };

        let mut table_mapping = HashMap::new();
        for (alias, uri) in &config.table_mapping {
            let uri_trim = uri.trim_start_matches("s3://");
            if let Some((bucket, prefix)) = uri_trim.split_once('/') {
                let prefix = prefix.trim_end_matches('/');
                table_mapping.insert(alias.clone(), (bucket.to_string(), prefix.to_string()));
            }
        }

        Self {
            s3_client,
            http_client,
            table_mapping,
            delta_cache: Cache::builder()
                .max_capacity(20)
                .time_to_live(Duration::from_secs(120))
                .build(),
            auth_cache: Cache::builder()
                .max_capacity(100)
                .time_to_live(Duration::from_secs(120))
                .build(),
            file_list_cache: Cache::builder()
                .max_capacity(100)
                .time_to_live(Duration::from_secs(120))
                .build(),
            config,
            db_pool,
            metrics,
        }
    }
}

// ---------------------- Main ----------------------
#[tokio::main]
async fn main() {
    // Initialize logging with environment filter (e.g., RUST_LOG=info)
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let metrics = AppMetrics::new();
    tokio::spawn(AppMetrics::start_aggregation_task(metrics.clone()));

    let state = Arc::new(AppState::new(metrics.clone()).await);
    let port = state.config.port;

    let list_buckets_route = warp::path::end().and(warp::get()).map(move || {
        let mut xml = String::new();
        use std::fmt::Write;
        write!(&mut xml, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>").unwrap();
        write!(
            &mut xml,
            "<ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
        )
        .unwrap();
        write!(&mut xml, "<Owner><ID>owner-id</ID><DisplayName>owner-display-name</DisplayName></Owner>").unwrap();
        write!(&mut xml, "<Buckets>").unwrap();
        write!(&mut xml, "<Bucket>").unwrap();
        write!(&mut xml, "<Name>datalake</Name>").unwrap();
        write!(
            &mut xml,
            "<CreationDate>2024-01-01T00:00:00.000Z</CreationDate>"
        )
        .unwrap();
        write!(&mut xml, "</Bucket>").unwrap();
        write!(&mut xml, "</Buckets>").unwrap();
        write!(&mut xml, "</ListAllMyBucketsResult>").unwrap();

        warp::http::Response::builder()
            .header("Content-Type", "application/xml")
            .body(Body::from(xml))
            .unwrap()
    });

    let datalake_prefix = warp::path("datalake");

    // GET eroute: handle partition-aware GET requests
    let get_route = warp::get()
        .and(datalake_prefix.clone())
        .and(warp::header::optional("authorization"))
        .and(warp::path::tail())
        .and(warp::query::raw().map(Some).or(warp::any().map(|| None)).unify())
        .and(warp::header::headers_cloned())
        .and(with_state(state.clone()))
        .and_then(handle_get);

    // Other methods: transparent proxy
    let other_routes = warp::method()
        .and(datalake_prefix.clone())
        .and(warp::path::tail())
        .and(warp::header::headers_cloned())
        .and(warp::body::stream())
        .and(with_state(state.clone()))
        .and_then(|method: Method, path, headers, body, state| async move {
            if method == Method::GET || method == Method::HEAD {
                // If we reached here with GET/HEAD, it means the specific GET/HEAD routes failed to match.
                // This usually happens if the query string was invalid (handled by handle_rejection now)
                // or if some other filter failed.
                // We should NOT process it as a proxy request.
                // We return a rejection so that handle_rejection can pick up the original error (e.g. InvalidQuery).
                Err(warp::reject::not_found())
            } else {
                proxy_to_s3(method, path, headers, body, state).await
            }
        });

    // HEAD route: handle partition-aware HEAD requests
    let head_route = warp::head()
        .and(datalake_prefix.clone())
        .and(warp::header::optional("authorization"))
        .and(warp::path::tail())
        .and(warp::query::raw().map(Some).or(warp::any().map(|| None)).unify())
        .and(warp::header::headers_cloned())
        .and(with_state(state.clone()))
        .and_then(handle_head_wrapper);

    let head_root_route = warp::path::end().and(warp::head()).map(move || {
        debug!("HEAD request for /");
        warp::http::Response::builder()
            .status(StatusCode::OK)
            .body(Body::empty())
            .unwrap()
    });

    let routes = list_buckets_route
        .or(head_root_route)
        .or(head_route)
        .or(get_route)
        .or(other_routes)
        .recover(handle_rejection)
        .with(warp::trace::request());

    let main_server = warp::serve(routes).run(([0, 0, 0, 0], port));

    if let Some(metrics_port) = state.config.metrics_port {
        info!("Starting metrics server on port {}", metrics_port);
        let metrics_server = warp::serve(metrics_route()).run(([0, 0, 0, 0], metrics_port));
        tokio::spawn(metrics_server);
    }

    main_server.await;
}

// ---------------------- Warp helpers ----------------------
fn with_state(
    state: Arc<AppState>,
) -> impl Filter<Extract = (Arc<AppState>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || state.clone())
}

use std::fmt::Write;

fn escape_xml(s: &str) -> String {
    s.replace("&", "&amp;")
     .replace("<", "&lt;")
     .replace(">", "&gt;")
     .replace('"', "&quot;")
     .replace("'", "&apos;")
}

fn list_tables_as_s3_response(state: &Arc<AppState>) -> Box<dyn Reply> {
    let mut xml = String::new();
    write!(&mut xml, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>").unwrap();
    write!(&mut xml, "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"> ").unwrap();
    write!(&mut xml, "<Name>datalake</Name>").unwrap();
    write!(&mut xml, "<Prefix></Prefix>").unwrap();

    let tables: Vec<String> = state.table_mapping.keys().cloned().collect();
    write!(&mut xml, "<KeyCount>{}</KeyCount>", tables.len()).unwrap();
    write!(&mut xml, "<MaxKeys>1000</MaxKeys>", ).unwrap();
    write!(&mut xml, "<IsTruncated>false</IsTruncated>", ).unwrap();

    for table_alias in tables {
        write!(&mut xml, "<CommonPrefixes>").unwrap();
        write!(&mut xml, "<Prefix>{}/</Prefix>", escape_xml(&table_alias)).unwrap();
        write!(&mut xml, "</CommonPrefixes>").unwrap();
    }

    write!(&mut xml, "</ListBucketResult>").unwrap();

    let resp = warp::http::Response::builder()
        .header("Content-Type", "application/xml")
        .body(Body::from(xml))
        .unwrap();

    Box::new(resp)
}

async fn proxy_s3_list(
    s3_client: &S3Client,
    bucket: &str,
    list_prefix: &str,
    base_prefix: &str,
    table_alias: &str,
    query: HashMap<String, String>,
) -> Result<Box<dyn Reply>, warp::Rejection> {
    let mut list_request = s3_client
        .list_objects_v2()
        .bucket(bucket)
        .delimiter("/");

    // Determine the prefix. If query has 'prefix', it takes precedence.
    let final_prefix = if let Some(query_prefix) = query.get("prefix") {
        let client_prefix = query_prefix.strip_prefix(&format!("{}/", table_alias)).unwrap_or(query_prefix);
        format!("{}/{}", base_prefix, client_prefix)
    } else {
        list_prefix.to_string()
    };
    list_request = list_request.prefix(final_prefix.clone());


    if let Some(value) = query.get("start-after") {
        let client_start_after = value.strip_prefix(&format!("{}/", table_alias)).unwrap_or(value);
        list_request = list_request.start_after(format!("{}/{}", base_prefix, client_start_after));
    }
    if let Some(value) = query.get("continuation-token") {
        list_request = list_request.continuation_token(value.clone());
    }
    if let Some(value) = query.get("max-keys") {
        if let Ok(n) = value.parse::<i32>() {
            list_request = list_request.max_keys(n);
        }
    }

    let result = list_request.send().await;

    match result {
        Ok(output) => {
            let mut xml = String::new();
            write!(&mut xml, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>").unwrap();
            write!(&mut xml, "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"> ").unwrap();
            write!(&mut xml, "<Name>{}</Name>", escape_xml("datalake")).unwrap();
            
            let client_prefix = final_prefix
                .strip_prefix(base_prefix) //.and_then(|s| s.strip_prefix("/"))
                .map(|s| format!("{}{}", table_alias, s))
                .unwrap_or_else(|| final_prefix.clone());
            write!(&mut xml, "<Prefix>{}</Prefix>", escape_xml(&client_prefix)).unwrap();
            
            write!(&mut xml, "<KeyCount>{}</KeyCount>", output.key_count.unwrap_or(0)).unwrap();
            write!(&mut xml, "<MaxKeys>{}</MaxKeys>", output.max_keys.unwrap_or(1000)).unwrap();
            write!(&mut xml, "<IsTruncated>{}</IsTruncated>", output.is_truncated.unwrap_or(false)).unwrap();

            for object in output.contents() {
                if let Some(key) = object.key() {
                    let client_key = key
                        .strip_prefix(base_prefix)
                        .or_else(|| key.strip_prefix(&format!("{}/{}", bucket, base_prefix)))
                        .map(|s| format!("{}{}", table_alias, s))
                        .unwrap_or_else(|| key.to_string());
                    write!(&mut xml, "<Contents>").unwrap();
                    write!(&mut xml, "<Key>{}</Key>", escape_xml(&client_key)).unwrap();
                    if let Some(lm) = object.last_modified() {
                        write!(&mut xml, "<LastModified>{}</LastModified>", lm).unwrap();
                    }
                    if let Some(etag) = object.e_tag() {
                        write!(&mut xml, "<ETag>{}</ETag>", escape_xml(etag)).unwrap();
                    }
                    write!(&mut xml, "<Size>{}</Size>", object.size.unwrap_or(0)).unwrap();
                    write!(&mut xml, "<StorageClass>{}</StorageClass>", object.storage_class().map(|s| s.as_str()).unwrap_or("STANDARD")).unwrap();
                    write!(&mut xml, "</Contents>").unwrap();
                }
            }

            for prefix in output.common_prefixes() {
                if let Some(p) = prefix.prefix() {
                    let client_prefix = p
                        .strip_prefix(base_prefix)
                        .map(|s| format!("{}{}", table_alias, s))
                        .unwrap_or_else(|| p.to_string());
                    write!(&mut xml, "<CommonPrefixes>").unwrap();
                    write!(&mut xml, "<Prefix>{}</Prefix>", escape_xml(&client_prefix)).unwrap();
                    write!(&mut xml, "</CommonPrefixes>").unwrap();
                }
            }

            write!(&mut xml, "</ListBucketResult>").unwrap();

            let resp = warp::http::Response::builder()
                .header("Content-Type", "application/xml")
                .body(Body::from(xml))
                .unwrap();

            Ok(Box::new(resp))
        }
        Err(e) => {
            error!("S3 List error: {:?}", e);
            if let aws_sdk_s3::error::SdkError::ServiceError(service_err) = &e {
                error!("S3 Upstream response: {:?}", service_err.raw());
            }
            Ok(Box::new(warp::reply::with_status(
                "S3 List error",
                warp::http::StatusCode::INTERNAL_SERVER_ERROR,
            )))
        }
    }
}

// ---------------------- Caching Helpers ----------------------

// Helper to get a delta table, using a cache
async fn get_delta_table(state: &Arc<AppState>, table_uri: &str) -> Result<Arc<DeltaTable>, warp::Rejection> {
    let cache = &state.delta_cache;
    if let Some(table) = cache.get(table_uri) {
        debug!("DeltaTable cache hit for: {}", table_uri);
        return Ok(table.clone());
    }

    debug!("DeltaTable cache miss for: {}", table_uri);
    let url = Url::parse(table_uri).map_err(|e| {
        error!("Failed to parse table URI {}: {:?}", table_uri, e);
        custom_rejection("Invalid table URI")
    })?;
    
    let fetched_dt = match deltalake::open_table(url).await {
        Ok(dt) => Arc::new(dt),
        Err(e) => {
            error!("Failed to load DeltaTable at {}: {:?}", table_uri, e);
            return Err(custom_rejection("Delta Table Load Error"));
        }
    };
    
    cache.insert(table_uri.to_string(), fetched_dt.clone());
    Ok(fetched_dt)
}

// Gets the list of allowed files for a user and table, using a cache.
async fn get_allowed_files_for_table(
    state: &Arc<AppState>,
    user_id: &Option<String>,
    table_alias: &str,
    table_uri: &str,
    partition_filters_config: &Vec<HashMap<String, String>>,
) -> Result<Arc<Vec<String>>, warp::Rejection> {
    let cache_key = format!("{}:{}", user_id.as_deref().unwrap_or("anonymous"), table_alias);

    // Check cache first
    if let Some(files) = state.file_list_cache.get(&cache_key) {
        debug!("File list cache hit for key: {}", cache_key);
        return Ok(files.clone());
    }

    debug!("File list cache miss for key: {}", cache_key);

    // Cache miss, so we need to load the table and files
    let delta_table = get_delta_table(state, table_uri).await?;
    let mut all_allowed_files = HashSet::new();

    for filter_map in partition_filters_config {
        let partition_filters: Result<Vec<PartitionFilter>, _> = filter_map
            .iter()
            .map(|(k, v)| PartitionFilter::try_from((k.as_str(), "=", v.as_str())))
            .collect();

        match partition_filters {
            Ok(filters) => {
                if filters.is_empty() {
                    continue;
                }
                let files = delta_table.get_file_uris_by_partitions(&filters).await.map_err(|e| {
                    error!("Failed to get files by partition for {}: {:?}", table_uri, e);
                    custom_rejection("Partition file lookup error")
                })?;
                all_allowed_files.extend(files);
            }
            Err(e) => {
                error!("Failed to parse partition filters: {:?}", e);
                return Err(custom_rejection("Invalid partition filter config"));
            }
        }
    }

    let allowed_files = Arc::new(all_allowed_files.into_iter().collect::<Vec<String>>());
    // Store in cache
    state.file_list_cache.insert(cache_key, allowed_files.clone());

    Ok(allowed_files)
}

// ---------------------- Handle GET ----------------------
async fn handle_get(
    auth_header: Option<String>,
    path: warp::path::Tail,
    query_str: Option<String>,
    headers: HeaderMap,
    state: Arc<AppState>,
) -> Result<Box<dyn Reply>, warp::Rejection> {
    state.metrics.inc_queries_served();
    let path_str = path.as_str();
    let user_id = extract_aws_user(auth_header.as_deref());
    if let Some(uid) = &user_id {
        state.metrics.record_user(uid.clone());
    }

    // Parse query string leniently for our internal logic
    let query: HashMap<String, String> = if let Some(qs) = &query_str {
        url::form_urlencoded::parse(qs.as_bytes())
            .into_owned()
            .collect()
    } else {
        HashMap::new()
    };

    debug!("handle_get: path={}, query_raw={:?}, parsed={:?}, user={:?}", path_str, query_str, query, user_id);

    // Handle root listing
    if path_str.is_empty() {
        if let Some(p) = query.get("prefix").cloned() {
            if let Some(table_alias) = p.split('/').next() {
                if let Some((bucket, s3_prefix)) = state.table_mapping.get(table_alias) {
                    return proxy_s3_list(
                        &state.s3_client,
                        bucket,
                        s3_prefix,
                        s3_prefix,
                        table_alias,
                        query,
                    )
                    .await;
                }
            }
        }
        return Ok(list_tables_as_s3_response(&state));
    }

    // Extract table alias and relative file path
    let (table_alias, file_path) = match path_str.split_once('/') {
        Some((alias, path)) => (alias, path),
        None => (path_str, ""),
    };

    let (bucket, s3_prefix) = match state.table_mapping.get(table_alias) {
        Some(val) => val,
        None => {
            warn!("Table alias not found: {}", table_alias);
            return Ok(Box::new(warp::reply::with_status(
                "Table not found",
                warp::http::StatusCode::NOT_FOUND,
            )));
        }
    };

    // If path ends with '/', list files via proxy
    if file_path.ends_with('/')
        || file_path.is_empty()
        || query.get("list-type").map(|s| s.as_str()) == Some("2")
    {
        let s3_key = format!("{}/{}", s3_prefix, file_path);
        return proxy_s3_list(
            &state.s3_client,
            bucket,
            &s3_key,
            s3_prefix,
            table_alias,
            query,
        )
        .await;
    }

    // If accessing catalog/log, proxy directly (no presign)
    if file_path.starts_with("_delta_log/") || file_path == "_delta_log" {
        debug!("Proxying _delta_log request directly for {}", table_alias);
        let s3_key = format!("{}/{}", s3_prefix, file_path);
        return proxy_s3_get(&state.s3_client, bucket, &s3_key, &headers, state.metrics.clone())
            .await
            .map(|r| Box::new(r) as Box<dyn Reply>);
    }

    // Validate credentials if using AWS Auth (Mock/Placeholder)
    if let Some(ref uid) = user_id {
        if auth_header.is_some() {
            let auth_cache = &state.auth_cache;
            if auth_cache.get(uid).is_none() {
                // TODO: Perform actual validation if possible (requires secrets or upstream check)
                auth_cache.insert(uid.clone(), true);
            }
        }
    }

    // Step 1: Check if file is allowed by partition filters
    let table_uri = format!("s3://{}/{}", bucket, s3_prefix);
    let full_s3_path = format!("s3://{}/{}/{}", bucket, s3_prefix, file_path);

    let mut combined_partition_filters = state.config.allowed_partitions.get(table_alias)
        .cloned()
        .unwrap_or_default();

    if state.config.database_enabled {
        if let Some(ref db_pool) = state.db_pool {
            if let Some(uid) = &user_id {
                match db_pool.get_permissions(uid, table_alias).await {
                    Ok(db_filters) => {
                        combined_partition_filters.extend(db_filters);
                    },
                    Err(e) => {
                        error!("Failed to get permissions from DB for user {}: {:?}", uid, e);
                        return Err(custom_rejection("Database permission lookup error"));
                    }
                }
            }
        }
    }


    if !combined_partition_filters.is_empty() {
        let allowed_files = get_allowed_files_for_table(
            &state,
            &user_id,
            table_alias,
            &table_uri,
            &combined_partition_filters,
        )
        .await?;

        if !allowed_files.iter().any(|f| f == &full_s3_path) {
            warn!("Access denied for user {:?} on file {}", user_id, file_path);
            return Ok(Box::new(warp::reply::with_status(
                "Forbidden by partition policy",
                warp::http::StatusCode::FORBIDDEN,
            )));
        }
        info!("Access granted for {} based on partition filter", full_s3_path);
    }

    // Step 2: Decide whether to proxy or presign based on config
    let full_s3_key = format!("{}/{}", s3_prefix, file_path);
    match state.config.get_mode {
        GetMode::Proxy => {
            info!("Proxying GET request for {}", full_s3_key);
            proxy_s3_get(&state.s3_client, bucket, &full_s3_key, &headers, state.metrics.clone())
                .await
                .map(|r| Box::new(r) as Box<dyn Reply>)
        }
        GetMode::PresignedUrl => {
            // If Range header is present, still proxy the request for efficiency
            if let Some(range_header) = headers.get("range") {
                                if state.config.proxy_partial {
                                    info!("Proxying Partial GET request for {}", full_s3_key);
                                    return proxy_s3_get(&state.s3_client, bucket, &full_s3_key, &headers, state.metrics.clone())
                                        .await
                                        .map(|r| Box::new(r) as Box<dyn Reply>);
                                }
                else {
                    if let Ok(range_str) = range_header.to_str() {
                        info!("Presigned URL for partial range request for {}", full_s3_key);
                        let presigned_url = generate_presigned_url(&state.s3_client, bucket, &full_s3_key, Some(range_str)).await;
                        return Ok(Box::new(warp::redirect::temporary(
                            presigned_url.parse::<Uri>().unwrap(),
                        )));
                    }
                }

            }

            info!("Generating presigned URL for {}", full_s3_key);
            let presigned_url = generate_presigned_url(&state.s3_client, bucket, &full_s3_key, None).await;
            Ok(Box::new(warp::redirect::temporary(
                presigned_url.parse::<Uri>().unwrap(),
            )))
        }
        }
    }
    
    async fn handle_head_wrapper(
        _auth_header: Option<String>,
        path: warp::path::Tail,
        _query_str: Option<String>,
        _headers: HeaderMap,
        state: Arc<AppState>,
    ) -> Result<Box<dyn Reply>, warp::Rejection> {
        handle_head(path, state).await
    }
    
    // ---------------------- Handle HEAD ----------------------
    async fn handle_head(
        path: warp::path::Tail,
        state: Arc<AppState>,
    ) -> Result<Box<dyn Reply>, warp::Rejection> {
        state.metrics.inc_queries_served();
        let path_str = path.as_str();
    
        debug!("handle_head: path={}", path_str);
    
        if path_str.is_empty() {
        // This is a HEAD on the bucket. S3 returns 200 OK if bucket exists.
        // Our "datalake" bucket always exists.
        info!("HEAD on bucket successful");
        return Ok(Box::new(warp::reply::with_status(
            "",
            warp::http::StatusCode::OK,
        )));
    }
    
        // Extract table alias and relative file path
        let (table_alias, file_path) = match path_str.split_once('/') {
            Some((alias, path)) => (alias, path),
            None => (path_str, ""),
        };
    
        let (bucket, s3_prefix) = match state.table_mapping.get(table_alias) {
            Some(val) => val,
            None => {
                warn!("Table alias not found for HEAD request: {}", table_alias);
                return Ok(Box::new(warp::reply::with_status(
                    "Table not found",
                    warp::http::StatusCode::NOT_FOUND,
                )));
            }
        };
    
        let full_s3_key = format!("{}/{}", s3_prefix, file_path);
    
        info!("Proxying HEAD request for {}", full_s3_key);
        proxy_s3_head(&state.s3_client, bucket, &full_s3_key, state.metrics.clone())
            .await
            .map(|r| Box::new(r) as Box<dyn Reply>)
    }
    
    
    // ---------------------- Mock: presigned URL ----------------------
async fn generate_presigned_url(client: &S3Client, bucket: &str, key: &str, range_str: Option<&str>) -> String {
    let presigning_config = PresigningConfig::expires_in(Duration::from_secs(300)).unwrap();
    let mut presigned_prereq = client
        .get_object()
        .bucket(bucket)
        .key(key);

    if let Some(range) = range_str {
        presigned_prereq = presigned_prereq.range(range);
    }

    let presigned_req = presigned_prereq   
        .presigned(presigning_config)
        .await
        .expect("Failed to presign request");

    presigned_req.uri().to_string()
}

// ---------------------- Helper: Proxy S3 GET ----------------------
async fn proxy_s3_get(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
    metrics: Arc<AppMetrics>,
) -> Result<Response<Body>, warp::Rejection> {
    let start_time = Instant::now();
    let mut request_builder = s3_client.get_object().bucket(bucket).key(key);
    if let Some(range) = headers.get("range") {
        if let Ok(range_str) = range.to_str() {
            request_builder = request_builder.range(range_str.to_string());
        }
    }
    let output = request_builder.send().await;

    match output {
        Ok(o) => {
            let latency = start_time.elapsed();
            metrics.record_backend_latency(latency);

            if let Some(len) = o.content_length() {
                metrics.record_message_size(len as usize);
            }

            let mut resp_builder = Response::builder();

            // Default to 200 OK, but check for 206 Partial Content
            let mut status = 200;
            if let Some(content_range) = o.content_range() {
                status = 206;
                resp_builder = resp_builder.header("Content-Range", content_range);
            }

            if let Some(ct) = o.content_type() {
                resp_builder = resp_builder.header("content-type", ct);
            }
            if let Some(len) = o.content_length() {
                resp_builder = resp_builder.header("content-length", len.to_string());
            }
            if let Some(etag) = o.e_tag() {
                resp_builder = resp_builder.header("etag", etag);
            }
            if let Some(last_modified) = o.last_modified() {
                resp_builder = resp_builder.header("last-modified", last_modified.fmt(DateTimeFormat::HttpDate).unwrap());
            }

            let body_async_read = o.body.into_async_read();
            let stream = tokio_util::io::ReaderStream::new(body_async_read).map_err(|e| {
                error!("Error reading from S3 ByteStream: {:?}", e);
                Box::new(e) as Box<dyn std::error::Error + Send + Sync>
            });

            Ok(resp_builder
                .status(status)
                .body(Body::wrap_stream(stream))
                .unwrap())
        }
        Err(err) => {
            error!("S3 GET error: {:?}", err);
            if let aws_sdk_s3::error::SdkError::ServiceError(service_err) = &err {
                error!("S3 Upstream response: {:?}", service_err.raw());
            }
            Ok(warp::reply::with_status(
                "S3 GET error",
                warp::http::StatusCode::INTERNAL_SERVER_ERROR,
            )
            .into_response())
        }
    }
}

// ---------------------- Helper: Proxy S3 HEAD ----------------------
async fn proxy_s3_head(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
    metrics: Arc<AppMetrics>,
) -> Result<Response<Body>, warp::Rejection> {
    let start_time = Instant::now();
    let request_builder = s3_client.head_object().bucket(bucket).key(key);
    let output = request_builder.send().await;

    match output {
        Ok(o) => {
            let latency = start_time.elapsed();
            metrics.record_backend_latency(latency);

            let mut resp_builder = Response::builder();

            if let Some(ct) = o.content_type() {
                resp_builder = resp_builder.header("content-type", ct);
            }
            if let Some(len) = o.content_length() {
                resp_builder = resp_builder.header("content-length", len.to_string());
            }
            if let Some(etag) = o.e_tag() {
                resp_builder = resp_builder.header("etag", etag);
            }
            if let Some(last_modified) = o.last_modified() {
                resp_builder = resp_builder.header("last-modified", last_modified.fmt(DateTimeFormat::HttpDate).unwrap());
            }

            // HEAD requests do not have a body
            Ok(resp_builder.status(200).body(Body::empty()).unwrap())
        }
        Err(err) => {
            error!("S3 HEAD error: {:?}", err);
            if let aws_sdk_s3::error::SdkError::ServiceError(service_err) = &err {
                error!("S3 Upstream response: {:?}", service_err.raw());
            }
            Ok(warp::reply::with_status(
                "S3 HEAD error",
                warp::http::StatusCode::INTERNAL_SERVER_ERROR,
            )
            .into_response())
        }
    }
}

// ---------------------- Helper: Proxy S3 Forward (Raw) ----------------------
async fn proxy_s3_forward(
    method: Method,
    bucket: &str,
    key: &str,
    headers: HeaderMap,
    body: impl Stream<Item = Result<impl Buf, warp::Error>> + Send + Sync + 'static,
    client: &reqwest::Client,
    metrics: Arc<AppMetrics>,
) -> Result<Box<dyn Reply>, warp::Rejection> {
    // Construct S3 URL (assuming standard AWS S3)
    let url = format!("https://{}.s3.amazonaws.com/{}", bucket, key);

    let mut req_builder = client.request(
        reqwest::Method::from_bytes(method.as_str().as_bytes()).unwrap(),
        url,
    );

    // Forward headers (exclude Host to let reqwest set it)
    let mut new_headers = reqwest::header::HeaderMap::new();
    for (k, v) in headers.iter() {
        if k != "host" {
            new_headers.insert(
                reqwest::header::HeaderName::from_bytes(k.as_str().as_bytes()).unwrap(),
                reqwest::header::HeaderValue::from_bytes(v.as_bytes()).unwrap(),
            );
        }
    }
    req_builder = req_builder.headers(new_headers);

    // Stream body
    let body = body.map_ok(|mut buf| buf.copy_to_bytes(buf.remaining()));
    req_builder = req_builder.body(reqwest::Body::wrap_stream(body));

    let start_time = Instant::now();

    match req_builder.send().await {
        Ok(resp) => {
            let latency = start_time.elapsed();
            metrics.record_backend_latency(latency);

            if let Some(content_length) = resp.headers().get(reqwest::header::CONTENT_LENGTH) {
                if let Ok(len_str) = content_length.to_str() {
                    if let Ok(len) = len_str.parse::<usize>() {
                        metrics.record_message_size(len);
                    }
                }
            }

            let status = resp.status();
            let headers = resp.headers().clone();
            let stream = resp
                .bytes_stream()
                .map_err(std::io::Error::other);

            let mut response = Response::new(Body::wrap_stream(stream));
            *response.status_mut() = warp::http::StatusCode::from_u16(status.as_u16()).unwrap();

            let mut new_headers = HeaderMap::new();
            for (k, v) in headers.iter() {
                new_headers.insert(
                    warp::http::HeaderName::from_bytes(k.as_str().as_bytes()).unwrap(),
                    warp::http::HeaderValue::from_bytes(v.as_bytes()).unwrap(),
                );
            }
            *response.headers_mut() = new_headers;
            Ok(Box::new(response))
        }
        Err(e) => {
            error!("Forward proxy error: {:?}", e);
            Ok(Box::new(warp::reply::with_status(
                "Upstream Error",
                warp::http::StatusCode::BAD_GATEWAY,
            )))
        }
    }
}

/// Fully streaming proxy for S3
async fn proxy_to_s3(
    method: Method,
    path: warp::path::Tail,
    req_headers: HeaderMap,
    body: impl Stream<Item = Result<impl Buf, warp::Error>> + Send + Sync + 'static,
    state: Arc<AppState>,
) -> Result<Box<dyn Reply>, warp::Rejection> {
    state.metrics.inc_queries_proxied(); // Increment proxied queries
    let path_str = path.as_str();
    debug!("proxy_to_s3: method={} path={}", method, path_str);
    let user_id = extract_aws_user(req_headers.get("authorization").and_then(|h| h.to_str().ok()));
    if let Some(uid) = user_id {
        state.metrics.record_user(uid); // Record unique user
    }

    // Enforce Read-Only Mode
    if state.config.read_only
        && (method == Method::PUT || method == Method::POST || method == Method::DELETE)
    {
        return Ok(Box::new(warp::reply::with_status(
            "Read-only mode enabled",
            warp::http::StatusCode::METHOD_NOT_ALLOWED,
        )));
    }

    if path_str.is_empty() {
        if method == Method::POST || method == Method::PUT {
            return Ok(Box::new(warp::reply::with_status(
                "Root modification denied",
                warp::http::StatusCode::METHOD_NOT_ALLOWED,
            )));
        }
        // Fallback for GET if handle_get didn't catch it (unlikely)
        return Ok(list_tables_as_s3_response(&state));
    }

    let (table_alias, relative_key) = match path_str.split_once('/') {
        Some((alias, path)) => (alias, path),
        None => (path_str, ""),
    };

    let (bucket, s3_prefix) = match state.table_mapping.get(table_alias) {
        Some(val) => val,
        None => {
            warn!("Table alias not found in proxy: {}", table_alias);
            return Ok(Box::new(warp::reply::with_status(
                "Table not found",
                warp::http::StatusCode::NOT_FOUND,
            )));
        }
    };

    let s3_key = format!("{}/{}", s3_prefix, relative_key);
    let s3_client = &state.s3_client;

    // Determine Auth Mode: Check header override, then fallback to global default
    let auth_mode = if let Some(mode_str) = req_headers
        .get("x-proxy-auth-mode")
        .and_then(|h| h.to_str().ok())
    {
        match mode_str.to_lowercase().as_str() {
            "forward" => AuthMode::Forward,
            "iam" => AuthMode::Iam,
            _ => state.config.auth_mode,
        }
    } else {
        state.config.auth_mode
    };

    if auth_mode == AuthMode::Forward || method.as_str() == "PROPFIND" {
        info!("Forwarding {} request for {}", method, s3_key);
        return proxy_s3_forward(
            method,
            bucket,
            &s3_key,
            req_headers,
            body,
            &state.http_client,
            state.metrics.clone(),
        )
        .await;
    }

    match method {
        Method::GET => proxy_s3_get(s3_client, bucket, &s3_key, &req_headers, state.metrics.clone())
            .await
            .map(|r| Box::new(r) as Box<dyn Reply>),

        Method::HEAD => {
            let output = s3_client
                .head_object()
                .bucket(bucket)
                .key(s3_key)
                .send()
                .await;

            match output {
                Ok(o) => {
                    let mut resp = Response::new(Body::empty());
                    if let Some(len) = o.content_length() {
                        if let Ok(len_str) = len.to_string().parse() {
                            resp.headers_mut().insert("content-length", len_str);
                        }
                    }
                    if let Some(ct) = o.content_type() {
                        resp.headers_mut()
                            .insert("content-type", ct.parse().unwrap());
                    }
                    Ok(Box::new(resp))
                }
                Err(err) => {
                    error!("S3 HEAD error: {:?}", err);
                    if let aws_sdk_s3::error::SdkError::ServiceError(service_err) = &err {
                        error!("S3 Upstream response: {:?}", service_err.raw());
                    }
                    Ok(Box::new(warp::reply::with_status(
                        "S3 HEAD error",
                        warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                    )))
                }
            }
        }

        Method::PUT => {
            // Stream request body to S3
            let body_stream = body.map_ok(|mut buf| buf.copy_to_bytes(buf.remaining()));
            let hyper_body = Body::wrap_stream(body_stream);
            let byte_stream = ByteStream::from_body_0_4(hyper_body);

            let output = s3_client
                .put_object()
                .bucket(bucket)
                .key(s3_key)
                .body(byte_stream)
                .send()
                .await;

            match output {
                Ok(_) => Ok(Box::new(warp::reply::with_status(
                    "PUT OK",
                    warp::http::StatusCode::OK,
                ))),
                Err(err) => {
                    error!("S3 PUT error: {:?}", err);
                    if let aws_sdk_s3::error::SdkError::ServiceError(service_err) = &err {
                        error!("S3 Upstream response: {:?}", service_err.raw());
                    }
                    Ok(Box::new(warp::reply::with_status(
                        "S3 PUT error",
                        warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                    )))
                }
            }
        }

        Method::POST => {
            // Stream POST body to S3 as well (use PutObject, adapt if you want multi-part)
            let body_stream = body.map_ok(|mut buf| buf.copy_to_bytes(buf.remaining()));
            let hyper_body = Body::wrap_stream(body_stream);
            let byte_stream = ByteStream::from_body_0_4(hyper_body);

            let output = s3_client
                .put_object()
                .bucket(bucket)
                .key(s3_key)
                .body(byte_stream)
                .send()
                .await;

            match output {
                Ok(_) => Ok(Box::new(warp::reply::with_status(
                    "POST OK",
                    warp::http::StatusCode::OK,
                ))),
                Err(err) => {
                    error!("S3 POST error: {:?}", err);
                    if let aws_sdk_s3::error::SdkError::ServiceError(service_err) = &err {
                        error!("S3 Upstream response: {:?}", service_err.raw());
                    }
                    Ok(Box::new(warp::reply::with_status(
                        "S3 POST error",
                        warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                    )))
                }
            }
        }

        Method::OPTIONS => {
            // Typically CORS preflight → just respond 200
            let mut resp = Response::new(Body::empty());
            *resp.status_mut() = warp::http::StatusCode::OK;
            Ok(Box::new(resp))
        }

        _ => {
            warn!("Unsupported method received: {}", method);
            Ok(Box::new(warp::reply::with_status(
            "Method Not Allowed",
            warp::http::StatusCode::METHOD_NOT_ALLOWED,
        )))},
    }
}

// ---------------------- Auth Helper ----------------------
fn extract_aws_user(auth_header: Option<&str>) -> Option<String> {
    // Header format: AWS4-HMAC-SHA256 Credential=<AccessKeyID>/<date>/<region>/<service>/aws4_request, ...
    let header = auth_header?;
    let credential_part = header.split("Credential=").nth(1)?;
    let access_key = credential_part.split('/').next()?;
    Some(access_key.to_string())
}

async fn handle_rejection(err: warp::Rejection) -> Result<impl Reply, std::convert::Infallible> {
    if let Some(CustomError(msg)) = err.find() {
        error!("Request rejected: {}", msg);
        Ok(warp::reply::with_status(
            msg.clone(),
            StatusCode::INTERNAL_SERVER_ERROR,
        ))
    } else if err.is_not_found() {
        Ok(warp::reply::with_status(
            "Not Found".to_string(),
            StatusCode::NOT_FOUND,
        ))
    } else if let Some(_) = err.find::<warp::filters::body::BodyDeserializeError>() {
        Ok(warp::reply::with_status(
            "Bad Request".to_string(),
            StatusCode::BAD_REQUEST,
        ))
    } else if let Some(_) = err.find::<warp::reject::InvalidQuery>() {
        error!("Invalid Query String: {:?}", err);
        Ok(warp::reply::with_status(
            "Invalid Query String".to_string(),
            StatusCode::BAD_REQUEST,
        ))
    } else if let Some(_) = err.find::<warp::reject::MethodNotAllowed>() {
        error!("Method Not Allowed: {:?}", err);
        Ok(warp::reply::with_status(
            "Method Not Allowed".to_string(),
            StatusCode::METHOD_NOT_ALLOWED,
        ))
    } else {
        error!("Unhandled rejection: {:?}", err);
        Ok(warp::reply::with_status(
            "Internal Server Error".to_string(),
            StatusCode::INTERNAL_SERVER_ERROR,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::config::{Region};
    
    use aws_sdk_s3::Client as S3Client;
    use aws_sdk_s3::config::SharedCredentialsProvider;
    use mockito;
    use mockito::Matcher;
    use std::collections::HashMap;
    use warp::test::request;

    // Helper function to create an AppState with a mock S3 client
    async fn test_app_state(s3_endpoint: &str) -> AppState {
        let aws_config = aws_config::load_from_env()
            .await
            .to_builder()
            .endpoint_url(s3_endpoint)
            .credentials_provider(SharedCredentialsProvider::new(aws_sdk_s3::config::Credentials::for_tests()))
            .region(Region::new("us-east-1"))
            .build();
        let s3_client = S3Client::new(&aws_config);
        let http_client = reqwest::Client::new();

        let settings = ::config::Config::builder()
            .add_source(::config::File::with_name("test_config").required(true))
            .add_source(::config::Environment::with_prefix("PROXY"))
            .build()
            .unwrap();

        let config: Config = settings.try_deserialize().unwrap();

        let mut table_mapping = HashMap::new();
        for (alias, uri) in &config.table_mapping {
            let uri_trim = uri.trim_start_matches("s3://");
            if let Some((bucket, prefix)) = uri_trim.split_once('/') {
                let prefix = prefix.trim_end_matches('/');
                table_mapping.insert(alias.clone(), (bucket.to_string(), prefix.to_string()));
            }
        }

        let db_pool = if config.database_enabled {
            Some(DBPool::new(&config).await.unwrap())
        } else {
            None
        };

        let metrics = AppMetrics::new();

        AppState {
            s3_client,
            http_client,
            table_mapping,
            delta_cache: Cache::builder()
                .max_capacity(20)
                .time_to_live(Duration::from_secs(120))
                .build(),
            auth_cache: Cache::builder()
                .max_capacity(100)
                .time_to_live(Duration::from_secs(120))
                .build(),
            file_list_cache: Cache::builder()
                .max_capacity(100)
                .time_to_live(Duration::from_secs(120))
                .build(),
            config,
            db_pool,
            metrics,
        }
    }

    #[tokio::test]
    async fn test_list_path_mapping() {
        let mut server = mockito::Server::new_async().await;
        let s3_endpoint = server.url();


        let _m = server.mock("GET", Matcher::Regex(r"/uuuid1/?\?.*list-type=2.*prefix=dfdf%2Fuuid2%2F_delta_log%2F.*".to_string()))
            .with_status(200)
            .with_header("content-type", "application/xml")
            .with_body(
                r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-0_3_01/">
  <Name>test-bucket</Name>
  <Prefix>uuuid1/dfdf/uuid2/_delta_log/</Prefix>
  <KeyCount>1</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>dfdf/uuid2/_delta_log/00000000000000000000.json</Key>
    <LastModified>2024-01-01T00:00:00.000Z</LastModified>
    <ETag>"etag"</ETag>
    <Size>123</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
</ListBucketResult>"#,
            )
            .create();

        let state = Arc::new(test_app_state(&s3_endpoint).await);
        let routes = warp::get()
            .and(warp::path("datalake"))
            .and(warp::header::optional("authorization"))
            .and(warp::path::tail())
            .and(warp::query::raw().map(Some).or(warp::any().map(|| None)).unify())
            .and(warp::header::headers_cloned())
            .and(with_state(state.clone()))
            .and_then(|auth_header, path, query, headers, state| handle_get(auth_header, path, query, headers, state));

        let res = request()
            .method("GET")
            .path("/datalake/table/_delta_log/?list-type=2")
            .reply(&routes)
            .await;

        assert_eq!(res.status(), 200);
        let body = String::from_utf8(res.body().to_vec()).unwrap();
        println!("Body: {}", body);
        assert!(body.contains("<Prefix>table/_delta_log/</Prefix>"));
        assert!(body.contains("<Key>table/_delta_log/00000000000000000000.json</Key>"));
    }

    #[tokio::test]
    async fn test_proxy_s3_list_with_prefix_and_start_after() {
        let mut server = mockito::Server::new_async().await;
        let s3_endpoint = server.url();

        let _m = server.mock("GET", Matcher::Regex(r"/uuuid1/?\?.*list-type=2.*prefix=dfdf%2Fuuid2%2Fsome%2Fprefix.*start-after=dfdf%2Fuuid2%2Fsome%2Fkey.*".to_string()))
            .with_status(200)
            .with_header("content-type", "application/xml")
            .with_body(
                r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>test-bucket</Name>
  <Prefix>uuuid1/dfdf/uuid2/some/prefix</Prefix>
  <StartAfter>uuuid1/dfdf/uuid2/some/key</StartAfter>
  <KeyCount>1</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>uuuid1/dfdf/uuid2/some/prefix/another.json</Key>
    <LastModified>2024-01-01T00:00:00.000Z</LastModified>
    <ETag>"etag"</ETag>
    <Size>123</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
</ListBucketResult>"#,
            )
            .create();

        let state = Arc::new(test_app_state(&s3_endpoint).await);
        let routes = warp::get()
            .and(warp::path("datalake"))
            .and(warp::header::optional("authorization"))
            .and(warp::path::tail())
            .and(warp::query::raw().map(Some).or(warp::any().map(|| None)).unify())
            .and(warp::header::headers_cloned())
            .and(with_state(state.clone()))
            .and_then(|auth_header, path, query, headers, state| handle_get(auth_header, path, query, headers, state));

        let res = request()
            .method("GET")
            .path("/datalake/table/?list-type=2&prefix=some/prefix&start-after=some/key")
            .reply(&routes)
            .await;

        assert_eq!(res.status(), 200);
        let body = String::from_utf8(res.body().to_vec()).unwrap();
        println!("Body: {}", body);
        assert!(body.contains("<Prefix>table/some/prefix</Prefix>"));
        assert!(body.contains("<Key>table/some/prefix/another.json</Key>"));
    }

    #[tokio::test]
    async fn test_proxy_s3_list_with_full_path_prefix_and_start_after() {
        let mut server = mockito::Server::new_async().await;
        let s3_endpoint = server.url();

        let _m = server.mock("GET", Matcher::Regex(r"/uuuid1/?\?.*list-type=2.*prefix=dfdf%2Fuuid2%2Fsome%2Fprefix.*start-after=dfdf%2Fuuid2%2Fsome%2Fkey.*".to_string()))
            .with_status(200)
            .with_header("content-type", "application/xml")
            .with_body(
                r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>test-bucket</Name>
  <Prefix>uuuid1/dfdf/uuid2/some/prefix</Prefix>
  <StartAfter>uuuid1/dfdf/uuid2/some/key</StartAfter>
  <KeyCount>1</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>uuuid1/dfdf/uuid2/some/prefix/another.json</Key>
    <LastModified>2024-01-01T00:00:00.000Z</LastModified>
    <ETag>"etag"</ETag>
    <Size>123</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
</ListBucketResult>"#,
            )
            .create();

        let state = Arc::new(test_app_state(&s3_endpoint).await);
        let routes = warp::get()
            .and(warp::path("datalake"))
            .and(warp::header::optional("authorization"))
            .and(warp::path::tail())
            .and(warp::query::raw().map(Some).or(warp::any().map(|| None)).unify())
            .and(warp::header::headers_cloned())
            .and(with_state(state.clone()))
            .and_then(|auth_header, path, query, headers, state| handle_get(auth_header, path, query, headers, state));

        let res = request()
            .method("GET")
            .path("/datalake/table/?list-type=2&prefix=table/some/prefix&start-after=table/some/key")
            .reply(&routes)
            .await;

        assert_eq!(res.status(), 200);
        let body = String::from_utf8(res.body().to_vec()).unwrap();
        println!("Body: {}", body);
        assert!(body.contains("<Prefix>table/some/prefix</Prefix>"));
        assert!(body.contains("<Key>table/some/prefix/another.json</Key>"));
    }

    #[tokio::test]
    async fn test_get_with_range_header() {
        let mut server = mockito::Server::new_async().await;
        let s3_endpoint = server.url();
        let response_body = "partial content";

        let _m = server.mock("GET", Matcher::Regex(r"/some-other-bucket/path/some/file.parquet.*".to_string()))
            .match_header("range", "bytes=0-14")
            .with_status(206)
            .with_header("Content-Range", "bytes 0-14/100")
            .with_body(response_body)
            .create();

        let state = Arc::new(test_app_state(&s3_endpoint).await);
        let routes = warp::get()
            .and(warp::path("datalake"))
            .and(warp::header::optional("authorization"))
            .and(warp::path::tail())
            .and(warp::query::raw().map(Some).or(warp::any().map(|| None)).unify())
            .and(warp::header::headers_cloned())
            .and(with_state(state.clone()))
            .and_then(|auth_header, path, query, headers, state| handle_get(auth_header, path, query, headers, state));

        let res = request()
            .method("GET")
            .path("/datalake/other_table/some/file.parquet")
            .header("range", "bytes=0-14")
            .reply(&routes)
            .await;

        assert_eq!(res.status(), 206);
        assert_eq!(res.headers().get("Content-Range").unwrap(), "bytes 0-14/100");
        let body = String::from_utf8(res.body().to_vec()).unwrap();
        assert_eq!(body, response_body);
    }

    #[tokio::test]
    async fn test_head_request_handled_correctly() {
        let mut server = mockito::Server::new_async().await;
        let s3_endpoint = server.url();

        // Mock the S3 HEAD response
        let _m = server.mock("HEAD", Matcher::Regex(r"/uuuid1/dfdf/uuid2/_delta_log/00000000000000000000.json".to_string()))
            .with_status(200)
            .with_header("Content-Length", "123")
            .with_header("ETag", "\"some-etag\"")
            .create();

        let state = Arc::new(test_app_state(&s3_endpoint).await);
        let datalake_prefix = warp::path("datalake");

        // Define routes similar to main() to test routing logic
        let head_route = warp::head()
            .and(datalake_prefix.clone())
            .and(warp::header::optional("authorization"))
            .and(warp::path::tail())
            .and(warp::query::raw().map(Some).or(warp::any().map(|| None)).unify())
            .and(warp::header::headers_cloned())
            .and(with_state(state.clone()))
            .and_then(handle_head_wrapper);

        let get_route = warp::get()
            .and(datalake_prefix.clone())
            .and(warp::header::optional("authorization"))
            .and(warp::path::tail())
            .and(warp::query::raw().map(Some).or(warp::any().map(|| None)).unify())
            .and(warp::header::headers_cloned())
            .and(with_state(state.clone()))
            .and_then(handle_get);

        let other_routes = warp::method()
            .and_then(|method: Method| async move {
                if method == Method::GET || method == Method::HEAD {
                    Err(warp::reject())
                } else {
                    Ok(method)
                }
            })
            .and(datalake_prefix.clone())
            .and(warp::path::tail())
            .and(warp::header::headers_cloned())
            .and(warp::body::stream())
            .and(with_state(state.clone()))
            .and_then(|method, path, headers, body, state| {
                proxy_to_s3(method, path, headers, body, state)
            });
        
        let routes = head_route.or(get_route).or(other_routes);

        let res = request()
            .method("HEAD")
            .path("/datalake/table/_delta_log/00000000000000000000.json")
            .reply(&routes)
            .await;

        assert_eq!(res.status(), 200, "HEAD request should return 200 OK");
        assert_eq!(res.headers().get("Content-Length").unwrap(), "123");
        assert_eq!(res.headers().get("ETag").unwrap(), "\"some-etag\"");
    }

    #[tokio::test]
    async fn test_head_request_on_bucket() {
        let server = mockito::Server::new_async().await;
        let s3_endpoint = server.url();

        let state = Arc::new(test_app_state(&s3_endpoint).await);
        let datalake_prefix = warp::path("datalake");

        let head_route = warp::head()
            .and(datalake_prefix.clone())
            .and(warp::header::optional("authorization"))
            .and(warp::path::tail())
            .and(warp::query::raw().map(Some).or(warp::any().map(|| None)).unify())
            .and(warp::header::headers_cloned())
            .and(with_state(state.clone()))
            .and_then(handle_head_wrapper);
        
        let routes = head_route;

        let res = request()
            .method("HEAD")
            .path("/datalake/")
            .reply(&routes)
            .await;

        assert_eq!(res.status(), 200, "HEAD request on bucket should return 200 OK");
    }

    #[tokio::test]
    async fn test_head_request_on_table() {
        let mut server = mockito::Server::new_async().await;
        let s3_endpoint = server.url();

        // Mock the S3 HEAD response for the object that represents the table.
        let _m = server.mock("HEAD", Matcher::Regex(r"/uuuid1/dfdf/uuid2/".to_string()))
            .with_status(200)
            .with_header("Content-Length", "0")
            .with_header("ETag", "\"table-etag\"")
            .create();

        let state = Arc::new(test_app_state(&s3_endpoint).await);
        let datalake_prefix = warp::path("datalake");

        let head_route = warp::head()
            .and(datalake_prefix.clone())
            .and(warp::header::optional("authorization"))
            .and(warp::path::tail())
            .and(warp::query::raw().map(Some).or(warp::any().map(|| None)).unify())
            .and(warp::header::headers_cloned())
            .and(with_state(state.clone()))
            .and_then(handle_head_wrapper);
        
        let routes = head_route;

        let res = request()
            .method("HEAD")
            .path("/datalake/table/")
            .reply(&routes)
            .await;

        assert_eq!(res.status(), 200, "HEAD request on table should return 200 OK");
    }

    #[tokio::test]
    async fn test_head_request_non_existent_table() {
        let server = mockito::Server::new_async().await;
        let s3_endpoint = server.url();

        let state = Arc::new(test_app_state(&s3_endpoint).await);
        let datalake_prefix = warp::path("datalake");

        let head_route = warp::head()
            .and(datalake_prefix.clone())
            .and(warp::header::optional("authorization"))
            .and(warp::path::tail())
            .and(warp::query::raw().map(Some).or(warp::any().map(|| None)).unify())
            .and(warp::header::headers_cloned())
            .and(with_state(state.clone()))
            .and_then(handle_head_wrapper);
        
        let routes = head_route;

        let res = request()
            .method("HEAD")
            .path("/datalake/non_existent_table/")
            .reply(&routes)
            .await;

        assert_eq!(res.status(), 404, "HEAD request on non-existent table should return 404 Not Found");
    }

    #[tokio::test]
    async fn test_head_request_non_existent_file() {
        let mut server = mockito::Server::new_async().await;
        let s3_endpoint = server.url();

        // Mock the S3 HEAD response for the non-existent object.
        let _m = server.mock("HEAD", Matcher::Regex(r"/uuuid1/dfdf/uuid2/non_existent_file".to_string()))
            .with_status(404)
            .create();

        let state = Arc::new(test_app_state(&s3_endpoint).await);
        let datalake_prefix = warp::path("datalake");

        let head_route = warp::head()
            .and(datalake_prefix.clone())
            .and(warp::header::optional("authorization"))
            .and(warp::path::tail())
            .and(warp::query::raw().map(Some).or(warp::any().map(|| None)).unify())
            .and(warp::header::headers_cloned())
            .and(with_state(state.clone()))
            .and_then(handle_head_wrapper);
        
        let routes = head_route;

        let res = request()
            .method("HEAD")
            .path("/datalake/table/non_existent_file")
            .reply(&routes)
            .await;

        assert_eq!(res.status(), 500, "HEAD request on non-existent file should return 500");
    }
}
