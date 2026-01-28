use prometheus::{opts, register_counter, register_gauge, Counter, Gauge, Encoder};
use std::collections::HashSet;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::time::interval;
use warp::{Filter, Rejection, Reply};

static METRICS: OnceLock<Arc<AppMetrics>> = OnceLock::new();

pub struct AppMetrics {
    queries_served_total: Counter,
    queries_proxied_total: Counter,
    // raw data for minute-based aggregation
    raw_user_ids_this_minute: Arc<Mutex<HashSet<String>>>,
    raw_backend_latencies_this_minute: Arc<Mutex<Vec<Duration>>>,
    raw_message_sizes_this_minute: Arc<Mutex<Vec<usize>>>,
    // aggregated metrics exposed via Prometheus
    unique_users_last_minute: Gauge,
    backend_reply_latency_seconds_avg: Gauge, // Using Gauge for simple average for now
    average_message_size_bytes: Gauge,
}

impl AppMetrics {
    pub fn new() -> Arc<Self> {
        METRICS.get_or_init(|| {
            let queries_served_total = register_counter!(opts!(
                "queries_served_total",
                "Total number of queries served."
            ))
            .unwrap();
            let queries_proxied_total = register_counter!(opts!(
                "queries_proxied_total",
                "Total number of queries proxied."
            ))
            .unwrap();

            let unique_users_last_minute = register_gauge!(opts!(
                "unique_users_last_minute",
                "Number of unique users in the last minute."
            ))
            .unwrap();
            let backend_reply_latency_seconds_avg = register_gauge!(opts!(
                "backend_reply_latency_seconds_avg",
                "Average backend reply latency in seconds over the last minute."
            ))
            .unwrap();
            let average_message_size_bytes = register_gauge!(opts!(
                "average_message_size_bytes",
                "Average message size in bytes over the last minute."
            ))
            .unwrap();

            Arc::new(AppMetrics {
                queries_served_total,
                queries_proxied_total,
                raw_user_ids_this_minute: Arc::new(Mutex::new(HashSet::new())),
                raw_backend_latencies_this_minute: Arc::new(Mutex::new(Vec::new())),
                raw_message_sizes_this_minute: Arc::new(Mutex::new(Vec::new())),
                unique_users_last_minute,
                backend_reply_latency_seconds_avg,
                average_message_size_bytes,
            })
        }).clone()
    }

    pub fn inc_queries_served(&self) {
        self.queries_served_total.inc();
    }

    pub fn inc_queries_proxied(&self) {
        self.queries_proxied_total.inc();
    }

    pub fn record_user(&self, user_id: String) {
        let mut users = self.raw_user_ids_this_minute.lock().unwrap();
        users.insert(user_id);
    }

    pub fn record_backend_latency(&self, duration: Duration) {
        let mut latencies = self.raw_backend_latencies_this_minute.lock().unwrap();
        latencies.push(duration);
    }

    pub fn record_message_size(&self, size: usize) {
        let mut sizes = self.raw_message_sizes_this_minute.lock().unwrap();
        sizes.push(size);
    }

    // This function will run in a separate Tokio task to aggregate metrics every minute
    pub async fn start_aggregation_task(metrics: Arc<AppMetrics>) {
        let mut interval = interval(Duration::from_secs(60));
        // Skip the first tick to align with minute boundaries after the first full minute
        interval.tick().await; 

        loop {
            interval.tick().await; // Wait for the next minute boundary

            // Aggregate unique users
            let unique_users_count = {
                let mut users = metrics.raw_user_ids_this_minute.lock().unwrap();
                let count = users.len();
                users.clear(); // Reset for the next minute
                count
            };
            metrics.unique_users_last_minute.set(unique_users_count as f64);

            // Aggregate backend latencies
            let avg_latency = {
                let mut latencies = metrics.raw_backend_latencies_this_minute.lock().unwrap();
                let total_duration: Duration = latencies.iter().sum();
                let count = latencies.len();
                latencies.clear(); // Reset for the next minute
                if count > 0 {
                    total_duration.as_secs_f64() / count as f64
                } else {
                    0.0
                }
            };
            metrics.backend_reply_latency_seconds_avg.set(avg_latency);

            // Aggregate message sizes
            let avg_message_size = {
                let mut sizes = metrics.raw_message_sizes_this_minute.lock().unwrap();
                let total_size: usize = sizes.iter().sum();
                let count = sizes.len();
                sizes.clear(); // Reset for the next minute
                if count > 0 {
                    total_size as f64 / count as f64
                } else {
                    0.0
                }
            };
            metrics.average_message_size_bytes.set(avg_message_size);

            println!("Metrics aggregated for the last minute."); // For debugging
        }
    }
}

pub async fn metrics_handler() -> Result<impl Reply, Rejection> {
    let encoder = prometheus::TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    let response = String::from_utf8(buffer).unwrap();
    Ok(warp::reply::with_header(
        response,
        "Content-Type",
        encoder.format_type(),
    ))
}

pub fn metrics_route() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::path!("metrics")
        .and(warp::get())
        .and_then(metrics_handler)
}