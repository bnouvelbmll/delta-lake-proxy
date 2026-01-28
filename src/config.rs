use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    pub table_mapping: HashMap<String, String>,
    #[serde(default = "read_only")]
    pub read_only: bool,
    #[serde(default = "proxy_partial")]
    pub proxy_partial: bool,
    #[serde(default)]
    pub auth_mode: AuthMode,
    #[serde(default)]
    pub get_mode: GetMode,
    #[serde(default)]
    pub allowed_partitions: HashMap<String, Vec<HashMap<String, String>>>,
    #[serde(default = "port")]
    pub port: u16,
    #[serde(default = "metrics_port")]
    pub metrics_port: Option<u16>,
    #[serde(default)]
    pub database: Database,
    #[serde(default = "database_enabled")]
    pub database_enabled: bool,
}

fn port() -> u16 {
    18080
}

fn metrics_port() -> Option<u16> {
    Some(9090) // Default metrics port
}

fn read_only() -> bool {
    true
}

fn proxy_partial() -> bool {
    false
}

fn database_enabled() -> bool {
    false
}


#[derive(Deserialize, Clone, Copy, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum AuthMode {
    Iam,
    Forward,
}

impl Default for AuthMode {
    fn default() -> Self {
        AuthMode::Iam
    }
}

#[derive(Deserialize, Clone, Copy, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum GetMode {
    Proxy,
    PresignedUrl,
}

impl Default for GetMode {
    fn default() -> Self {
        GetMode::PresignedUrl
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Database {
    #[serde(default = "db_uri")]
    pub uri: String,
}

fn db_uri() -> String {
    "sqlite:delta_proxy.db".to_string()
}

impl Default for Database {
    fn default() -> Self {
        Self {
            uri: db_uri(),
        }
    }
}
