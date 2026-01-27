use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    #[serde(rename = "table_mapping")]
    pub table_mapping: HashMap<String, String>,
    #[serde(default = "default_read_only")]
    pub read_only: bool,
    #[serde(default = "default_proxy_partial")]
    pub proxy_partial: bool,
    #[serde(default)]
    pub default_auth_mode: AuthMode,
    #[serde(default)]
    pub get_mode: GetMode,
    #[serde(default)]
    pub allowed_partitions: HashMap<String, Vec<HashMap<String, String>>>,
    #[serde(default = "default_port")]
    pub port: u16,
}

fn default_port() -> u16 {
    18080
}


fn default_read_only() -> bool {
    true
}

fn default_proxy_partial() -> bool {
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
