use crate::config::Config;
use crate::permission;
use cached::SizedCache;
use cached::proc_macro::cached;
use sea_orm::{ColumnTrait, Database, DatabaseConnection, DbErr, EntityTrait, QueryFilter};
use std::collections::HashMap;

#[derive(Clone)]
pub struct DBPool {
    conn: DatabaseConnection,
}

impl DBPool {
    pub async fn new(config: &Config) -> Result<Self, DbErr> {
        let conn = Database::connect(&config.database.uri).await?;
        Ok(Self { conn })
    }

    pub async fn get_permissions(
        &self,
        user_id: &str,
        table_name: &str,
    ) -> Result<Vec<HashMap<String, String>>, String> {
        _get_permissions_from_db(&self.conn, user_id, table_name).await
    }
}

#[cached(
    ty = "SizedCache<String, Result<Vec<HashMap<String, String>>, String>>",
    create = "{ SizedCache::with_size(100) }",
    convert = r#"{ format!("{}:{}", user_id, table_name) }"#
)]
async fn _get_permissions_from_db(
    conn: &DatabaseConnection,
    user_id: &str,
    table_name: &str,
) -> Result<Vec<HashMap<String, String>>, String> {
    let permissions = permission::Entity::find()
        .filter(permission::Column::UserId.eq(user_id))
        .filter(permission::Column::TableName.eq(table_name))
        .all(conn)
        .await
        .map_err(|e| e.to_string())?;

    let mut partition_filters = Vec::new();
    for p in permissions {
        if let Ok(filters) = serde_json::from_value(p.partition_filters) {
            partition_filters.push(filters);
        }
    }
    Ok(partition_filters)
}
