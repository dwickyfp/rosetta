use super::client::SnowpipeClient;
use crate::config::SnowflakeConfig;
use etl::destination::Destination;
use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use etl::types::{Cell, Event, TableId, TableRow};
use serde_json::{Value, json};
use sqlx::{Pool, Postgres};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

#[derive(Debug, Clone)]
pub struct SnowflakeDestination {
    client: Arc<Mutex<SnowpipeClient>>,
    current_token: Arc<Mutex<HashMap<TableId, String>>>,
    pg_pool: Pool<Postgres>,
    table_cache: Arc<Mutex<HashMap<TableId, String>>>,
}

// Helper: Convert ETL Cell to JSON
fn row_to_json_values(row: &TableRow) -> Vec<Value> {
    row.values
        .iter()
        .map(|cell| match cell {
            Cell::Null => Value::Null,
            Cell::Bool(v) => json!(v),
            Cell::String(v) => json!(v),
            Cell::I16(v) => json!(v),
            Cell::I32(v) => json!(v),
            Cell::I64(v) => json!(v),
            Cell::F32(v) => json!(v),
            Cell::F64(v) => json!(v),
            Cell::Bytes(v) => json!(format!("<bytes len={}>", v.len())),
            Cell::Json(v) => v.clone(),
            Cell::Numeric(v) => json!(v.to_string()), // Attempt conversion
            Cell::Array(v) => match v {
                etl::types::ArrayCell::Bool(list) => json!(list),
                etl::types::ArrayCell::I16(list) => json!(list),
                etl::types::ArrayCell::I32(list) => json!(list),
                etl::types::ArrayCell::I64(list) => json!(list),
                etl::types::ArrayCell::F32(list) => json!(list),
                etl::types::ArrayCell::F64(list) => json!(list),
                etl::types::ArrayCell::String(list) => json!(list),
                etl::types::ArrayCell::Numeric(list) => json!(
                    list.iter()
                        .map(|opt| opt.as_ref().map(|n| n.to_string()))
                        .collect::<Vec<_>>()
                ),
                etl::types::ArrayCell::Date(list) => json!(
                    list.iter()
                        .map(|opt| opt.as_ref().map(|d| d.to_string()))
                        .collect::<Vec<_>>()
                ),
                etl::types::ArrayCell::TimestampTz(list) => json!(
                    list.iter()
                        .map(|opt| opt.as_ref().map(|t| t.to_rfc3339()))
                        .collect::<Vec<_>>()
                ),
                _ => json!(format!("{:?}", v)), // Fallback for Bytes, Uuid, etc.
            }, // Attempt conversion
            Cell::Date(v) => json!(v.to_string()),
            Cell::TimestampTz(v) => json!(v.to_rfc3339()),
            _ => json!(format!("{:?}", cell)),
        })
        .collect()
}

impl SnowflakeDestination {
    pub fn new(config: SnowflakeConfig, pg_pool: Pool<Postgres>) -> EtlResult<Self> {
        // Init client (akan hitung fingerprint di sini)
        let client = SnowpipeClient::new(config)
            .map_err(|e| etl_error!(ErrorKind::Unknown, "Client init error: {}", e))?;

        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            current_token: Arc::new(Mutex::new(HashMap::new())),
            pg_pool,
            table_cache: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    async fn resolve_table_name(&self, table_id: TableId) -> String {
        let mut cache = self.table_cache.lock().await;
        if let Some(name) = cache.get(&table_id) {
            return name.clone();
        }

        // Query Postgres for table name
        let query = "SELECT cast($1::regclass as text)";
        let row: Option<String> = sqlx::query_scalar(query)
            .bind(table_id.0 as i32)
            .fetch_optional(&self.pg_pool)
            .await
            .unwrap_or(None);

        let table_name = if let Some(raw_name) = row {
            // Handle schema.table format if present, usually regclass returns just name if in search path,
            // or schema.name if not. We just want the name part for now, or full?
            // User req: LANDING_<TABLE_NAME_UPPER>
            // Let's assume we strip schema if present for simplicity or take full.
            // Usually just taking the last part is safer if we want a flat structure.
            let name_part = raw_name.split('.').last().unwrap_or(&raw_name);
            format!("LANDING_{}", name_part.to_uppercase())
        } else {
            format!("LANDING_UNKNOWN_{}", table_id)
        };

        info!(
            "Resolved TableId {} to Snowflake Table {}",
            table_id, table_name
        );
        cache.insert(table_id, table_name.clone());
        table_name
    }
}

impl Destination for SnowflakeDestination {
    fn name() -> &'static str {
        "snowflake_streaming_rest"
    }

    async fn truncate_table(&self, _table_id: TableId) -> EtlResult<()> {
        info!("Truncate ignored (Append-only)");
        Ok(())
    }

    async fn write_table_rows(&self, table_id: TableId, rows: Vec<TableRow>) -> EtlResult<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let table_name = self.resolve_table_name(table_id).await;
        let mut client = self.client.lock().await;
        let mut tokens = self.current_token.lock().await;

        let token = tokens.entry(table_id).or_insert_with(String::new);

        if token.is_empty() {
            *token = client
                .open_channel(&table_name, "default")
                .await
                .map_err(|e| etl_error!(ErrorKind::Unknown, "Open channel failed: {}", e))?;
        }

        let json_rows: Vec<Value> = rows
            .iter()
            .map(|r| {
                json!({
                    "record": {
                        "op": "r",
                        "data": row_to_json_values(r)
                    }
                })
            })
            .collect();

        let next_token = client
            .insert_rows(&table_name, "default", json_rows, Some(token.clone()))
            .await
            .map_err(|e| etl_error!(ErrorKind::Unknown, "Write rows failed: {}", e))?;

        *token = next_token;
        Ok(())
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        // Group events by table_id
        let mut events_by_table: HashMap<TableId, Vec<Event>> = HashMap::new();
        for event in events {
            let table_id = match &event {
                Event::Insert(i) => Some(i.table_id),
                Event::Update(u) => Some(u.table_id),
                Event::Delete(d) => Some(d.table_id),
                _ => None,
            };

            if let Some(tid) = table_id {
                events_by_table.entry(tid).or_default().push(event);
            }
        }

        for (table_id, events) in events_by_table {
            let table_name = self.resolve_table_name(table_id).await;

            let mut client = self.client.lock().await;
            let mut tokens = self.current_token.lock().await;

            let token = tokens.entry(table_id).or_insert_with(String::new);

            if token.is_empty() {
                *token = client
                    .open_channel(&table_name, "default")
                    .await
                    .map_err(|e| etl_error!(ErrorKind::Unknown, "Open channel failed: {}", e))?;
            }

            let mut json_rows = Vec::new();
            for event in events {
                let payload = match event {
                    Event::Insert(i) => Some(json!({
                        "op": "c",
                        "data": row_to_json_values(&i.table_row)
                    })),
                    Event::Update(u) => Some(json!({
                        "op": "u",
                        "data": row_to_json_values(&u.table_row),
                        "before": u.old_table_row.as_ref().map(|(_, r)| row_to_json_values(r))
                    })),
                    Event::Delete(d) => {
                        let old_data = d
                            .old_table_row
                            .as_ref()
                            .map(|(_, r)| row_to_json_values(r))
                            .unwrap_or_default();
                        Some(json!({
                            "op": "d",
                            "data": old_data
                        }))
                    }
                    _ => None,
                };

                if let Some(p) = payload {
                    json_rows.push(json!({ "record": p }));
                }
            }

            if !json_rows.is_empty() {
                let next_token = client
                    .insert_rows(&table_name, "default", json_rows, Some(token.clone()))
                    .await
                    .map_err(|e| etl_error!(ErrorKind::Unknown, "Write events failed: {}", e))?;
                *token = next_token;
            }
        }

        Ok(())
    }
}
