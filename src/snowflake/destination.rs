use super::client::SnowpipeClient;
use crate::config::SnowflakeConfig;
use etl::destination::Destination;
use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use etl::types::{Cell, Event, TableId, TableRow};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

#[derive(Debug, Clone)]
pub struct SnowflakeDestination {
    client: Arc<Mutex<SnowpipeClient>>,
    current_token: Arc<Mutex<String>>,
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
            _ => json!(format!("{:?}", cell)),
        })
        .collect()
}

impl SnowflakeDestination {
    pub fn new() -> EtlResult<Self> {
        let config = SnowflakeConfig::from_env()
            .map_err(|e| etl_error!(ErrorKind::Unknown, "Config error: {}", e))?;

        // Init client (akan hitung fingerprint di sini)
        let client = SnowpipeClient::new(config)
            .map_err(|e| etl_error!(ErrorKind::Unknown, "Client init error: {}", e))?;

        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            current_token: Arc::new(Mutex::new(String::new())),
        })
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

    async fn write_table_rows(&self, _table_id: TableId, rows: Vec<TableRow>) -> EtlResult<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut client = self.client.lock().await;
        let mut token_store = self.current_token.lock().await;

        if token_store.is_empty() {
            *token_store = client
                .open_channel("default")
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
            .insert_rows("default", json_rows, Some(token_store.clone()))
            .await
            .map_err(|e| etl_error!(ErrorKind::Unknown, "Write rows failed: {}", e))?;

        *token_store = next_token;
        Ok(())
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        let mut client = self.client.lock().await;
        let mut token_store = self.current_token.lock().await;

        if token_store.is_empty() {
            *token_store = client
                .open_channel("default")
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
                .insert_rows("default", json_rows, Some(token_store.clone()))
                .await
                .map_err(|e| etl_error!(ErrorKind::Unknown, "Write events failed: {}", e))?;
            *token_store = next_token;
        }

        Ok(())
    }
}
