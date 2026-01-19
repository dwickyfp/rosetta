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
use tracing::{error, info};

#[derive(Debug, Clone)]
pub struct SnowflakeDestination {
    client: Arc<Mutex<SnowpipeClient>>,
    current_token: Arc<Mutex<HashMap<TableId, String>>>,
    pg_pool: Pool<Postgres>,
    table_cache: Arc<Mutex<HashMap<TableId, String>>>,
    column_cache: Arc<Mutex<HashMap<TableId, Vec<String>>>>,
}

// Helper: Convert ETL Cell to JSON Value
fn cell_to_json_value(cell: &Cell) -> Value {
    match cell {
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
        Cell::Numeric(v) => json!(v.to_string()),
        Cell::Uuid(v) => json!(v.to_string()), // Handle UUID properly
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
            etl::types::ArrayCell::Uuid(list) => json!(
                list.iter()
                    .map(|opt| opt.as_ref().map(|u| u.to_string()))
                    .collect::<Vec<_>>()
            ),
            _ => json!(format!("{:?}", v)),
        },
        Cell::Date(v) => json!(v.to_string()),
        Cell::Time(v) => json!(v.to_string()),
        Cell::Timestamp(v) => json!(v.to_string()), // NaiveDateTime uses to_string()
        Cell::TimestampTz(v) => json!(v.to_rfc3339()),
        _ => json!(format!("{:?}", cell)),
    }
}

// Helper: Convert TableRow to JSON object with column names
fn row_to_json_object(row: &TableRow, column_names: &[String], operation: &str) -> Value {
    let mut obj = serde_json::Map::new();

    // Add column values - convert to uppercase for Snowflake
    for (i, cell) in row.values.iter().enumerate() {
        let col_name = column_names
            .get(i)
            .map(|s| s.to_uppercase())
            .unwrap_or_else(|| format!("COL_{}", i));
        obj.insert(col_name, cell_to_json_value(cell));
    }

    // Add operation column (uppercase for Snowflake)
    obj.insert("OPERATION".to_string(), json!(operation));

    // Add sync timestamp (uppercase for Snowflake)
    obj.insert(
        "SYNC_TIMESTAMP_ROSETTA".to_string(),
        json!(chrono::Utc::now().to_rfc3339()),
    );

    Value::Object(obj)
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
            column_cache: Arc::new(Mutex::new(HashMap::new())),
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
            .unwrap_or_else(|e| {
                error!("Failed to query table name for TableId {}: {}", table_id, e);
                None
            });

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

        cache.insert(table_id, table_name.clone());
        table_name
    }

    async fn resolve_column_names(&self, table_id: TableId) -> Vec<String> {
        let mut cache = self.column_cache.lock().await;
        if let Some(columns) = cache.get(&table_id) {
            return columns.clone();
        }

        // Query Postgres for column names
        let query = r#"
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = (SELECT nspname FROM pg_namespace WHERE oid = (SELECT relnamespace FROM pg_class WHERE oid = $1))
              AND table_name = (SELECT relname FROM pg_class WHERE oid = $1)
            ORDER BY ordinal_position
        "#;

        let rows: Vec<(String,)> = sqlx::query_as(query)
            .bind(table_id.0 as i32)
            .fetch_all(&self.pg_pool)
            .await
            .unwrap_or_else(|e| {
                error!(
                    "Failed to query column names for TableId {}: {}",
                    table_id, e
                );
                vec![]
            });

        let column_names: Vec<String> = rows.into_iter().map(|(name,)| name).collect();

        cache.insert(table_id, column_names.clone());
        column_names
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
        let column_names = self.resolve_column_names(table_id).await;

        let mut client = self.client.lock().await;
        let mut tokens = self.current_token.lock().await;

        let token = tokens.entry(table_id).or_insert_with(String::new);

        if token.is_empty() {
            *token = client
                .open_channel(&table_name, "default")
                .await
                .map_err(|e| {
                    error!("Open channel failed for {}: {}", table_name, e);
                    etl_error!(ErrorKind::Unknown, "Open channel failed: {}", e)
                })?;
        }

        let json_rows: Vec<Value> = rows
            .iter()
            .map(|r| row_to_json_object(r, &column_names, "C"))
            .collect();

        let next_token = client
            .insert_rows(&table_name, "default", json_rows, Some(token.clone()))
            .await
            .map_err(|e| {
                error!("Insert rows failed for {}: {}", table_name, e);
                etl_error!(ErrorKind::Unknown, "Write rows failed: {}", e)
            })?;

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
            let column_names = self.resolve_column_names(table_id).await;

            let mut client = self.client.lock().await;
            let mut tokens = self.current_token.lock().await;

            let token = tokens.entry(table_id).or_insert_with(String::new);

            if token.is_empty() {
                *token = client
                    .open_channel(&table_name, "default")
                    .await
                    .map_err(|e| {
                        error!("Open channel failed for {}: {}", table_name, e);
                        etl_error!(ErrorKind::Unknown, "Open channel failed: {}", e)
                    })?;
            }

            let mut json_rows = Vec::new();

            for event in events {
                let row_obj = match event {
                    Event::Insert(i) => Some(row_to_json_object(&i.table_row, &column_names, "C")),
                    Event::Update(u) => Some(row_to_json_object(&u.table_row, &column_names, "U")),
                    Event::Delete(d) => {
                        // For deletes, use old_table_row if available, otherwise skip
                        if let Some((_, old_row)) = &d.old_table_row {
                            Some(row_to_json_object(old_row, &column_names, "D"))
                        } else {
                            None
                        }
                    }
                    _ => None,
                };

                if let Some(obj) = row_obj {
                    json_rows.push(obj);
                }
            }

            if !json_rows.is_empty() {
                let next_token = client
                    .insert_rows(&table_name, "default", json_rows, Some(token.clone()))
                    .await
                    .map_err(|e| {
                        error!("Insert events failed for {}: {}", table_name, e);
                        etl_error!(ErrorKind::Unknown, "Write events failed: {}", e)
                    })?;
                *token = next_token;
            }
        }

        Ok(())
    }
}
