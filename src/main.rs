use anyhow::Result;
use dotenv::dotenv;
use etl::config::{
    BatchConfig, PgConnectionConfig, PipelineConfig, TableSyncCopyConfig, TlsConfig,
};
use etl::pipeline::Pipeline;
use figlet_rs::FIGfont;
use rosetta::snowflake::SnowflakeDestination;
use rosetta::store::memory::CustomStore;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    tracing_subscriber::fmt::init();

    if let Ok(font) = FIGfont::from_file("assets/fonts/Slant.flf") {
        if let Some(figure) = font.convert("Rosetta") {
            println!("{}", figure);
        }
    }

    let destination = SnowflakeDestination::new()?;
    let store = CustomStore::new();

    let config = PipelineConfig {
        id: 1,
        publication_name: std::env::var("PG_PUB_NAME").unwrap_or("supabase_pub".into()),
        pg_connection: PgConnectionConfig {
            host: std::env::var("PG_HOST")?,
            port: std::env::var("PG_PORT")?.parse()?,
            name: std::env::var("PG_DB")?,
            username: std::env::var("PG_USER")?,
            password: Some(std::env::var("PG_PASS")?.into()),
            tls: TlsConfig {
                enabled: false,
                trusted_root_certs: String::new(),
            },
            keepalive: None,
        },
        batch: BatchConfig {
            max_size: 1000,
            max_fill_ms: 5000,
        },
        table_error_retry_delay_ms: 10000,
        table_error_retry_max_attempts: 5,
        max_table_sync_workers: 4,
        table_sync_copy: TableSyncCopyConfig::SkipAllTables,
    };

    let mut pipeline = Pipeline::new(config, store, destination);
    pipeline.start().await?;
    pipeline.wait().await?;

    Ok(())
}
