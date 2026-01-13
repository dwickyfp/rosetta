use anyhow::{Context, Result};
use std::env;

#[derive(Debug, Clone)]
pub struct SnowflakeConfig {
    pub account_id: String,
    pub user: String,
    pub database: String,
    pub schema: String,
    pub table: String,
    pub role: String,

    // Auth Config
    pub private_key_path: String,               // Path ke file .p8
    pub private_key_passphrase: Option<String>, // Password key (jika ada)
}

impl SnowflakeConfig {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            account_id: env::var("SNOWFLAKE_ACCOUNT").context("SNOWFLAKE_ACCOUNT missing")?,
            user: env::var("SNOWFLAKE_USER").context("SNOWFLAKE_USER missing")?,
            database: env::var("SNOWFLAKE_DB").context("SNOWFLAKE_DB missing")?,
            schema: env::var("SNOWFLAKE_SCHEMA").context("SNOWFLAKE_SCHEMA missing")?,
            table: env::var("SNOWFLAKE_TABLE").context("SNOWFLAKE_TABLE missing")?,
            role: env::var("SNOWFLAKE_ROLE").unwrap_or_else(|_| "PUBLIC".to_string()),

            private_key_path: env::var("SNOWFLAKE_PRIVATE_KEY_PATH")
                .context("SNOWFLAKE_PRIVATE_KEY_PATH missing")?,
            private_key_passphrase: env::var("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE").ok(),
        })
    }
}
