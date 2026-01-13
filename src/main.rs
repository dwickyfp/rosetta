use anyhow::Result;
use dotenv::dotenv;
use figlet_rs::FIGfont;
use rosetta::manager::PipelineManager;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    tracing_subscriber::fmt::init();

    if let Ok(font) = FIGfont::from_file("assets/fonts/Slant.flf") {
        if let Some(figure) = font.convert("Rosetta") {
            println!("{}", figure);
        }
    }

    // Config DB URL should be in environment
    let database_url = std::env::var("CONFIG_DATABASE_URL")
        .expect("CONFIG_DATABASE_URL environment variable must be set");

    info!("Starting Rosetta Pipeline Manager...");
    info!("Connecting to config database: {}", database_url);

    let manager = PipelineManager::new(&database_url).await?;
    manager.run().await?;

    Ok(())
}
