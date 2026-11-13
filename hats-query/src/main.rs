use axum::Router;
use clap::Parser;
use hats_query_core::configuration::Configuration;
use std::error::Error;
use std::path::PathBuf;
use tokio::net::TcpListener;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    /// TOML configuration file
    #[arg(short, long, env = "HATS_QUERY_CONFIG")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    let config = Configuration::load(&cli.config)?;
    let listener = TcpListener::bind((config.server.address, config.server.port)).await?;
    axum::serve(listener, Router::new()).await?;
    Ok(())
}
