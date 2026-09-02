use std::path::PathBuf;

use anyhow::{Result, anyhow};
use iroh::{Endpoint, PublicKey, SecretKey, endpoint::presets, protocol::Router};
use iroh_blobs::{BlobsProtocol, store::mem::MemStore};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};
use tracing_subscriber::{
    filter::{LevelFilter, Targets},
    prelude::*,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut filter = Targets::new();
    filter = filter
        .with_target(env!("CARGO_PKG_NAME"), LevelFilter::DEBUG)
        .with_target("iroh", LevelFilter::INFO);
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(filter)
        .init();
    let pb = PathBuf::from("config.toml");
    let config = Config::load(pb)?;

    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(config.get_secret())
        .bind()
        .await?;

    info!("public key {}",config.get_public());
    // We initialize an in-memory backing store for iroh-blobs
    let store = MemStore::new();
    // Then we initialize a struct that can accept blobs requests over iroh connections
    let blobs = BlobsProtocol::new(&store, None);
    let router = Router::builder(endpoint)
        .accept(iroh_blobs::ALPN, blobs)
        .spawn();

    tokio::signal::ctrl_c().await?;

    router.shutdown().await?;
    info!("exiting");
    Ok(())
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    secret: SecretKey,
}

impl Config {
    pub fn get_secret(&self) -> SecretKey {
        self.secret.clone()
    }

    pub fn get_public(&self) -> PublicKey {
        self.secret.public()
    }

    pub fn save(&self, path: &PathBuf) -> Result<()> {
        let contents = toml::to_string(&self).expect("Broken Config");
        std::fs::write(path, contents).expect("Can't write config file");
        Ok(())
    }

    pub fn load(path: PathBuf) -> Result<Self> {
        let config = match std::fs::read_to_string(&path) {
            Ok(content) => {
                let content = content.as_str();
                let config: Config = toml::from_str(&content).expect("Bad config file");
                config
            }
            Err(e) => {
                error!("{:?}", &e);
                warn!("Config file does not exist");
                let secret = SecretKey::generate();
                let slf = Self { secret };
                slf.save(&path)?;
                slf
                // return Err(anyhow!("Bad Config File Parse {:#?}", e));
            }
        };
        Ok(config)
    }
}
