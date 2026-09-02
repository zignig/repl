use std::path::PathBuf;

use anyhow::Result;
use iroh::{Endpoint, PublicKey, SecretKey, endpoint::presets, protocol::Router};
use iroh_blobs::store::fs::FsStore;
use iroh_gossip::{Gossip, TopicId};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};
use tracing_subscriber::{
    filter::{LevelFilter, Targets},
    prelude::*,
};

use crate::replicate::Replicator;

mod replicate;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut filter = Targets::new();
    filter = filter
        .with_target(env!("CARGO_PKG_NAME"), LevelFilter::DEBUG)
        .with_target("iroh-blobs", LevelFilter::DEBUG);
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

    info!("public key {}", config.get_public());
    // BLOBS!
    let path = PathBuf::from("data/blobs");
    let store = FsStore::load(path).await.unwrap();
    let blobs = iroh_blobs::BlobsProtocol::new(&store, None);

    // GOSSIP!
    let gossip = Gossip::builder().spawn(endpoint.clone());

    let router = Router::builder(endpoint.clone())
        .accept(iroh_blobs::ALPN, blobs.clone())
        .accept(iroh_gossip::ALPN, gossip.clone())
        .spawn();

    // Create the replica system
    let topic = blake3::hash(b"copycopycopy");
    let topic_id = TopicId::from_bytes(*topic.as_bytes());
    let repl_res = Replicator::new(
        gossip.clone(),
        blobs.clone(),
        endpoint.clone(),
        topic_id,
        config.get_peers(),
        config.get_secret(),
        vec!["col".to_string(), "notes".to_string()],
    )
    .await;
    match repl_res {
        Ok(repl) => repl.run().await.expect("borked"),
        Err(e) => error!("repl fail {}", e),
    }

    tokio::signal::ctrl_c().await?;

    router.shutdown().await?;
    info!("exiting");
    Ok(())
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    secret: SecretKey,
    peers: Vec<PublicKey>,
}

impl Config {
    pub fn get_secret(&self) -> SecretKey {
        self.secret.clone()
    }

    pub fn get_public(&self) -> PublicKey {
        self.secret.public()
    }

    pub fn get_peers(&self) -> Vec<PublicKey> {
        self.peers.clone()
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
                let slf = Self {
                    secret,
                    peers: vec![],
                };
                slf.save(&path)?;
                slf
                // return Err(anyhow!("Bad Config File Parse {:#?}", e));
            }
        };
        Ok(config)
    }
}
