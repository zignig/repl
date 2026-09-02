// Make a replicator using the iroh-smol-kv
//

use std::{str::FromStr, time::Duration};

use bytes::Bytes;
use iroh::{Endpoint, PublicKey, SecretKey};
use iroh_blobs::{BlobsProtocol, Hash, HashAndFormat, api::downloader::Shuffled};
use iroh_gossip::{net::Gossip, proto::TopicId};

use iroh_smol_kv::{Client, Config};
use n0_future::StreamExt;
use n0_snafu::{Result, ResultExt};
use tokio::task;
use tracing::{info, warn};

pub struct Replicator {
    blobs: BlobsProtocol,
    endpoint: Endpoint,
    client: Client,
    secret: SecretKey,
    prefixes: Vec<String>,
}

impl Replicator {
    pub async fn new(
        gossip: Gossip,
        blobs: BlobsProtocol,
        endpoint: Endpoint,
        topic_id: TopicId,
        bootstrap: Vec<PublicKey>,
        secret: SecretKey,
        prefixes: Vec<String>,
    ) -> Result<Self> {
        let topic = gossip.subscribe(topic_id, bootstrap).await.e()?;
        let client = Client::local(topic, Config::default());
        Ok(Self {
            blobs,
            endpoint,
            client,
            secret,
            prefixes,
        })
    }

    // Testing for the kv share.
    pub async fn run(&self) -> Result<()> {
        let client = self.client.clone();
        let secret = self.secret.clone();
        let blobs = self.blobs.clone();
        let endpoint = self.endpoint.clone();
        let prefixes = self.prefixes.clone();
        task::spawn(test_runner(client, secret, blobs,endpoint, prefixes));
        Ok(())
    }
}

// Add to the kv once an hour, do it first...
pub async fn test_runner(
    client: Client,
    secret: SecretKey,
    blobs: BlobsProtocol,
    endpoint: Endpoint,
    prefixes: Vec<String>,
) -> Result<()> {
    let ws = client.write(secret);
    let mut op_id = 0;
    let mut next_op_id = || {
        let id = op_id;
        op_id += 1;
        id
    };

    let id = next_op_id();
    println!("update count {:?}", id);
    let mut ticker = tokio::time::interval(Duration::from_secs(3600));
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                for pre in prefixes.clone().into_iter() {
                    println!("scan prefix {pre}");
                    let mut tag_scan = blobs.store().tags().list_prefix(pre).await.unwrap();
                    // let mut tag_scan = blobs.store().tags().list().await.unwrap();
                    while let Some(event) = tag_scan.next().await {
                        let tag = event.unwrap();
                        let tag_name = str::from_utf8(&tag.name.0).unwrap().to_owned();
                        let _ = ws.put(tag_name, tag.hash.to_hex()).await;
                    }
                }
                // fetch some blobs from friends
                let items = client.iter().collect::<Vec<_>>().await.expect("collect borked");
                    for (target,name, content_hash) in items {
                        info!("{} , {:#?} , {:#?}",target.fmt_short(),name,content_hash);
                        let res = get_item(&blobs,&endpoint,target,name,content_hash).await;
                        info!("{:#?}",res);
                };
            }


        };
        // tokio::time::sleep(Duration::from_secs(3600)).await;
    }
}

async fn get_item(
    blobs: &BlobsProtocol,
    endpoint: &Endpoint,
    target: PublicKey,
    name: Bytes,
    hash: Bytes,
) -> Result<()> {
    // info!("get item len {:#?}", hash.len());
    let s = str::from_utf8(&hash).expect("bad hash");
    let hash = Hash::from_str(s).expect("bad conversion");
    let r = blobs.blobs().has(hash).await.expect("blob fail list");
    // info!("have blob {} -> {:#?}", &hash, &r);
    if !r {
        warn!("fetch some blobage");
        let req = HashAndFormat::hash_seq(hash);
        let addrs = Shuffled::new(vec![target]);
        blobs
            .downloader(endpoint)
            .download(req, addrs)
            .await.expect("blob fail");
        blobs.tags().set(name, hash).await.expect("bad tag");
    }
    Ok(())
}
