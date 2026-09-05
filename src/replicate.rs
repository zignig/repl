// Make a replicator using the iroh-smol-kv
//

use std::collections::{BTreeMap, HashSet};
use std::{str::FromStr, time::Duration};

use bytes::Bytes;
use iroh::{Endpoint, PublicKey, SecretKey};
use iroh_blobs::{BlobsProtocol, Hash, HashAndFormat};
use iroh_gossip::{net::Gossip, proto::TopicId};

use iroh_smol_kv::util::format_bytes;
use iroh_smol_kv::{Client, Config, SubscribeItem, SubscribeResponse};
use n0_future::{StreamExt, task::AbortOnDropHandle};
use n0_snafu::{Result, ResultExt};
use tokio::task;
use tracing::{error, info, warn};

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
        task::spawn(test_runner(client, secret, blobs, endpoint, prefixes));
        Ok(())
    }
}

// Add to the kv once an hour, do it first...
pub async fn test_runner(
    client: Client,
    secret: SecretKey,
    blobs: BlobsProtocol,
    endpoint: Endpoint,
    prefix: Vec<String>,
) -> Result<()> {
    let mut subscribers = BTreeMap::new();
    let ws = client.write(secret);
    let mut op_id = 0;
    let mut next_op_id = || {
        let id = op_id;
        op_id += 1;
        id
    };
    let sub = client.subscribe();
    let id = next_op_id();
    let task = tokio::spawn(handle_subscription(
        id,
        sub,
        blobs.clone(),
        endpoint.clone(),
        prefix.clone(),
    ));
    subscribers.insert(id, AbortOnDropHandle::new(task));
    println!("update count {:?}", id);
    let mut ticker = tokio::time::interval(Duration::from_secs(600));

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let mut wrap_up: BTreeMap<Bytes,Vec<PublicKey>> = BTreeMap::new();
                let items = client
                    .iter()
                    .collect::<Vec<_>>()
                    .await
                    .expect("collect borked");
                for (ep,_,val) in items {
                    wrap_up.entry(val.clone()).or_default().push(ep.clone())
                };
                println!("{:#?}",&wrap_up);
                for pre in prefix.clone().into_iter() {
                    // info!("scan prefix {}",&pre);
                    let mut counter  = 0;
                    let mut tag_scan = blobs.store().tags().list_prefix(&pre).await.unwrap();
                    // let mut tag_scan = blobs.store().tags().list().await.unwrap();
                    while let Some(event) = tag_scan.next().await {
                        let tag = event.unwrap();
                        let tag_name = str::from_utf8(&tag.name.0).unwrap().to_owned();
                        counter +=1 ;
                        // info!("{:#?}",tag_name);
                        let _ = ws.put(tag_name, tag.hash.to_hex()).await;
                    }
                    info!("prefix {} - {} items",&pre,counter);
                }
            }


        };
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
    let knf = HashAndFormat::hash_seq(hash);
    // if let Ok(status)  = blobs.blobs().status(hash).await{
    //     info!("status {:?} {:?}",status,hash);
    // }
    match blobs.store().remote().local(knf).await {
        Ok(info) => {
            if !info.is_complete() {
                info!("fetch blob {:?} {:#?}", &name, &s);
                if target != endpoint.id() {
                    let conn = endpoint
                        .connect(target, iroh_blobs::ALPN)
                        .await
                        .expect("connect fail");
                    let r = blobs
                        .store()
                        .remote()
                        .fetch(conn, knf)
                        .await
                        .expect("blob fail");
                    warn!("{:?}", r);
                }
                // let addrs = Shuffled::new(vec![target]);
                // let _ = blobs.downloader(endpoint).download(req, addrs).await;
                blobs.tags().set(name, hash).await.expect("bad tag");
                info!("finish blob {:#?}", &s);
            }
        }
        Err(e) => error!("blob fail , {:#?}", e),
    }
    Ok(())
}

async fn handle_subscription(
    id: usize,
    sub: SubscribeResponse,
    blobs: BlobsProtocol,
    endpoint: Endpoint,
    _prefix: Vec<String>,
) {
    let stream = sub.stream_raw();
    tokio::pin!(stream);
    while let Some(item) = stream.next().await {
        match item {
            Ok(SubscribeItem::Entry((scope, key, value))) => {
                println!(
                    "#{}: ({},{},{})",
                    id,
                    scope.fmt_short(),
                    format_bytes(&key),
                    format_bytes(&value.value)
                );

                let _ = get_item(&blobs, &endpoint, scope, key, value.value).await;
            }
            Ok(SubscribeItem::Expired((scope, key, timestamp))) => {
                println!(
                    "#{}: expired ({},{},{})",
                    id,
                    scope.fmt_short(),
                    format_bytes(&key),
                    timestamp,
                );
            }
            Ok(SubscribeItem::CurrentDone) => {
                info!("sub up to date");
            }
            Err(e) => {
                println!("#{id}: Error in subscription: {e:?}");
                break;
            }
        }
    }
    println!("#{id} ended");
}
