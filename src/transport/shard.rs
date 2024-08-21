use crate::config::Config;
use crate::transport::http::post;
use crate::transport::shard_op::ShardOp;
use futures::future::join_all;
use log::info;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use tokio::io;

#[derive(Debug)]
pub struct Shard {
    pub ip_port: String,
    pub client: Client,
}

impl Shard {
    fn new(ip_port: String) -> Self {
        Self {
            ip_port,
            client: Client::new(),
        }
    }

    async fn call<I: Serialize, O: for<'a> Deserialize<'a>>(
        &self,
        shard_op: &impl ShardOp<I, O>,
    ) -> io::Result<O> {
        post(self, shard_op).await
    }
}

#[derive(Debug)]
pub struct Shards {
    shards: Vec<Shard>,
    next_index: Mutex<u64>,
}

impl Shards {
    pub fn new(config: &Config) -> Self {
        let mut shards = Vec::new();
        for instance in config.instances.iter() {
            shards.push(Shard::new(instance.ip_port.clone()));
        }

        Self {
            shards,
            next_index: Mutex::new(0),
        }
    }

    pub fn number_of_shards(&self) -> usize {
        self.shards.len()
    }

    pub async fn broadcast<I: Serialize, O: for<'a> Deserialize<'a>>(
        &self,
        shard_op: impl ShardOp<I, O>,
    ) -> io::Result<Vec<O>> {
        // Create a collection of futures representing each shard operation.
        let futures: Vec<_> = self
            .shards
            .iter()
            .map(|shard| {
                info!("Broadcasting shard op to '{}'", shard_op.url(shard));
                shard.call(&shard_op)
            }) // Generate the future for each shard call.
            .collect();

        // Wait for all futures to complete.
        let results = join_all(futures).await;

        // Collect the results, returning an error if any call failed.
        results.into_iter().collect::<Result<Vec<_>, _>>()
    }

    pub async fn rr_unicast<I: Serialize, O: for<'a> Deserialize<'a>>(
        &self,
        shard_op: impl ShardOp<I, O>,
    ) -> io::Result<O> {
        let shard = self.next_shard();
        info!("Sending shard op to '{}'", shard_op.url(shard));

        let result = shard.call(&shard_op).await?;

        Ok(result)
    }

    fn next_shard(&self) -> &Shard {
        let mut next_index = self.next_index.lock().unwrap();
        let shard = &self.shards[*next_index as usize];

        *next_index = (*next_index + 1u64) % self.shards.len() as u64;

        shard
    }
}
