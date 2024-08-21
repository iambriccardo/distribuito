use crate::config::Config;
use crate::transport::http::post;
use crate::transport::shard_op::ShardOp;
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

    pub async fn broadcast<I: Serialize, O: for<'a> Deserialize<'a>>(
        &self,
        shard_op: impl ShardOp<I, O>,
    ) -> io::Result<Vec<O>> {
        let mut results = Vec::with_capacity(self.shards.len());

        for shard in self.shards.iter() {
            let result = shard.call(&shard_op).await?;
            results.push(result)
        }

        Ok(results)
    }

    pub async fn rr_unicast<I: Serialize, O: for<'a> Deserialize<'a>>(
        &self,
        shard_op: impl ShardOp<I, O>,
    ) -> io::Result<O> {
        let shard = self.next_shard();
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
