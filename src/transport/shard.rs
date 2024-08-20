use std::error::Error;
use reqwest::Client;
use serde::Deserialize;
use crate::config::Config;

pub struct Shard {
    ip_port: String,
    client: Client,
}

impl Shard {
    
    pub fn new(ip_port: String) -> Self {
        Self {
            ip_port,
            client: Client::new()
        }
    }
    
}

pub struct Shards {
    shards: Vec<Shard>,
}

impl Shards {
    async fn new(config: Config) -> Self {
        let mut shards = Vec::new();
        let client = Client::new();

        for instance in config.instances {
            shards.push(Shard {
                ip_port: instance.ip_port,
                client: client.clone(),
            });
        }

        Self { shards }
    }

    async fn send_http_request(&self, shard_index: usize, path: &str) -> Result<reqwest::Response, Box<dyn Error>> {
        if let Some(shard) = self.shards.get(shard_index) {
            let url = format!("http://{}/{}", shard.ip_port, path);
            let response = shard.client.get(&url).send().await?;
            Ok(response)
        } else {
            Err(Box::new(std::io::Error::new(std::io::ErrorKind::NotFound, "Shard not found")))
        }
    }

    async fn send_http_request_and_parse_json<T: for<'de> Deserialize<'de>>(
        &self,
        shard_index: usize,
        path: &str,
    ) -> Result<T, Box<dyn Error>> {
        let res = self.send_http_request(shard_index, path).await?;
        let parsed = res.json::<T>().await?;
        Ok(parsed)
    }
}