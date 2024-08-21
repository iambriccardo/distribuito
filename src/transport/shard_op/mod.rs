pub mod create_table;

use crate::transport::shard::Shard;
use serde::{Deserialize, Serialize};

pub fn build_url(ip_port: &str, path: &str) -> String {
    format!("http://{}/{}", ip_port, path)
}

pub trait ShardOp<I, O>
where
    I: Serialize,
    for<'a> O: Deserialize<'a>,
{
    fn input(&self) -> &I;

    fn url(&self, shard: &Shard) -> String;
}
