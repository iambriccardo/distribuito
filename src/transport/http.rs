use crate::transport::shard::Shard;
use crate::transport::shard_op::ShardOp;
use serde::{Deserialize, Serialize};
use std::io;
use std::io::{Error, ErrorKind};

pub async fn post<I: Serialize, O: for<'a> Deserialize<'a>>(
    shard: &Shard,
    shard_op: &impl ShardOp<I, O>,
) -> io::Result<O> {
    let url = shard_op.url(shard);
    let response = shard
        .client
        .post(url)
        .json(shard_op.input())
        .send()
        .await
        .map_err(|e| {
            Error::new(
                ErrorKind::Other,
                format!("Error while sending the request: {}", e),
            )
        })?;

    response.json().await.map_err(|e| {
        Error::new(
            ErrorKind::Other,
            format!("Error while deserializing the request: {}", e),
        )
    })
}
