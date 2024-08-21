use crate::transport::api::InsertRequest;
use crate::transport::shard::Shard;
use crate::transport::shard_op::{build_url, ShardOp};

pub struct Insert<'a> {
    request: &'a InsertRequest,
}

impl<'a> Insert<'a> {
    pub fn new(request: &'a InsertRequest) -> Self {
        Self { request }
    }
}

impl<'a> ShardOp<InsertRequest, String> for Insert<'a> {
    fn input(&self) -> &InsertRequest {
        &self.request
    }

    fn url(&self, shard: &Shard) -> String {
        build_url(&shard.ip_port, "insert")
    }
}
