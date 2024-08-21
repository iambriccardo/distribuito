use crate::transport::api::{QueryRequest, QueryResponse};
use crate::transport::shard::Shard;
use crate::transport::shard_op::{build_url, ShardOp};

pub struct Query<'a> {
    request: &'a QueryRequest
}

impl <'a> Query<'a> {

    pub fn new(request: &'a QueryRequest) -> Self {
        Self {
            request
        }
    }
}

impl <'a> ShardOp<QueryRequest, QueryResponse> for Query<'a> {
    fn input(&self) -> &QueryRequest {
        &self.request
    }

    fn url(&self, shard: &Shard) -> String {
        build_url(&shard.ip_port, "query")
    }
}