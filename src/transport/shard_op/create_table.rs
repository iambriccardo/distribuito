use crate::transport::api::CreateTableRequest;
use crate::transport::shard::Shard;
use crate::transport::shard_op::{build_url, ShardOp};

pub struct CreateTable<'a> {
    request: &'a CreateTableRequest,
}

impl<'a> CreateTable<'a> {
    pub fn new(request: &'a CreateTableRequest) -> Self {
        Self { request }
    }
}

impl<'a> ShardOp<CreateTableRequest, String> for CreateTable<'a> {
    fn input(&self) -> &CreateTableRequest {
        self.request
    }

    fn url(&self, shard: &Shard) -> String {
        build_url(&shard.ip_port, "create_table")
    }
}
