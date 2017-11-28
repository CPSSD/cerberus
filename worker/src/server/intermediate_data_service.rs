use grpc::{RequestOptions, SingleResponse};

use cerberus_proto::worker as pb;
use cerberus_proto::worker_grpc as grpc_pb;

pub struct IntermediateDataService;

impl IntermediateDataService {
    pub fn new() -> Self {
        IntermediateDataService {}
    }
}

impl grpc_pb::IntermediateDataService for IntermediateDataService {
    fn get_intermediate_data(
        &self,
        _o: RequestOptions,
        _req: pb::GetIntermediateDataRequest,
    ) -> SingleResponse<pb::Partition> {
        // TODO(voy): Implement
        return SingleResponse::completed(pb::Partition::new());
    }
}
