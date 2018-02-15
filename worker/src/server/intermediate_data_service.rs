use grpc::{Error, RequestOptions, SingleResponse};

use operations::io;

use cerberus_proto::worker as pb;
use cerberus_proto::worker_grpc as grpc_pb;

const DATA_NOT_AVAILABLE: &str = "Data not available";

pub struct IntermediateDataService;

impl grpc_pb::IntermediateDataService for IntermediateDataService {
    fn get_intermediate_data(
        &self,
        _o: RequestOptions,
        req: pb::IntermediateDataRequest,
    ) -> SingleResponse<pb::IntermediateData> {
        // TODO: After the unnecessary stuff is removed from the request path, add the absolute
        //       path for this worker.
        info!("Serving file {}", &req.get_path());
        if let Ok(data) = io::read_local(req.get_path()) {
            let mut res = pb::IntermediateData::new();
            res.set_data(data.into_bytes());

            return SingleResponse::completed(res);
        }

        SingleResponse::err(Error::Other(DATA_NOT_AVAILABLE))
    }
}
