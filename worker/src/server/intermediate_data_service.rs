use grpc::{Error, RequestOptions, SingleResponse};

use operations::io;

use cerberus_proto::worker as pb;
use cerberus_proto::worker_grpc as grpc_pb;
use util;

const DATA_NOT_AVAILABLE: &str = "Data not available";

pub struct IntermediateDataService;

impl grpc_pb::IntermediateDataService for IntermediateDataService {
    fn get_intermediate_data(
        &self,
        _o: RequestOptions,
        req: pb::IntermediateDataRequest,
    ) -> SingleResponse<pb::IntermediateData> {
        info!("Serving file {}", &req.get_path());
        match io::read_local(req.get_path()) {
            Ok(data) => {
                let mut res = pb::IntermediateData::new();
                res.set_data(data.into_bytes());

                SingleResponse::completed(res)
            }
            Err(err) => {
                util::output_error(&err.chain_err(|| "Unable to read intermediate data: {:?}"));
                SingleResponse::err(Error::Other(DATA_NOT_AVAILABLE))
            }
        }
    }
}
