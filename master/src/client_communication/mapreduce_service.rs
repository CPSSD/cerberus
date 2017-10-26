use std::sync::{Arc, Mutex};
use grpc::{SingleResponse, Error, RequestOptions};

use scheduler::MapReduceScheduler;
use mapreduce_job::MapReduceJob;

use cerberus_proto::mapreduce as pb;
use cerberus_proto::mapreduce_grpc as grpc_pb;

const SCHEDULER_BUSY: &'static str = "Scheduler busy";
const JOB_SCHEDULE_ERROR: &'static str = "Unable to schedule mapreduce job";
const JOB_RETRIEVAL_ERROR: &'static str = "Unable to retrieve mapreduce jobs";
const MISSING_JOB_IDS: &'static str = "No client_id or mapreduce_id provided";

pub struct MapReduceServiceImpl {
    scheduler: Arc<Mutex<MapReduceScheduler>>,
}

impl MapReduceServiceImpl {
    pub fn new(scheduler: Arc<Mutex<MapReduceScheduler>>) -> Self {
        MapReduceServiceImpl { scheduler: scheduler }
    }
}

impl grpc_pb::MapReduceService for MapReduceServiceImpl {
    fn perform_map_reduce(
        &self,
        _: RequestOptions,
        req: pb::MapReduceRequest,
    ) -> SingleResponse<pb::MapReduceResponse> {
        match self.scheduler.lock() {
            Err(_) => SingleResponse::err(Error::Other(SCHEDULER_BUSY)),
            Ok(mut scheduler) => {
                let mut response = pb::MapReduceResponse::new();
                let job = match MapReduceJob::new(
                    req.client_id,
                    req.binary_path,
                    req.input_directory,
                    req.output_directory,
                ) {
                    Ok(job) => job,
                    Err(_) => return SingleResponse::err(Error::Other(JOB_SCHEDULE_ERROR)),
                };
                response.mapreduce_id = job.get_map_reduce_id().to_owned();
                let result = scheduler.schedule_map_reduce(job);
                match result {
                    Err(_) => SingleResponse::err(Error::Other(JOB_SCHEDULE_ERROR)),
                    Ok(_) => SingleResponse::completed(response),
                }
            }
        }
    }

    fn map_reduce_status(
        &self,
        _: RequestOptions,
        req: pb::MapReduceStatusRequest,
    ) -> SingleResponse<pb::MapReduceStatusResponse> {
        match self.scheduler.lock() {
            Err(_) => SingleResponse::err(Error::Other(SCHEDULER_BUSY)),
            Ok(scheduler) => {
                let mut response = pb::MapReduceStatusResponse::new();
                let jobs: Vec<&MapReduceJob>;

                if !req.client_id.is_empty() {
                    let client_result =
                        scheduler.get_mapreduce_client_status(req.client_id.clone());
                    jobs = match client_result {
                        Err(_) => return SingleResponse::err(Error::Other(JOB_RETRIEVAL_ERROR)),
                        Ok(js) => js,
                    };
                } else if !req.mapreduce_id.is_empty() {
                    let result = scheduler.get_mapreduce_status(req.mapreduce_id);
                    jobs = match result {
                        Err(_) => return SingleResponse::err(Error::Other(JOB_RETRIEVAL_ERROR)),
                        Ok(job) => vec![job],
                    };
                } else {
                    return SingleResponse::err(Error::Other(MISSING_JOB_IDS));
                }

                for job in jobs {
                    let mut report = pb::MapReduceReport::new();
                    report.mapreduce_id = job.get_map_reduce_id().to_owned();
                    report.status = job.get_status().to_owned();

                    // TODO: Add timestamps for scheduled, started and done time
                    // to each report.

                    response.reports.push(report);
                }

                SingleResponse::completed(response)
            }
        }
    }

    fn cluster_status(
        &self,
        _: RequestOptions,
        _: pb::EmptyMessage,
    ) -> SingleResponse<pb::ClusterStatusResponse> {
        match self.scheduler.lock() {
            Err(_) => SingleResponse::err(Error::Other(SCHEDULER_BUSY)),
            Ok(scheduler) => {
                let mut response = pb::ClusterStatusResponse::new();
                response.workers = i64::from(scheduler.get_available_workers());
                response.queue_size = scheduler.get_map_reduce_job_queue_size() as i64;

                SingleResponse::completed(response)
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use errors::*;
    use mapreduce_job::MapReduceJob;
    use mapreduce_tasks::{MapReduceTask, TaskProcessorTrait};
    use cerberus_proto::mapreduce::Status as MapReduceStatus;
    use cerberus_proto::mapreduce_grpc::MapReduceService;

    struct NullTaskProcessor;

    impl TaskProcessorTrait for NullTaskProcessor {
        fn create_map_tasks(&self, _: &MapReduceJob) -> Result<Vec<MapReduceTask>> {
            Ok(Vec::new())
        }

        fn create_reduce_tasks(
            &self,
            _: &MapReduceJob,
            _: &[&MapReduceTask],
        ) -> Result<Vec<MapReduceTask>> {
            Ok(Vec::new())
        }
    }

    fn create_map_reduce_scheduler() -> MapReduceScheduler {
        MapReduceScheduler::new(Box::new(NullTaskProcessor {}))
    }

    #[test]
    fn queue_mapreduce() {
        let scheduler = create_map_reduce_scheduler();
        assert!(!scheduler.get_map_reduce_in_progress());

        let master_impl = MapReduceServiceImpl { scheduler: Arc::new(Mutex::new(scheduler)) };

        let _ = master_impl
            .perform_map_reduce(RequestOptions::new(), pb::MapReduceRequest::new())
            .wait();

        let map_reduce_in_progress = match master_impl.scheduler.lock() {
            Ok(scheduler) => scheduler.get_map_reduce_in_progress(),
            Err(_) => false,
        };

        assert!(map_reduce_in_progress);
    }

    #[test]
    fn get_mapreduce_status() {
        let mut scheduler = create_map_reduce_scheduler();

        let job = MapReduceJob::new("client-2", "/", "/", "").unwrap();
        let mut request = pb::MapReduceStatusRequest::new();
        request.mapreduce_id = job.get_map_reduce_id().to_owned();
        let result = scheduler.schedule_map_reduce(job);
        assert!(result.is_ok());

        let master_impl = MapReduceServiceImpl { scheduler: Arc::new(Mutex::new(scheduler)) };
        let response = master_impl.map_reduce_status(RequestOptions::new(), request);

        let (_, mut item, _) = response.wait().unwrap();
        let status = item.reports.pop().unwrap().status;

        assert_eq!(MapReduceStatus::IN_PROGRESS, status)
    }

    #[test]
    fn cluster_status() {
        let mut scheduler = create_map_reduce_scheduler();
        scheduler.set_available_workers(5);
        let _ = scheduler.schedule_map_reduce(MapReduceJob::new("client-1", "/", "/", "").unwrap());
        let _ = scheduler.schedule_map_reduce(MapReduceJob::new("client-1", "/", "/", "").unwrap());

        let master_impl = MapReduceServiceImpl { scheduler: Arc::new(Mutex::new(scheduler)) };
        let response = master_impl.cluster_status(RequestOptions::new(), pb::EmptyMessage::new());
        let (_, item, _) = response.wait().unwrap();

        assert_eq!(5, item.workers);
        assert_eq!(2, item.queue_size);
    }
}
