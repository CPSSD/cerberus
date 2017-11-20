use std::sync::{Arc, Mutex};

use grpc::{SingleResponse, Error, RequestOptions};

use common::{Job, JobOptions};
use scheduler::Scheduler;
use util::output_error;

use cerberus_proto::mapreduce as pb;
use cerberus_proto::mapreduce_grpc as grpc_pb;

const JOB_SCHEDULE_ERROR: &str = "Unable to schedule mapreduce job";
const JOB_RETRIEVAL_ERROR: &str = "Unable to retrieve mapreduce jobs";
const MISSING_JOB_IDS: &str = "No client_id or mapreduce_id provided";

/// `ClientService` recieves communication from a client.
pub struct ClientService {
    scheduler: Arc<Mutex<Scheduler>>,
}

impl ClientService {
    pub fn new(scheduler: Arc<Mutex<Scheduler>>) -> Self {
        ClientService { scheduler: scheduler }
    }
}

impl grpc_pb::MapReduceService for ClientService {
    fn perform_map_reduce(
        &self,
        _: RequestOptions,
        req: pb::MapReduceRequest,
    ) -> SingleResponse<pb::MapReduceResponse> {
        let mut scheduler = self.scheduler.lock().unwrap();

        let mut response = pb::MapReduceResponse::new();
        let job = match Job::new(JobOptions::from(req)) {
            Ok(job) => job,
            Err(_) => return SingleResponse::err(Error::Other(JOB_SCHEDULE_ERROR)),
        };
        response.mapreduce_id = job.id.clone();
        match scheduler.schedule_job(job) {
            Err(err) => {
                output_error(&err.chain_err(|| "Error scheduling map reduce job."));
                SingleResponse::err(Error::Other(JOB_SCHEDULE_ERROR))
            }
            Ok(_) => SingleResponse::completed(response),
        }
    }

    fn map_reduce_status(
        &self,
        _: RequestOptions,
        req: pb::MapReduceStatusRequest,
    ) -> SingleResponse<pb::MapReduceStatusResponse> {
        let scheduler = self.scheduler.lock().unwrap();

        let mut response = pb::MapReduceStatusResponse::new();
        let jobs: Vec<&Job>;

        if !req.client_id.is_empty() {
            match scheduler.get_mapreduce_client_status(&req.client_id) {
                Err(err) => {
                    output_error(&err.chain_err(|| "Error getting mapreduces for client."));
                    return SingleResponse::err(Error::Other(JOB_RETRIEVAL_ERROR));
                }
                Ok(jbs) => jobs = jbs,
            }
        } else if !req.mapreduce_id.is_empty() {
            match scheduler.get_mapreduce_status(&req.mapreduce_id) {
                Err(err) => {
                    output_error(&err.chain_err(|| "Error getting mapreduces status."));
                    return SingleResponse::err(Error::Other(JOB_RETRIEVAL_ERROR));
                }
                Ok(job) => jobs = vec![job],
            }
        } else {
            return SingleResponse::err(Error::Other(MISSING_JOB_IDS));
        }

        for job in jobs {
            let mut report = pb::MapReduceReport::new();
            report.mapreduce_id = job.id.clone();
            report.status = job.status;
            if job.status == pb::Status::FAILED {
                report.failure_details = job.status_details.clone().unwrap_or_else(
                    || "Unknown.".to_owned(),
                );
            }
            report.scheduled_timestamp = job.time_requested.timestamp();
            report.output_directory = job.output_directory.clone();
            if let Some(time) = job.time_started {
                report.started_timestamp = time.timestamp();
            }
            if let Some(time) = job.time_completed {
                report.done_timestamp = time.timestamp();
            }

            response.reports.push(report);
        }

        SingleResponse::completed(response)
    }

    fn cluster_status(
        &self,
        _: RequestOptions,
        _: pb::EmptyMessage,
    ) -> SingleResponse<pb::ClusterStatusResponse> {
        let scheduler = self.scheduler.lock().unwrap();

        let mut response = pb::ClusterStatusResponse::new();
        response.workers = i64::from(scheduler.get_available_workers());
        response.queue_size = scheduler.get_job_queue_size() as i64;

        SingleResponse::completed(response)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use common::{Job, Task};
    use errors::*;
    use mapreduce_tasks::TaskProcessorTrait;
    use cerberus_proto::mapreduce::Status as MapReduceStatus;
    use cerberus_proto::mapreduce_grpc::MapReduceService;

    struct NullTaskProcessor;

    impl TaskProcessorTrait for NullTaskProcessor {
        fn create_map_tasks(&self, _: &Job) -> Result<Vec<Task>> {
            Ok(Vec::new())
        }

        fn create_reduce_tasks(&self, _: &Job, _: &[&Task]) -> Result<Vec<Task>> {
            Ok(Vec::new())
        }
    }

    fn create_scheduler() -> Scheduler {
        Scheduler::new(Box::new(NullTaskProcessor {}))
    }

    #[test]
    fn queue_mapreduce() {
        let scheduler = create_scheduler();
        assert!(!scheduler.get_map_reduce_in_progress());

        let master_impl = ClientService { scheduler: Arc::new(Mutex::new(scheduler)) };

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
        let mut scheduler = create_scheduler();

        let job = Job::new(JobOptions::default()).unwrap();
        let mut request = pb::MapReduceStatusRequest::new();
        request.mapreduce_id = job.id.clone();
        let result = scheduler.schedule_job(job);
        assert!(result.is_ok());

        let master_impl = ClientService { scheduler: Arc::new(Mutex::new(scheduler)) };
        let response = master_impl.map_reduce_status(RequestOptions::new(), request);

        let (_, mut item, _) = response.wait().unwrap();
        let status = item.reports.pop().unwrap().status;

        assert_eq!(MapReduceStatus::IN_PROGRESS, status)
    }

    #[test]
    fn cluster_status() {
        let mut scheduler = create_scheduler();
        scheduler.set_available_workers(5);
        let _ = scheduler.schedule_job(Job::new(JobOptions::default()).unwrap());
        let _ = scheduler.schedule_job(Job::new(JobOptions::default()).unwrap());

        let master_impl = ClientService { scheduler: Arc::new(Mutex::new(scheduler)) };
        let response = master_impl.cluster_status(RequestOptions::new(), pb::EmptyMessage::new());
        let (_, item, _) = response.wait().unwrap();

        assert_eq!(5, item.workers);
        assert_eq!(2, item.queue_size);
    }
}
