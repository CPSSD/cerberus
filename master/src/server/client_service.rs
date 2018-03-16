use std::sync::Arc;

use grpc::{SingleResponse, Error, RequestOptions};

use common::{Job, JobOptions};
use scheduling::Scheduler;
use util::output_error;
use util::data_layer::AbstractionLayer;

use cerberus_proto::mapreduce as pb;
use cerberus_proto::mapreduce_grpc as grpc_pb;

const JOB_CANCEL_ERROR: &str = "Unable to cancel mapreduce job";
const JOB_SCHEDULE_ERROR: &str = "Unable to schedule mapreduce job";
const JOB_RETRIEVAL_ERROR: &str = "Unable to retrieve mapreduce jobs";
const MISSING_JOB_IDS: &str = "No client_id or mapreduce_id provided";

/// `ClientService` recieves communication from a client.
pub struct ClientService {
    scheduler: Arc<Scheduler>,
    data_abstraction_layer: Arc<AbstractionLayer + Send + Sync>,
    testing: bool,
}

impl ClientService {
    pub fn new(
        scheduler: Arc<Scheduler>,
        data_abstraction_layer: Arc<AbstractionLayer + Send + Sync>,
    ) -> Self {
        ClientService {
            scheduler: scheduler,
            data_abstraction_layer: data_abstraction_layer,
            testing: false,
        }
    }
}

impl grpc_pb::MapReduceService for ClientService {
    fn perform_map_reduce(
        &self,
        _: RequestOptions,
        req: pb::MapReduceRequest,
    ) -> SingleResponse<pb::MapReduceResponse> {
        let mut response = pb::MapReduceResponse::new();
        let mut job_options = JobOptions::from(req);

        if self.testing {
            job_options.validate_paths = false;
        }

        let job = match Job::new(job_options, &self.data_abstraction_layer) {
            Ok(job) => job,
            Err(err) => {
                output_error(&err.chain_err(|| "Error processing map reduce request."));
                return SingleResponse::err(Error::Other(JOB_SCHEDULE_ERROR));
            }
        };

        response.mapreduce_id = job.id.clone();
        match self.scheduler.schedule_job(job) {
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
        let mut response = pb::MapReduceStatusResponse::new();
        let jobs: Vec<Job>;

        if !req.client_id.is_empty() {
            jobs = self.scheduler.get_mapreduce_client_status(&req.client_id);
        } else if !req.mapreduce_id.is_empty() {
            match self.scheduler.get_mapreduce_status(&req.mapreduce_id) {
                Err(err) => {
                    output_error(&err.chain_err(|| "Error getting mapreduces status."));
                    return SingleResponse::err(Error::Other(JOB_RETRIEVAL_ERROR));
                }
                Ok(job) => jobs = vec![job],
            }
        } else {
            error!("Client requested job status without job id or client id.");
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

    fn cancel_map_reduce(
        &self,
        _: RequestOptions,
        req: pb::MapReduceCancelRequest,
    ) -> SingleResponse<pb::MapReduceCancelResponse> {
        let mut response = pb::MapReduceCancelResponse::new();
        let job_id = {
            if !req.mapreduce_id.is_empty() {
                req.mapreduce_id
            } else {
                match self.scheduler.get_most_recent_client_job_id(&req.client_id) {
                    Err(err) => {
                        output_error(&err.chain_err(|| "Error cancelling MapReduce."));
                        return SingleResponse::err(Error::Other("No jobs found for this client"));
                    }
                    Ok(job_id) => job_id,
                }
            }
        };
        response.set_mapreduce_id(job_id.clone());

        println!("Attempting to cancel MapReduce: {}", job_id);
        let result = self.scheduler.cancel_job(job_id.as_ref());
        if let Err(err) = result {
            output_error(&err.chain_err(|| "Error cancelling MapReduce"));
            return SingleResponse::err(Error::Other(JOB_CANCEL_ERROR));
        }

        SingleResponse::completed(response)
    }

    fn cluster_status(
        &self,
        _: RequestOptions,
        _: pb::EmptyMessage,
    ) -> SingleResponse<pb::ClusterStatusResponse> {
        let mut response = pb::ClusterStatusResponse::new();
        response.workers = i64::from(self.scheduler.get_available_workers());
        response.queue_size = i64::from(self.scheduler.get_job_queue_size());

        SingleResponse::completed(response)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use common::{Job, Task, Worker};
    use errors::*;
    use scheduling::TaskProcessor;
    use cerberus_proto::worker as wpb;
    use cerberus_proto::mapreduce::Status as MapReduceStatus;
    use cerberus_proto::mapreduce_grpc::MapReduceService;
    use util::data_layer::NullAbstractionLayer;
    use worker_management::WorkerManager;
    use worker_communication::WorkerInterface;

    struct NullTaskProcessor;

    impl TaskProcessor for NullTaskProcessor {
        fn create_map_tasks(&self, _: &Job) -> Result<Vec<Task>> {
            Ok(Vec::new())
        }

        fn create_reduce_tasks(&self, _: &Job, _: Vec<&Task>) -> Result<Vec<Task>> {
            Ok(Vec::new())
        }
    }

    struct NullWorkerInterface;

    impl WorkerInterface for NullWorkerInterface {
        fn add_client(&self, _worker: &Worker) -> Result<()> {
            Ok(())
        }

        fn remove_client(&self, _worker_id: &str) -> Result<()> {
            Ok(())
        }

        fn schedule_map(&self, _request: wpb::PerformMapRequest, _worker_id: &str) -> Result<()> {
            Ok(())
        }

        fn schedule_reduce(
            &self,
            _request: wpb::PerformReduceRequest,
            _worker_id: &str,
        ) -> Result<()> {
            Ok(())
        }

        fn cancel_task(&self, _request: wpb::CancelTaskRequest, _worker_id: &str) -> Result<()> {
            Ok(())
        }
    }

    fn create_scheduler() -> Scheduler {
        Scheduler::new(
            Arc::new(WorkerManager::new(Arc::new(NullWorkerInterface {}))),
            Arc::new(NullTaskProcessor {}),
        )
    }

    #[test]
    fn queue_mapreduce() {
        let scheduler = create_scheduler();
        assert_eq!(0, scheduler.get_job_queue_size());

        let master_impl = ClientService {
            scheduler: Arc::new(scheduler),
            testing: true,
            data_abstraction_layer: Arc::new(NullAbstractionLayer),
        };

        let _ = master_impl
            .perform_map_reduce(RequestOptions::new(), pb::MapReduceRequest::new())
            .wait();

        assert_eq!(1, master_impl.scheduler.get_job_queue_size());
    }

    #[test]
    fn get_mapreduce_status() {
        let scheduler = create_scheduler();

        let job = Job::new_no_validate(JobOptions::default()).unwrap();
        let mut request = pb::MapReduceStatusRequest::new();
        request.mapreduce_id = job.id.clone();
        let result = scheduler.schedule_job(job);
        assert!(result.is_ok());

        let master_impl = ClientService {
            scheduler: Arc::new(scheduler),
            testing: true,
            data_abstraction_layer: Arc::new(NullAbstractionLayer),
        };
        let response = master_impl.map_reduce_status(RequestOptions::new(), request);

        let (_, mut item, _) = response.wait().unwrap();
        let status = item.reports.pop().unwrap().status;

        assert_eq!(MapReduceStatus::IN_QUEUE, status)
    }

    #[test]
    fn cluster_status() {
        let worker_manager = WorkerManager::new(Arc::new(NullWorkerInterface {}));
        for _x in 0..10 {
            let w = Worker::new("172.30.0.1:8008", "").unwrap();
            worker_manager.register_worker(w).unwrap();
        }

        let scheduler = Scheduler::new(Arc::new(worker_manager), Arc::new(NullTaskProcessor {}));

        let data_abstraction_layer: Arc<AbstractionLayer + Send + Sync> =
            Arc::new(NullAbstractionLayer);
        let _ = scheduler.schedule_job(Job::new_no_validate(JobOptions::default()).unwrap());
        let _ = scheduler.schedule_job(Job::new_no_validate(JobOptions::default()).unwrap());

        let master_impl = ClientService {
            scheduler: Arc::new(scheduler),
            testing: true,
            data_abstraction_layer: data_abstraction_layer,
        };
        let response = master_impl.cluster_status(RequestOptions::new(), pb::EmptyMessage::new());
        let (_, item, _) = response.wait().unwrap();

        assert_eq!(10, item.workers);
        assert_eq!(2, item.queue_size);
    }
}
