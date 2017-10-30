use mapreduce_tasks::{MapReduceTask, MapReduceTaskStatus, TaskType};
use std::{thread, time};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use scheduler::MapReduceScheduler;
use worker_communication::WorkerInterface;
use worker_management::{Worker, WorkerManager};
use util::output_error;

use cerberus_proto::worker::WorkerStatusResponse_OperationStatus as OperationStatus;
use cerberus_proto::worker as pb;

const SCHEDULING_LOOP_INTERVAL_MS: u64 = 100;

struct SchedulerResources<I>
where
    I: WorkerInterface + Send + Sync,
{
    scheduler_arc: Arc<Mutex<MapReduceScheduler>>,
    worker_manager_arc: Arc<Mutex<WorkerManager>>,
    worker_interface_arc: Arc<RwLock<I>>,
}

impl<I> Clone for SchedulerResources<I>
where
    I: WorkerInterface + Send + Sync,
{
    fn clone(&self) -> Self {
        SchedulerResources {
            scheduler_arc: Arc::clone(&self.scheduler_arc),
            worker_manager_arc: Arc::clone(&self.worker_manager_arc),
            worker_interface_arc: Arc::clone(&self.worker_interface_arc),
        }
    }
}

struct TaskAssignment {
    task_id: String,
    worker_id: String,
}

fn handle_assign_task_failure<I>(
    scheduler_resources: &SchedulerResources<I>,
    task_assignment: &TaskAssignment,
) where
    I: WorkerInterface + Send + Sync,
{
    match scheduler_resources.worker_manager_arc.lock() {
        Ok(mut worker_manager) => {
            let worker_option = worker_manager.get_worker(&task_assignment.worker_id);
            match worker_option {
                Some(worker) => {
                    worker.set_current_task_id(String::new());
                    worker.set_current_task_type(None);
                    worker.set_operation_status(OperationStatus::UNKNOWN);
                }
                None => {
                    error!("Error reverting worker state on task failure: Worker not found");
                }
            }
        }
        Err(err) => {
            error!("Error reverting worker state on task failure: {}", err);
        }
    }

    match scheduler_resources.scheduler_arc.lock() {
        Ok(mut scheduler) => {
            let result = scheduler.unschedule_task(task_assignment.task_id.to_owned());
            if let Err(err) = result {
                error!("Error reverting task state on task failure: {}", err);
            }
        }
        Err(err) => {
            error!("Error reverting worker state on task failure: {}", err);
        }
    }
}

fn assign_worker_map_task<I>(
    scheduler_resources: SchedulerResources<I>,
    task_assignment: TaskAssignment,
    map_task: pb::PerformMapRequest,
) where
    I: WorkerInterface + Send + Sync + 'static,
{
    thread::spawn(move || {
        let read_guard = scheduler_resources.worker_interface_arc.read();
        match read_guard {
            Ok(worker_interface) => {
                let result = worker_interface.schedule_map(map_task, &task_assignment.worker_id);
                if let Err(err) = result {
                    output_error(&err.chain_err(|| "Error assigning worker task."));
                    handle_assign_task_failure(&scheduler_resources, &task_assignment);
                }
            }
            Err(err) => {
                error!("Error assigning worker task: {}", err);
                handle_assign_task_failure(&scheduler_resources, &task_assignment);
            }
        }
    });
}

fn assign_worker_reduce_task<I>(
    scheduler_resources: SchedulerResources<I>,
    task_assignment: TaskAssignment,
    reduce_task: pb::PerformReduceRequest,
) where
    I: WorkerInterface + Send + Sync + 'static,
{
    thread::spawn(move || {
        let read_guard = scheduler_resources.worker_interface_arc.read();
        match read_guard {
            Ok(worker_interface) => {
                let result =
                    worker_interface.schedule_reduce(reduce_task, &task_assignment.worker_id);
                if let Err(err) = result {
                    output_error(&err.chain_err(|| "Error assigning worker task."));
                    handle_assign_task_failure(&scheduler_resources, &task_assignment);
                }
            }
            Err(err) => {
                error!("Error assigning worker task: {}", err);
                handle_assign_task_failure(&scheduler_resources, &task_assignment);
            }
        }
    });
}

fn assign_worker_task<I>(
    scheduler_resources: SchedulerResources<I>,
    task_assignment: TaskAssignment,
    task: &MapReduceTask,
) where
    I: WorkerInterface + Send + Sync + 'static,
{
    match task.get_task_type() {
        TaskType::Map => {
            let map_request = {
                match task.get_perform_map_request() {
                    None => {
                        error!("Error assigning task: Map request not found.");
                        handle_assign_task_failure(&scheduler_resources, &task_assignment);
                        return;
                    }
                    Some(req) => req,
                }
            };
            assign_worker_map_task(scheduler_resources, task_assignment, map_request)
        }
        TaskType::Reduce => {
            let reduce_request = {
                match task.get_perform_reduce_request() {
                    None => {
                        error!("Error assigning task: Reduce request not found.");
                        handle_assign_task_failure(&scheduler_resources, &task_assignment);
                        return;
                    }
                    Some(req) => req,
                }
            };
            assign_worker_reduce_task(scheduler_resources, task_assignment, reduce_request);
        }
    }
}

fn do_scheduling_loop_step<I>(
    scheduler_resources: &SchedulerResources<I>,
    mut scheduler: MutexGuard<MapReduceScheduler>,
    mut worker_manager: MutexGuard<WorkerManager>,
) where
    I: WorkerInterface + Send + Sync + 'static,
{
    scheduler.set_available_workers(worker_manager.get_total_workers());
    let mut available_workers: Vec<&mut Worker> = worker_manager.get_available_workers();
    while scheduler.get_map_reduce_task_queue_size() > 0 {
        if available_workers.is_empty() {
            break;
        }

        let task: &mut MapReduceTask = {
            match scheduler.pop_queued_map_reduce_task() {
                Some(task) => task,
                None => {
                    error!("Unexpected error getting queued map reduce task");
                    break;
                }
            }
        };

        let worker: &mut Worker = {
            match available_workers.pop() {
                Some(worker) => worker,
                None => {
                    error!("Unexpected error getting available worker");
                    break;
                }
            }
        };

        worker.set_current_task_id(task.get_task_id());
        worker.set_current_task_type(Some(task.get_task_type()));
        worker.set_operation_status(OperationStatus::IN_PROGRESS);

        task.set_assigned_worker_id(worker.get_worker_id());
        task.set_status(MapReduceTaskStatus::InProgress);

        let task_assignment = TaskAssignment {
            task_id: task.get_task_id().to_owned(),
            worker_id: worker.get_worker_id().to_owned(),
        };

        assign_worker_task(scheduler_resources.clone(), task_assignment, task);
    }
}

pub fn run_scheduling_loop<I>(
    worker_interface_arc: Arc<RwLock<I>>,
    scheduler_arc: Arc<Mutex<MapReduceScheduler>>,
    worker_manager_arc: Arc<Mutex<WorkerManager>>,
) where
    I: WorkerInterface + Send + Sync + 'static,
{
    thread::spawn(move || loop {
        thread::sleep(time::Duration::from_millis(SCHEDULING_LOOP_INTERVAL_MS));

        let scheduler = {
            match scheduler_arc.lock() {
                Ok(scheduler) => scheduler,
                Err(e) => {
                    error!("Error obtaining scheduler: {}", e);
                    continue;
                }
            }
        };

        let worker_manager = {
            match worker_manager_arc.lock() {
                Ok(worker_manager) => worker_manager,
                Err(e) => {
                    error!("Error obtaining worker manager: {}", e);
                    continue;
                }
            }
        };

        do_scheduling_loop_step(
            &SchedulerResources {
                scheduler_arc: Arc::clone(&scheduler_arc),
                worker_manager_arc: Arc::clone(&worker_manager_arc),
                worker_interface_arc: Arc::clone(&worker_interface_arc),
            },
            scheduler,
            worker_manager,
        );
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use errors::*;
    use mapreduce_job::MapReduceJob;
    use mapreduce_job::MapReduceJobOptions;
    use mapreduce_tasks::TaskProcessorTrait;
    use worker_management::WorkerTaskType;

    struct WorkerInterfaceStub;

    impl WorkerInterface for WorkerInterfaceStub {
        fn add_client(&mut self, _worker: &Worker) -> Result<()> {
            Ok(())
        }
        fn remove_client(&mut self, _worker_id: &str) -> Result<()> {
            Ok(())
        }
        fn get_worker_status(&self, _worker_id: &str) -> Result<pb::WorkerStatusResponse> {
            Ok(pb::WorkerStatusResponse::new())
        }
        fn schedule_map(&self, _request: pb::PerformMapRequest, _worker_id: &str) -> Result<()> {
            Ok(())
        }
        fn schedule_reduce(
            &self,
            _request: pb::PerformReduceRequest,
            _worker_id: &str,
        ) -> Result<()> {
            Ok(())
        }
        fn get_map_result(&self, _worker_id: &str) -> Result<pb::MapResponse> {
            Ok(pb::MapResponse::new())
        }
        fn get_reduce_result(&self, _worker_id: &str) -> Result<pb::ReduceResponse> {
            Ok(pb::ReduceResponse::new())
        }
    }

    struct TaskProcessorStub {
        map_tasks: Vec<MapReduceTask>,
        reduce_tasks: Vec<MapReduceTask>,
    }

    impl TaskProcessorStub {
        fn new(map_tasks: Vec<MapReduceTask>, reduce_tasks: Vec<MapReduceTask>) -> Self {
            TaskProcessorStub {
                map_tasks: map_tasks,
                reduce_tasks: reduce_tasks,
            }
        }
    }

    impl TaskProcessorTrait for TaskProcessorStub {
        fn create_map_tasks(&self, _map_reduce_job: &MapReduceJob) -> Result<Vec<MapReduceTask>> {
            Ok(self.map_tasks.clone())
        }
        fn create_reduce_tasks(
            &self,
            _map_reduce_job: &MapReduceJob,
            _completed_map_tasks: &[&MapReduceTask],
        ) -> Result<Vec<MapReduceTask>> {
            Ok(self.reduce_tasks.clone())
        }
    }

    fn create_map_reduce_scheduler(map_task: MapReduceTask) -> MapReduceScheduler {
        let mut map_reduce_scheduler =
            MapReduceScheduler::new(Box::new(TaskProcessorStub::new(vec![map_task], Vec::new())));
        let map_reduce_job = MapReduceJob::new(MapReduceJobOptions {
            client_id: "client-1".to_owned(),
            binary_path: "/tmp/bin".to_owned(),
            input_directory: "/tmp/input".to_owned(),
            output_directory: Some("/tmp/output".to_owned()),
        }).unwrap();
        map_reduce_scheduler
            .schedule_map_reduce(map_reduce_job)
            .unwrap();
        map_reduce_scheduler
    }

    fn create_worker_manager(worker: Worker) -> WorkerManager {
        let mut worker_manager = WorkerManager::new();
        worker_manager.add_worker(worker.clone());

        worker_manager
    }

    #[test]
    fn test_scheduling_assign_map() {
        let map_task = MapReduceTask::new_map_task("map-reduce1", "/tmp/bin", "input-1");
        let worker = Worker::new(String::from("127.0.0.1:8080")).unwrap();

        let map_reduce_scheduler = create_map_reduce_scheduler(map_task);
        let worker_interface = WorkerInterfaceStub;
        let worker_manager = create_worker_manager(worker);

        let scheduling_resources = &SchedulerResources {
            scheduler_arc: Arc::new(Mutex::new(map_reduce_scheduler)),
            worker_manager_arc: Arc::new(Mutex::new(worker_manager)),
            worker_interface_arc: Arc::new(RwLock::new(worker_interface)),
        };

        do_scheduling_loop_step(
            scheduling_resources,
            scheduling_resources.scheduler_arc.lock().unwrap(),
            scheduling_resources.worker_manager_arc.lock().unwrap(),
        );

        let map_reduce_scheduler = scheduling_resources.scheduler_arc.lock().unwrap();
        let worker_manager = scheduling_resources.worker_manager_arc.lock().unwrap();

        assert_eq!(0, map_reduce_scheduler.get_map_reduce_task_queue_size());
        assert!(!worker_manager.get_workers()[0]
            .get_current_task_id()
            .is_empty());
        assert_eq!(
            WorkerTaskType::Map,
            worker_manager.get_workers()[0].get_current_task_type()
        );
    }

    #[test]
    fn test_handle_assign_task_failure() {
        let map_task = MapReduceTask::new_map_task("map-reduce1", "/tmp/bin", "input-1");
        let worker = Worker::new(String::from("127.0.0.1:8080")).unwrap();

        let worker_id = worker.get_worker_id().to_owned();
        let task_id = map_task.get_task_id().to_owned();

        let map_reduce_scheduler = create_map_reduce_scheduler(map_task);
        let worker_interface = WorkerInterfaceStub;
        let worker_manager = create_worker_manager(worker);

        let scheduling_resources = &SchedulerResources {
            scheduler_arc: Arc::new(Mutex::new(map_reduce_scheduler)),
            worker_manager_arc: Arc::new(Mutex::new(worker_manager)),
            worker_interface_arc: Arc::new(RwLock::new(worker_interface)),
        };

        do_scheduling_loop_step(
            scheduling_resources,
            scheduling_resources.scheduler_arc.lock().unwrap(),
            scheduling_resources.worker_manager_arc.lock().unwrap(),
        );

        let task_assignment = TaskAssignment {
            task_id: task_id,
            worker_id: worker_id,
        };

        handle_assign_task_failure(scheduling_resources, &task_assignment);

        let map_reduce_scheduler = scheduling_resources.scheduler_arc.lock().unwrap();
        let worker_manager = scheduling_resources.worker_manager_arc.lock().unwrap();

        assert_eq!(1, map_reduce_scheduler.get_map_reduce_task_queue_size());
        assert!(
            worker_manager.get_workers()[0]
                .get_current_task_id()
                .is_empty()
        );
        assert_eq!(
            WorkerTaskType::Idle,
            worker_manager.get_workers()[0].get_current_task_type()
        );
    }
}
