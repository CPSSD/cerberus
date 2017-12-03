use std::{thread, time};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

use chrono::prelude::*;

use common::{Task, TaskStatus, TaskType, Worker};
use errors::*;
use scheduler::Scheduler;
use worker_communication::WorkerInterface;
use worker_management::WorkerManager;
use util::output_error;

use cerberus_proto::worker as pb;

const SCHEDULING_LOOP_INTERVAL_MS: u64 = 100;
const TIME_BEFORE_WORKER_TASK_REASSIGNMENT_S: i64 = 60;
const TIME_BEFORE_WORKER_TERMINATION_S: i64 = 120;

struct SchedulerResources<I>
where
    I: WorkerInterface + Send + Sync,
{
    scheduler_arc: Arc<Mutex<Scheduler>>,
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

/// `handle_assign_task_failure` reschedules a task and marks a worker as free in the case that a
/// task assignment could not be completed. This normally happens when there is a networking error.
fn handle_assign_task_failure<I>(
    scheduler_resources: &SchedulerResources<I>,
    task_assignment: &TaskAssignment,
) where
    I: WorkerInterface + Send + Sync,
{
    let mut worker_manager = scheduler_resources.worker_manager_arc.lock().unwrap();
    match worker_manager.get_worker(&task_assignment.worker_id) {
        Some(worker) => {
            worker.current_task_id = String::new();
            worker.operation_status = pb::OperationStatus::UNKNOWN;
        }
        None => error!("Error reverting worker state on task failure: Worker not found"),
    }

    let mut scheduler = scheduler_resources.scheduler_arc.lock().unwrap();
    let result = scheduler.unschedule_task(&task_assignment.task_id);
    if let Err(err) = result {
        error!("Error reverting task state on task failure: {}", err);
    }
}

/// `assign_worker_map_task` assigns a map task to a worker using the worker interface.
fn assign_worker_map_task<I>(
    scheduler_resources: SchedulerResources<I>,
    task_assignment: TaskAssignment,
    map_task: pb::PerformMapRequest,
) where
    I: WorkerInterface + Send + Sync + 'static,
{
    thread::spawn(move || {
        let worker_interface = scheduler_resources.worker_interface_arc.read().unwrap();
        let result = worker_interface.schedule_map(map_task, &task_assignment.worker_id);
        if let Err(err) = result {
            output_error(&err.chain_err(|| "Error assigning worker task."));
            handle_assign_task_failure(&scheduler_resources, &task_assignment);
        }
    });
}

/// `assign_worker_reduce_task` assigns a reduce task to a worker using the worker interface.
fn assign_worker_reduce_task<I>(
    scheduler_resources: SchedulerResources<I>,
    task_assignment: TaskAssignment,
    reduce_task: pb::PerformReduceRequest,
) where
    I: WorkerInterface + Send + Sync + 'static,
{
    thread::spawn(move || {
        let worker_interface = scheduler_resources.worker_interface_arc.read().unwrap();
        let result = worker_interface.schedule_reduce(reduce_task, &task_assignment.worker_id);
        if let Err(err) = result {
            output_error(&err.chain_err(|| "Error assigning worker task."));
            handle_assign_task_failure(&scheduler_resources, &task_assignment);
        }
    });
}

fn assign_worker_task<I>(
    scheduler_resources: SchedulerResources<I>,
    task_assignment: TaskAssignment,
    task: &Task,
) where
    I: WorkerInterface + Send + Sync + 'static,
{
    match task.task_type {
        TaskType::Map => {
            let map_request = {
                match task.map_request {
                    None => {
                        error!("Error assigning task: Map request not found.");
                        handle_assign_task_failure(&scheduler_resources, &task_assignment);
                        return;
                    }
                    Some(ref req) => req.clone(),
                }
            };
            assign_worker_map_task(scheduler_resources, task_assignment, map_request)
        }
        TaskType::Reduce => {
            let reduce_request = {
                match task.reduce_request {
                    None => {
                        error!("Error assigning task: Reduce request not found.");
                        handle_assign_task_failure(&scheduler_resources, &task_assignment);
                        return;
                    }
                    Some(ref req) => req.clone(),
                }
            };
            assign_worker_reduce_task(scheduler_resources, task_assignment, reduce_request);
        }
    }
}

/// `do_scheduling_loop_step` assigns `Task`s to available workers.
fn do_scheduling_loop_step<I>(
    scheduler_resources: &SchedulerResources<I>,
    mut scheduler: MutexGuard<Scheduler>,
    mut worker_manager: MutexGuard<WorkerManager>,
) where
    I: WorkerInterface + Send + Sync + 'static,
{
    scheduler.set_available_workers(worker_manager.get_total_workers());
    let mut available_workers: Vec<&mut Worker> = worker_manager.get_available_workers();

    // Sort workers by most recent health checks, to avoid repeatedly trying to assign work to a
    // worker which is not responding.
    available_workers.sort_by(|a, b| {
        a.status_last_updated.cmp(&b.status_last_updated).reverse()
    });

    while scheduler.get_task_queue_size() > 0 {
        if available_workers.is_empty() {
            break;
        }

        let task: &mut Task = {
            match scheduler.pop_queued_task() {
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

        worker.current_task_id = task.id.to_owned();
        worker.operation_status = pb::OperationStatus::IN_PROGRESS;

        task.assigned_worker_id = worker.worker_id.to_owned();
        task.status = TaskStatus::InProgress;

        let task_assignment = TaskAssignment {
            task_id: task.id.to_owned(),
            worker_id: worker.worker_id.to_owned(),
        };

        assign_worker_task(scheduler_resources.clone(), task_assignment, task);
    }
}

/// Remove a task from its worker and push it back onto the queue.
fn reschedule_task(task_id: &str, scheduler: &mut MutexGuard<Scheduler>) -> Result<()> {
    scheduler.unschedule_task(task_id).chain_err(|| {
        format!("Unable to unschedule task with ID {:?}.", task_id)
    })?;
    Ok(())
}

/// Remove a worker from the list of available workers.
fn remove_worker(
    worker_id: &str,
    worker_manager: &mut MutexGuard<WorkerManager>,
    scheduler: &mut MutexGuard<Scheduler>,
) {
    worker_manager.remove_worker(worker_id);
    let available_workers = scheduler.get_available_workers();
    scheduler.set_available_workers(available_workers - 1);
}

/// `update_healthy_workers` checks which workers have likely failed and
/// need to have their tasks restarted.
// If a worker has not updated it's status for a long time it is removed from the list of workers.
fn update_healthy_workers(
    scheduler: &mut MutexGuard<Scheduler>,
    worker_manager: &mut MutexGuard<WorkerManager>,
) {
    let mut workers_to_reassign = Vec::new();
    let mut workers_to_remove = Vec::new();

    for worker in worker_manager.get_workers() {
        let time_since_worker_updated = Utc::now().timestamp() -
            worker.status_last_updated.timestamp();

        if time_since_worker_updated >= TIME_BEFORE_WORKER_TERMINATION_S &&
            worker.current_task_id.is_empty()
        {
            workers_to_remove.push(worker.worker_id.to_owned());
        }

        if time_since_worker_updated >= TIME_BEFORE_WORKER_TASK_REASSIGNMENT_S &&
            !worker.current_task_id.is_empty()
        {
            workers_to_reassign.push(worker.worker_id.to_owned());
        }
    }

    for worker_id in workers_to_reassign {
        let worker = match worker_manager.get_worker(&worker_id) {
            Some(worker) => worker,
            None => {
                error!("Error rescheduling task for worker, id: {}", worker_id);
                continue;
            }
        };

        match reschedule_task(&worker.current_task_id, scheduler) {
            Ok(_) => worker.current_task_id = String::new(),
            Err(err) => error!("Error trying to reschedule task: {}", err),
        }
    }

    for worker_id in workers_to_remove {
        remove_worker(&worker_id, worker_manager, scheduler);
    }
}

/// `run_scheduling_loop` is the main loop that performs all functionality related to assigning
/// `Task`s to workers.
pub fn run_scheduling_loop<I>(
    worker_interface_arc: Arc<RwLock<I>>,
    scheduler_arc: Arc<Mutex<Scheduler>>,
    worker_manager_arc: Arc<Mutex<WorkerManager>>,
) where
    I: WorkerInterface + Send + Sync + 'static,
{
    thread::spawn(move || loop {
        thread::sleep(time::Duration::from_millis(SCHEDULING_LOOP_INTERVAL_MS));

        let mut scheduler = scheduler_arc.lock().unwrap();
        let mut worker_manager = worker_manager_arc.lock().unwrap();

        update_healthy_workers(&mut scheduler, &mut worker_manager);

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
    use common::{Job, JobOptions};
    use mapreduce_tasks::TaskProcessorTrait;

    struct WorkerInterfaceStub;

    impl WorkerInterface for WorkerInterfaceStub {
        fn add_client(&self, _worker: &Worker) -> Result<()> {
            Ok(())
        }
        fn remove_client(&self, _worker_id: &str) -> Result<()> {
            Ok(())
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
    }

    struct TaskProcessorStub {
        map_tasks: Vec<Task>,
        reduce_tasks: Vec<Task>,
    }

    impl TaskProcessorStub {
        fn new(map_tasks: Vec<Task>, reduce_tasks: Vec<Task>) -> Self {
            TaskProcessorStub {
                map_tasks: map_tasks,
                reduce_tasks: reduce_tasks,
            }
        }
    }

    impl TaskProcessorTrait for TaskProcessorStub {
        fn create_map_tasks(&self, _job: &Job) -> Result<Vec<Task>> {
            Ok(self.map_tasks.clone())
        }
        fn create_reduce_tasks(
            &self,
            __job: &Job,
            _completed_map_tasks: &[&Task],
        ) -> Result<Vec<Task>> {
            Ok(self.reduce_tasks.clone())
        }
    }

    fn create_scheduler(map_task: Task) -> Scheduler {
        let mut scheduler =
            Scheduler::new(Box::new(TaskProcessorStub::new(vec![map_task], Vec::new())));
        let job = Job::new(JobOptions {
            client_id: "client-1".to_owned(),
            binary_path: "/tmp/bin".to_owned(),
            input_directory: "/tmp/input".to_owned(),
            output_directory: Some("/tmp/output".to_owned()),
            validate_paths: false,
        }).unwrap();
        scheduler.schedule_job(job).unwrap();
        scheduler
    }

    fn create_worker_manager(worker: Worker) -> WorkerManager {
        let mut worker_manager = WorkerManager::new(None, None);
        worker_manager.add_worker(worker.clone());

        worker_manager
    }

    #[test]
    fn test_scheduling_assign_map() {
        let map_task = Task::new_map_task("map-reduce1", "/tmp/bin", "input-1");
        let worker = Worker::new(String::from("127.0.0.1:8080")).unwrap();

        let scheduler = create_scheduler(map_task);
        let worker_interface = WorkerInterfaceStub;
        let worker_manager = create_worker_manager(worker);

        let scheduling_resources = &SchedulerResources {
            scheduler_arc: Arc::new(Mutex::new(scheduler)),
            worker_manager_arc: Arc::new(Mutex::new(worker_manager)),
            worker_interface_arc: Arc::new(RwLock::new(worker_interface)),
        };

        do_scheduling_loop_step(
            scheduling_resources,
            scheduling_resources.scheduler_arc.lock().unwrap(),
            scheduling_resources.worker_manager_arc.lock().unwrap(),
        );

        let scheduler = scheduling_resources.scheduler_arc.lock().unwrap();
        let worker_manager = scheduling_resources.worker_manager_arc.lock().unwrap();

        assert_eq!(0, scheduler.get_task_queue_size());
        assert!(!worker_manager.get_workers()[0].current_task_id.is_empty());
    }

    #[test]
    fn test_handle_assign_task_failure() {
        let map_task = Task::new_map_task("map-reduce1", "/tmp/bin", "input-1");
        let worker = Worker::new(String::from("127.0.0.1:8080")).unwrap();

        let worker_id = worker.worker_id.to_owned();
        let task_id = map_task.id.to_owned();

        let scheduler = create_scheduler(map_task);
        let worker_interface = WorkerInterfaceStub;
        let worker_manager = create_worker_manager(worker);

        let scheduling_resources = &SchedulerResources {
            scheduler_arc: Arc::new(Mutex::new(scheduler)),
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

        let scheduler = scheduling_resources.scheduler_arc.lock().unwrap();
        let worker_manager = scheduling_resources.worker_manager_arc.lock().unwrap();

        assert_eq!(1, scheduler.get_task_queue_size());
        assert!(worker_manager.get_workers()[0].current_task_id.is_empty());
    }
}
