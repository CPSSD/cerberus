use mapreduce_tasks::{MapReduceTask, MapReduceTaskStatus, TaskType};
use std::{thread, time};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use scheduler::MapReduceScheduler;
use worker_communication::WorkerInterface;
use worker_management::{Worker, WorkerManager};

use cerberus_proto::mrworker::WorkerStatusResponse_OperationStatus as OperationStatus;
use cerberus_proto::mrworker as pb;

const SCHEDULING_LOOP_INTERVAL_MS: u64 = 100;

#[derive(Clone)]
struct SchedulerResources {
    scheduler_arc: Arc<Mutex<MapReduceScheduler>>,
    worker_manager_arc: Arc<Mutex<WorkerManager>>,
    worker_interface_arc: Arc<RwLock<WorkerInterface>>,
}

fn handle_assign_task_failure(
    scheduler_resources: &SchedulerResources,
    worker_id: &str,
    task_id: &str,
) {
    match scheduler_resources.worker_manager_arc.lock() {
        Ok(mut worker_manager) => {
            let worker_option = worker_manager.get_worker(worker_id);
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
            let result = scheduler.unschedule_task(task_id.to_owned());
            if let Err(err) = result {
                error!("Error reverting task state on task failure: {}", err);
            }
        }
        Err(err) => {
            error!("Error reverting worker state on task failure: {}", err);
        }
    }
}

fn assign_worker_map_task(
    scheduler_resources: SchedulerResources,
    worker_id: String,
    task_id: String,
    map_task: pb::PerformMapRequest,
) {
    thread::spawn(move || {
        let read_guard = scheduler_resources.worker_interface_arc.read();
        match read_guard {
            Ok(worker_interface) => {
                let result = worker_interface.schedule_map(map_task, &worker_id);
                if let Err(err) = result {
                    error!("Error assigning worker task: {}", err);
                    handle_assign_task_failure(&scheduler_resources, &worker_id, &task_id);
                }
            }
            Err(err) => {
                error!("Error assigning worker task: {}", err);
                handle_assign_task_failure(&scheduler_resources, &worker_id, &task_id);
            }
        }
    });
}

fn assign_worker_reduce_task(
    scheduler_resources: SchedulerResources,
    worker_id: String,
    task_id: String,
    reduce_task: pb::PerformReduceRequest,
) {
    thread::spawn(move || {
        let read_guard = scheduler_resources.worker_interface_arc.read();
        match read_guard {
            Ok(worker_interface) => {
                let result = worker_interface.schedule_reduce(reduce_task, &worker_id);
                if let Err(err) = result {
                    error!("Error assigning worker task: {}", err);
                    handle_assign_task_failure(&scheduler_resources, &worker_id, &task_id);
                }
            }
            Err(err) => {
                error!("Error assigning worker task: {}", err);
                handle_assign_task_failure(&scheduler_resources, &worker_id, &task_id);
            }
        }
    });
}

fn assign_worker_task<S: Into<String>>(
    scheduler_resources: SchedulerResources,
    worker_id: S,
    task_id: S,
    task: &MapReduceTask,
) {
    match task.get_task_type() {
        TaskType::Map => {
            let map_request = {
                match task.get_perform_map_request() {
                    None => {
                        error!("Error assigning task: Map request not found.");
                        handle_assign_task_failure(
                            &scheduler_resources,
                            &worker_id.into(),
                            &task_id.into(),
                        );
                        return;
                    }
                    Some(req) => req,
                }
            };
            assign_worker_map_task(
                scheduler_resources,
                worker_id.into(),
                task_id.into(),
                map_request,
            )
        }
        TaskType::Reduce => {
            let reduce_request = {
                match task.get_perform_reduce_request() {
                    None => {
                        error!("Error assigning task: Reduce request not found.");
                        handle_assign_task_failure(
                            &scheduler_resources,
                            &worker_id.into(),
                            &task_id.into(),
                        );
                        return;
                    }
                    Some(req) => req,
                }
            };
            assign_worker_reduce_task(
                scheduler_resources,
                worker_id.into(),
                task_id.into(),
                reduce_request,
            );
        }
    }
}

fn do_scheduling_loop_step(
    scheduler_resources: &SchedulerResources,
    mut scheduler: MutexGuard<MapReduceScheduler>,
    mut worker_manager: MutexGuard<WorkerManager>,
) {
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

        assign_worker_task(
            scheduler_resources.clone(),
            worker.get_worker_id(),
            task.get_task_id(),
            task,
        );
    }
}

pub fn run_scheduling_loop(
    worker_interface_arc: Arc<RwLock<WorkerInterface>>,
    scheduler_arc: Arc<Mutex<MapReduceScheduler>>,
    worker_manager_arc: Arc<Mutex<WorkerManager>>,
) {
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
mod tests {}
