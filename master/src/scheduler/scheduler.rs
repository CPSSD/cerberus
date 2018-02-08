use std::thread;
use std::collections::HashMap;
use std::sync::{Mutex, Arc};

use chrono::Utc;

use cerberus_proto::mapreduce::Status as JobStatus;
use common::{Task, Job};
use errors::*;
use scheduler::state::{ScheduledJob, State};
use scheduler::task_processor::TaskProcessor;
use worker_management::WorkerManager;
use util::output_error;

/// The `Scheduler` is responsible for the managing of `Job`s and `Task`s.
///
/// It delegates work to several child modules, and to the
/// [`WorkerManager`](worker_management::WorkerManager).
pub struct Scheduler {
    state: Arc<Mutex<State>>,

    worker_manager: Arc<WorkerManager>,
    task_processor: Arc<TaskProcessor + Send + Sync>,
}

impl Scheduler {
    /// Construct a new `Scheduler`, using the default [`CpuPool`](futures_cpupool::CpuPool).
    pub fn new(
        worker_manager: Arc<WorkerManager>,
        task_processor: Arc<TaskProcessor + Send + Sync>,
    ) -> Self {
        let state = Arc::new(Mutex::new(State::new()));
        Scheduler {
            state: state,

            worker_manager: worker_manager,
            task_processor: task_processor,
        }
    }

    fn process_completed_task(&self, task: Task) -> Result<()> {
        let mut state = self.state.lock().unwrap();

        state.add_completed_task(task.clone()).chain_err(
            || "Error processing completed task result",
        )?;

        let reduce_tasks_required = state
            .reduce_tasks_required(task.job_id.to_owned())
            .chain_err(|| "Error processing completed task result")?;

        if reduce_tasks_required {
            // TODO: finish here
            let job = state.get_job(task.job_id.clone()).chain_err(
                || "Error processing completed task result.",
            )?;

            let map_tasks = state.get_map_tasks(task.job_id.clone()).chain_err(
                || "Error processing completed task result.",
            )?;

            let reduce_tasks = self.task_processor
                .create_reduce_tasks(&job, map_tasks)
                .chain_err(|| "Error processing completed task results.")?;

            state
                .add_tasks_for_job(task.job_id.clone(), reduce_tasks)
                .chain_err(|| "Error processing completed task results.")?;
        }
        Ok(())
    }

    /// Schedule a [`Task`](common::Task) to be executed.
    fn schedule_task(&self, task: Task) {
        let worker_manager = Arc::clone(&self.worker_manager);
        thread::spawn(move || { worker_manager.run_task(task); });
    }

    /// Schedule a [`Job`](common::Job) to be executed.
    /// This function creates the map tasks before returning.
    pub fn schedule_job(&self, mut job: Job) -> Result<()> {
        let map_tasks_vec = self.task_processor.create_map_tasks(&job).chain_err(
            || "Error creating map tasks for job.",
        )?;

        job.map_tasks_total = map_tasks_vec.len() as u32;

        let mut map_tasks: HashMap<String, Task> = HashMap::new();
        for task in map_tasks_vec {
            self.schedule_task(task.clone());
            map_tasks.insert(task.id.to_owned(), task);
        }

        info!("Starting job with ID {}.", job.id);
        job.status = JobStatus::IN_PROGRESS;
        job.time_started = Some(Utc::now());

        let scheduled_job = ScheduledJob {
            job: job,
            tasks: map_tasks,
        };

        let mut state = self.state.lock().unwrap();
        state.add_job(scheduled_job).chain_err(
            || "Error adding scheduled job to state store",
        )
    }

    // TODO(conor): Finish these functions
    pub fn get_job_queue_size(&self) -> usize {
        0
    }

    pub fn get_available_workers(&self) -> u32 {
        0
    }

    pub fn get_mapreduce_status(&self, mapreduce_id: &str) -> Result<&Job> {
        Err("There was an error getting the result".into())
    }

    /// `get_mapreduce_client_status` returns a vector of `Job`s for a given client.
    pub fn get_mapreduce_client_status(&self, client_id: &str) -> Result<Vec<&Job>> {
        Ok(Vec::new())
    }
}

pub fn run_task_result_loop(scheduler: Arc<Scheduler>, worker_manager: Arc<WorkerManager>) {
    let reciever = worker_manager.get_result_reciever();
    thread::spawn(move || loop {
        let receiver = reciever.lock().unwrap();
        match receiver.recv() {
            Err(e) => error!("Error processing task results: {}", e),
            Ok(task) => {
                let result = scheduler.process_completed_task(task);
                if let Err(e) = result {
                    output_error(&e);
                }
            }
        }
    });
}
