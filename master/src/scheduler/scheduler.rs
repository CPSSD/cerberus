use std::thread;
use std::collections::HashMap;
use std::sync::{Mutex, Arc};

use chrono::Utc;
use serde_json;

use cerberus_proto::mapreduce as pb;
use common::{Task, Job};
use errors::*;
use scheduler::state::{ScheduledJob, State};
use scheduler::task_processor::TaskProcessor;
use state;
use state::StateHandling;
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

    fn process_completed_task(&self, task: &Task) -> Result<()> {
        info!(
            "Processing completed task {} with status {:?}",
            task.id,
            task.status
        );

        let mut state = self.state.lock().unwrap();

        state.add_completed_task(task.clone()).chain_err(
            || "Error processing completed task result",
        )?;

        let reduce_tasks_required = state.reduce_tasks_required(&task.job_id).chain_err(
            || "Error processing completed task result",
        )?;

        if reduce_tasks_required {
            info!("Creating reduce tasks for job {}", task.job_id);

            let job = state.get_job(&task.job_id).chain_err(
                || "Error processing completed task result.",
            )?;

            let reduce_tasks = {
                let map_tasks = state.get_map_tasks(&task.job_id).chain_err(
                    || "Error processing completed task result.",
                )?;

                self.task_processor
                    .create_reduce_tasks(&job, map_tasks)
                    .chain_err(|| "Error processing completed task results.")?
            };

            state
                .add_tasks_for_job(&task.job_id, reduce_tasks.clone())
                .chain_err(|| "Error processing completed task results.")?;

            for task in reduce_tasks {
                self.schedule_task(task);
            }
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
        job.status = pb::Status::IN_PROGRESS;
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

    pub fn get_job_queue_size(&self) -> u32 {
        let state = self.state.lock().unwrap();
        state.get_in_progress_job_count()
    }

    // Returns a count of workers registered with the master.
    pub fn get_available_workers(&self) -> u32 {
        self.worker_manager.get_available_workers()
    }

    /// `modify_job_status` modifies a job status to IN_QUEUE if no progress has been made on it
    /// yet.
    fn modify_job_status(&self, job: &mut Job) {
        if job.status == pb::Status::IN_PROGRESS && job.map_tasks_completed == 0 &&
            !self.worker_manager.is_job_in_progress(&job.id)
        {
            // If the job is not in progress, it's status should be queued.
            // This check is needed because the scheduler activates all jobs immediately.
            job.status = pb::Status::IN_QUEUE;
        }
    }

    pub fn get_mapreduce_status(&self, mapreduce_id: &str) -> Result<Job> {
        let state = self.state.lock().unwrap();
        let mut job = state.get_job(mapreduce_id).chain_err(
            || "Error getting map reduce status.",
        )?;

        self.modify_job_status(&mut job);

        Ok(job)
    }

    /// `get_mapreduce_client_status` returns a vector of `Job`s for a given client.
    pub fn get_mapreduce_client_status(&self, client_id: &str) -> Vec<Job> {
        let state = self.state.lock().unwrap();
        let mut jobs = state.get_jobs(client_id);

        for mut job in &mut jobs {
            self.modify_job_status(&mut job);
        }

        jobs
    }
}

impl state::SimpleStateHandling for Scheduler {
    fn dump_state(&self) -> Result<serde_json::Value> {
        let state = self.state.lock().unwrap();
        state.dump_state()
    }

    fn load_state(&self, data: serde_json::Value) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state.load_state(data).chain_err(
            || "Error loading scheduler state.",
        )?;

        let in_progress_tasks = state.get_in_progress_tasks();
        for task in in_progress_tasks {
            if !self.worker_manager.has_task(&task.id) {
                self.schedule_task(task.clone());
            }
        }

        Ok(())
    }
}


pub fn run_task_result_loop(scheduler: Arc<Scheduler>, worker_manager: &Arc<WorkerManager>) {
    let reciever = worker_manager.get_result_reciever();
    thread::spawn(move || loop {
        let receiver = reciever.lock().unwrap();
        match receiver.recv() {
            Err(e) => error!("Error processing task results: {}", e),
            Ok(task) => {
                let result = scheduler.process_completed_task(&task);
                if let Err(e) = result {
                    output_error(&e);
                }
            }
        }
    });
}
