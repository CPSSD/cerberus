use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;

use serde_json;

use cerberus_proto::mapreduce as pb;
use common::{Job, Task, TaskStatus};
use errors::*;
use scheduling::state::{ScheduledJob, State};
use scheduling::task_processor::TaskProcessor;
use util::output_error;
use util::state::{SimpleStateHandling, StateHandling};
use worker_management::WorkerManager;

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
            state,

            worker_manager,
            task_processor,
        }
    }

    fn schedule_reduce_tasks(&self, job_id: &str) -> Result<()> {
        info!("Creating reduce tasks for job {}", job_id);

        let mut state = self.state.lock().unwrap();

        let reduce_tasks = {
            let job = state
                .get_job(job_id)
                .chain_err(|| "Error scheduling reduce tasks")?;

            let map_tasks = state
                .get_map_tasks(job_id)
                .chain_err(|| format!("Could not get map tasks for job {}", job_id))?;

            self.task_processor
                .create_reduce_tasks(job, map_tasks)
                .chain_err(|| format!("Could not create reduce tasks for job {}", job_id))?
        };

        if reduce_tasks.is_empty() {
            state
                .set_job_completed(job_id)
                .chain_err(|| format!("Could not set job with id {} completed", job_id))?;
        } else {
            state
                .add_tasks_for_job(job_id, reduce_tasks.clone())
                .chain_err(|| format!("Could not add reduce tasks to job {}", job_id))?;

            for task in reduce_tasks {
                self.schedule_task(task);
            }
        }

        Ok(())
    }

    fn process_completed_task(&self, task: &Task) -> Result<()> {
        info!(
            "Processing completed task {} with status {:?}",
            task.id, task.status
        );

        let reduce_tasks_required = {
            let mut state = self.state.lock().unwrap();

            state
                .add_completed_task(task.clone())
                .chain_err(|| "Error processing completed task result")?;

            state
                .reduce_tasks_required(&task.job_id)
                .chain_err(|| "Error processing completed task result")?
        };

        if task.status != TaskStatus::Complete {
            self.cancel_job(&task.job_id)
                .chain_err(|| format!("Unable to cancel job with ID {}", &task.job_id))?;
            return Ok(());
        }

        if reduce_tasks_required {
            self.schedule_reduce_tasks(&task.job_id)
                .chain_err(|| format!("Could not schedule reduce tasks for job {}", task.job_id))?;
        }
        Ok(())
    }

    fn process_in_progress_task(&self, task: &Task) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state
            .update_job_started(
                &task.job_id,
                task.time_started
                    .chain_err(|| "Time started expected to exist.")?,
            )
            .chain_err(|| "Error updating job start time.")?;

        state
            .update_task(task.to_owned())
            .chain_err(|| "Error updating task info.")
    }

    pub fn process_updated_task(&self, task: &Task) -> Result<()> {
        if task.status == TaskStatus::Complete || task.status == TaskStatus::Failed {
            self.process_completed_task(task)
        } else {
            self.process_in_progress_task(task)
        }
    }

    /// Schedule a [`Task`](common::Task) to be executed.
    fn schedule_task(&self, task: Task) {
        let worker_manager = Arc::clone(&self.worker_manager);
        thread::spawn(move || {
            worker_manager.run_task(task);
        });
    }

    /// Splits the input for a job and schedules the map tasks in the background.
    pub fn split_input(&self, job: Job) {
        let state = Arc::clone(&self.state);
        let worker_manager = Arc::clone(&self.worker_manager);
        let task_processor = Arc::clone(&self.task_processor);

        thread::spawn(move || {
            info!("Splitting input for job with ID {}.", job.id);

            let map_tasks_vec = match task_processor.create_map_tasks(&job) {
                Ok(tasks) => tasks,
                Err(err) => {
                    output_error(&err.chain_err(|| "Error creating map tasks for job."));
                    return;
                }
            };

            let mut map_tasks: HashMap<String, Task> = HashMap::new();
            for task in map_tasks_vec {
                map_tasks.insert(task.id.to_owned(), task.clone());
                let worker_manager = Arc::clone(&worker_manager);
                thread::spawn(move || {
                    worker_manager.run_task(task);
                });
            }

            info!("Starting job with ID {}.", job.id);

            let mut state = state.lock().unwrap();
            let result = state.input_splitting_complete(&job.id, map_tasks);
            if let Err(err) = result {
                output_error(&err.chain_err(|| "Error creating map tasks for job."));
            }
        });
    }

    /// Schedule a [`Job`](common::Job) to be executed.
    pub fn schedule_job(&self, job: Job) -> Result<()> {
        let scheduled_job = ScheduledJob {
            job: job.clone(),
            tasks: Default::default(),
        };

        {
            let mut state = self.state.lock().unwrap();
            state
                .add_job(scheduled_job)
                .chain_err(|| "Error adding scheduled job to state store")?;
        }

        self.split_input(job);
        Ok(())
    }

    pub fn cancel_job(&self, job_id: &str) -> Result<bool> {
        let cancelled = {
            let mut state = self.state.lock().unwrap();
            state
                .cancel_job(job_id)
                .chain_err(|| format!("Unable to cancel job with ID: {}", job_id))?
        };
        if !cancelled {
            info!("Unable to cancel job with ID {}", job_id);
            return Ok(false);
        }

        let workers = self.worker_manager
            .get_workers_running_job(job_id)
            .chain_err(|| format!("Unable to get list of workers running job {}", job_id))?;

        self.worker_manager
            .remove_queued_tasks_for_job(job_id)
            .chain_err(|| "Unable to remove queued task from state")?;

        self.worker_manager
            .cancel_workers_tasks(workers)
            .chain_err(|| "Unable to cancel task on workers")?;

        info!("Succesfully cancelled job {}", job_id);
        Ok(cancelled)
    }

    pub fn get_job_queue_size(&self) -> u32 {
        let state = self.state.lock().unwrap();
        state.get_job_queue_size()
    }

    // Returns a count of workers registered with the master.
    pub fn get_available_workers(&self) -> u32 {
        self.worker_manager.get_available_workers()
    }

    fn get_status_for_job(&self, job: &Job) -> pb::MapReduceReport {
        let mut report = pb::MapReduceReport::new();
        report.mapreduce_id = job.id.clone();
        report.status = job.status;
        if job.status == pb::Status::FAILED {
            report.failure_details = job.status_details
                .clone()
                .unwrap_or_else(|| "Unknown.".to_owned());
        }
        report.scheduled_timestamp = job.time_requested.timestamp();
        report.output_directory = job.output_directory.clone();
        if let Some(time) = job.time_started {
            report.started_timestamp = time.timestamp();
        }
        if let Some(time) = job.time_completed {
            report.done_timestamp = time.timestamp();
        }

        report
    }

    pub fn get_mapreduce_status(&self, mapreduce_id: &str) -> Result<pb::MapReduceReport> {
        let state = self.state.lock().unwrap();
        let job = state
            .get_job(mapreduce_id)
            .chain_err(|| "Error getting map reduce status.")?;
        Ok(self.get_status_for_job(job))
    }

    /// `get_mapreduce_client_status` returns a vector of `MapReduceReport`s for a given client.
    pub fn get_mapreduce_client_status(&self, client_id: &str) -> Vec<pb::MapReduceReport> {
        let state = self.state.lock().unwrap();
        let jobs = state.get_jobs(client_id);

        let mut reports = Vec::new();
        for job in jobs {
            reports.push(self.get_status_for_job(job));
        }
        reports
    }

    pub fn get_most_recent_client_job_id(&self, client_id: &str) -> Result<String> {
        let state = self.state.lock().unwrap();
        let jobs = state.get_jobs(client_id);

        if jobs.is_empty() {
            return Err("No jobs queued for client".into());
        }

        let mut latest_job = &jobs[0];
        for job in jobs.iter().skip(1) {
            if job.time_requested > latest_job.time_requested {
                latest_job = job;
            }
        }

        Ok(latest_job.id.clone())
    }

    pub fn get_jobs_info(&self) -> Result<serde_json::Value> {
        let state = self.state.lock().unwrap();

        let mut results_vec = Vec::new();
        for job in state.get_all_jobs() {
            let job_info = json!({
                "job_id": job.id,
                "client_id": job.client_id,
                "binary_path": job.binary_path,
                "input_directory": job.input_directory,
                "output_directory": job.output_directory,
                "priority": job.priority,
                "status": job.get_serializable_status(),
                "map_tasks_completed": job.map_tasks_completed,
                "map_tasks_total": job.map_tasks_total,
                "reduce_tasks_completed": job.reduce_tasks_completed,
                "reduce_tasks_total": job.reduce_tasks_total,
            });

            results_vec.push(job_info);
        }

        Ok(json!(results_vec))
    }
}

impl SimpleStateHandling<Error> for Scheduler {
    fn dump_state(&self) -> Result<serde_json::Value> {
        let state = self.state.lock().unwrap();
        state.dump_state()
    }

    fn load_state(&self, data: serde_json::Value) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state
            .load_state(data)
            .chain_err(|| "Error loading scheduler state.")?;

        let in_progress_tasks = state.get_in_progress_tasks();
        for task in in_progress_tasks {
            if !self.worker_manager.has_task(&task.id) {
                self.schedule_task(task.clone());
            }
        }

        let splitting_jobs = state.get_splitting_jobs();
        for job in splitting_jobs {
            self.split_input(job);
        }

        Ok(())
    }
}

pub fn run_task_update_loop(scheduler: Arc<Scheduler>, worker_manager: &Arc<WorkerManager>) {
    let receiver = worker_manager.get_update_receiver();
    thread::spawn(move || loop {
        let receiver = receiver.lock().unwrap();
        match receiver.recv() {
            Err(e) => error!("Error processing task results: {}", e),
            Ok(task) => {
                let result = scheduler.process_updated_task(&task);
                if let Err(e) = result {
                    output_error(&e);
                }
            }
        }
    });
}
