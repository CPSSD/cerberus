use std::collections::HashMap;

use chrono::prelude::*;
use serde_json;

use cerberus_proto::mapreduce as pb;
use common::{Task, TaskType, TaskStatus, Job};
use errors::*;
use util::state::StateHandling;

/// A `ScheduledJob` holds the information about a scheduled `Job` and the `Task`s that relate to
/// that job.
pub struct ScheduledJob {
    pub job: Job,
    pub tasks: HashMap<String, Task>,
}

impl ScheduledJob {
    fn process_json(data: &serde_json::Value) -> Result<(Job, HashMap<String, Task>)> {
        let job = Job::new_from_json(data["job"].clone()).chain_err(
            || "Unable to create map reduce job from json.",
        )?;

        let mut tasks = HashMap::new();

        if let serde_json::Value::Array(ref tasks_array) = data["tasks"] {
            for task in tasks_array {
                let task = Task::new_from_json(task.clone()).chain_err(
                    || "Unable to create map reduce task from json.",
                )?;
                tasks.insert(task.id.to_owned(), task);
            }
        }

        Ok((job, tasks))
    }
}

impl StateHandling<Error> for ScheduledJob {
    fn new_from_json(data: serde_json::Value) -> Result<Self> {
        let (job, tasks) = ScheduledJob::process_json(&data)?;

        Ok(ScheduledJob { job, tasks })
    }

    fn dump_state(&self) -> Result<serde_json::Value> {
        let mut tasks_json: Vec<serde_json::Value> = Vec::new();
        for task in self.tasks.values() {
            tasks_json.push(task.dump_state().chain_err(
                || "Error dumping scheduled job state.",
            )?);
        }

        Ok(json!({
            "job": self.job.dump_state().chain_err(|| "Error dumping scheduled job state.")?,
            "tasks": json!(tasks_json),
        }))
    }

    fn load_state(&mut self, data: serde_json::Value) -> Result<()> {
        let (job, tasks) = ScheduledJob::process_json(&data)?;

        self.job = job;
        self.tasks = tasks;

        Ok(())
    }
}

/// The `State` object holds all of the `ScheduledJob`s inside a `HashMap`, with the job IDs as
/// keys.
#[derive(Default)]
pub struct State {
    scheduled_jobs: HashMap<String, ScheduledJob>,
}

impl State {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn add_job(&mut self, scheduled_job: ScheduledJob) -> Result<()> {
        if self.scheduled_jobs.contains_key(&scheduled_job.job.id) {
            return Err(
                format!(
                    "Job with ID {} is already scheduled.",
                    &scheduled_job.job.id
                ).into(),
            );
        }

        self.scheduled_jobs.insert(
            scheduled_job.job.id.to_owned(),
            scheduled_job,
        );
        Ok(())
    }

    pub fn update_job_started(&mut self, job_id: &str, time_started: DateTime<Utc>) -> Result<()> {
        let scheduled_job = match self.scheduled_jobs.get_mut(job_id) {
            Some(scheduled_job) => scheduled_job,
            None => return Err(format!("Job with ID {} is not found.", &job_id).into()),
        };

        if scheduled_job.job.status == pb::Status::IN_QUEUE {
            scheduled_job.job.status = pb::Status::IN_PROGRESS;
        }

        if let Some(job_time_started) = scheduled_job.job.time_started {
            if time_started < job_time_started {
                scheduled_job.job.time_started = Some(time_started);
            }
        } else {
            scheduled_job.job.time_started = Some(time_started);
        }

        Ok(())
    }

    pub fn cancel_job(&mut self, job_id: &str) -> Result<bool> {
        let scheduled_job = match self.scheduled_jobs.get_mut(job_id) {
            Some(job) => job,
            None => return Err(format!("Job with ID {} was not found.", &job_id).into()),
        };

        if scheduled_job.job.status != pb::Status::FAILED &&
            scheduled_job.job.status != pb::Status::DONE
        {
            scheduled_job.job.status = pb::Status::CANCELLED;

            // Cancel each of the tasks for this job.
            for task in scheduled_job.tasks.values_mut() {
                task.status = TaskStatus::Cancelled;
            }
        }
        Ok(scheduled_job.job.status == pb::Status::CANCELLED)
    }

    pub fn get_job(&self, job_id: &str) -> Result<&Job> {
        match self.scheduled_jobs.get(job_id) {
            Some(scheduled_job) => Ok(&scheduled_job.job),
            None => Err(format!("Job with ID {} is not found.", &job_id).into()),
        }
    }

    pub fn get_all_jobs(&self) -> Vec<&Job> {
        let mut jobs = Vec::new();
        for scheduled_job in self.scheduled_jobs.values() {
            jobs.push(&scheduled_job.job);
        }

        jobs
    }

    pub fn get_jobs(&self, client_id: &str) -> Vec<&Job> {
        let mut jobs = Vec::new();
        for scheduled_job in self.scheduled_jobs.values() {
            if scheduled_job.job.client_id == client_id {
                jobs.push(&scheduled_job.job);
            }
        }

        jobs
    }

    pub fn get_map_tasks(&self, job_id: &str) -> Result<Vec<&Task>> {
        let scheduled_job = match self.scheduled_jobs.get(job_id) {
            Some(scheduled_job) => scheduled_job,
            None => return Err(format!("Job with ID {} is not found.", &job_id).into()),
        };

        let mut map_tasks = Vec::new();
        for task in scheduled_job.tasks.values() {
            if task.task_type == TaskType::Map {
                map_tasks.push(task);
            }
        }

        Ok(map_tasks)
    }

    pub fn get_in_progress_tasks(&self) -> Vec<&Task> {
        let mut in_progress_tasks = Vec::new();
        for scheduled_job in self.scheduled_jobs.values() {
            for task in scheduled_job.tasks.values() {
                if task.status == TaskStatus::InProgress || task.status == TaskStatus::Queued {
                    in_progress_tasks.push(task);
                }
            }
        }

        in_progress_tasks
    }

    // Adds a Vector of newly created tasks for a given job.
    pub fn add_tasks_for_job(&mut self, job_id: &str, tasks: Vec<Task>) -> Result<()> {
        let scheduled_job = match self.scheduled_jobs.get_mut(job_id) {
            Some(scheduled_job) => scheduled_job,
            None => return Err(format!("Job with ID {} is not found.", job_id).into()),
        };

        for task in tasks {
            if scheduled_job.tasks.contains_key(&task.id) {
                return Err(
                    format!("Task with ID {} is already scheduled.", &task.id).into(),
                );
            }

            match task.task_type {
                TaskType::Map => scheduled_job.job.map_tasks_total += 1,
                TaskType::Reduce => scheduled_job.job.reduce_tasks_total += 1,
            }
            scheduled_job.tasks.insert(task.id.to_owned(), task);
        }

        Ok(())
    }

    pub fn update_task(&mut self, task: Task) -> Result<()> {
        let scheduled_job = match self.scheduled_jobs.get_mut(&task.job_id) {
            Some(scheduled_job) => scheduled_job,
            None => return Err(format!("Job with ID {} is not found.", task.job_id).into()),
        };

        if !scheduled_job.tasks.contains_key(&task.id) {
            return Err(
                format!("Task with ID {} is does not exist.", &task.id).into(),
            );
        }

        scheduled_job.tasks.insert(task.id.to_owned(), task);

        Ok(())
    }

    // Returns if reduce tasks are required for a given job.
    pub fn reduce_tasks_required(&self, job_id: &str) -> Result<bool> {
        let scheduled_job = match self.scheduled_jobs.get(job_id) {
            Some(scheduled_job) => scheduled_job,
            None => return Err(format!("Job with ID {} is not found.", &job_id).into()),
        };

        if scheduled_job.job.map_tasks_completed == scheduled_job.job.map_tasks_total {
            return Ok(scheduled_job.job.reduce_tasks_total == 0);
        }
        Ok(false)
    }

    // Adds the information for a completed task and updates the job.
    pub fn add_completed_task(&mut self, task: Task) -> Result<()> {
        self.update_job_started(
            &task.job_id,
            task.time_started.chain_err(
                || "Time started is expected to exist.",
            )?,
        ).chain_err(|| "Error adding completed task.")?;

        let scheduled_job = match self.scheduled_jobs.get_mut(&task.job_id) {
            Some(scheduled_job) => scheduled_job,
            None => return Err(format!("Job with ID {} is not found.", &task.job_id).into()),
        };

        if task.has_completed_before && task.task_type == TaskType::Map {
            scheduled_job.job.reduce_tasks_total = 0;
            scheduled_job.job.reduce_tasks_completed = 0;
        }

        scheduled_job.job.cpu_time += task.cpu_time;
        if task.status == TaskStatus::Complete {
            match task.task_type {
                TaskType::Map => {
                    if !task.has_completed_before {
                        scheduled_job.job.map_tasks_completed += 1;
                    }
                }
                TaskType::Reduce => {
                    scheduled_job.job.reduce_tasks_completed += 1;
                    if scheduled_job.job.reduce_tasks_completed ==
                        scheduled_job.job.reduce_tasks_total
                    {
                        scheduled_job.job.status = pb::Status::DONE;
                        scheduled_job.job.time_completed = Some(Utc::now());
                    }
                }
            }
        } else {
            scheduled_job.job.status = pb::Status::FAILED;
            if let Some(task_failure_reason) = task.failure_details.clone() {
                scheduled_job.job.status_details = Some(task_failure_reason);
            }
        }

        scheduled_job.tasks.insert(task.id.to_owned(), task);

        Ok(())
    }

    pub fn get_job_queue_size(&self) -> u32 {
        let mut job_count = 0;
        for scheduled_job in self.scheduled_jobs.values() {
            if scheduled_job.job.status == pb::Status::IN_PROGRESS ||
                scheduled_job.job.status == pb::Status::IN_QUEUE
            {
                job_count += 1;
            }
        }
        job_count
    }

    pub fn set_job_completed(&mut self, job_id: &str) -> Result<()> {
        let scheduled_job = match self.scheduled_jobs.get_mut(job_id) {
            Some(scheduled_job) => scheduled_job,
            None => return Err(format!("Job with ID {} is not found.", job_id).into()),
        };

        scheduled_job.job.status = pb::Status::DONE;
        scheduled_job.job.time_completed = Some(Utc::now());

        Ok(())
    }
}

impl StateHandling<Error> for State {
    fn new_from_json(_: serde_json::Value) -> Result<Self> {
        Err("Unable to create Scheduler State from JSON.".into())
    }

    fn dump_state(&self) -> Result<serde_json::Value> {
        let mut jobs_json: Vec<serde_json::Value> = Vec::new();
        for job in self.scheduled_jobs.values() {
            jobs_json.push(job.dump_state().chain_err(
                || "Unable to dump ScheduledJob state",
            )?);
        }

        Ok(json!({
            "scheduled_jobs": json!(jobs_json),
        }))
    }

    fn load_state(&mut self, data: serde_json::Value) -> Result<()> {
        if let serde_json::Value::Array(ref jobs_array) = data["scheduled_jobs"] {
            for job in jobs_array {
                let scheduled_job = ScheduledJob::new_from_json(job.clone()).chain_err(
                    || "Unable to create ScheduledJob from json.",
                )?;
                self.scheduled_jobs.insert(
                    scheduled_job.job.id.to_owned(),
                    scheduled_job,
                );
            }
        } else {
            return Err("Error processing scheduled_jobs array.".into());
        }
        Ok(())
    }
}
