use std::collections::HashMap;

use chrono::prelude::*;
use cerberus_proto::mapreduce as pb;
use common::{Task, TaskType, TaskStatus, Job};
use errors::*;

/// A `ScheduledJob` holds the information about a scheduled `Job` and the `Task`s that relate to
/// that job.
pub struct ScheduledJob {
    pub job: Job,
    pub tasks: HashMap<String, Task>,
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


    pub fn get_job(&self, job_id: &str) -> Result<Job> {
        match self.scheduled_jobs.get(job_id) {
            Some(scheduled_job) => Ok(scheduled_job.job.clone()),
            None => Err(format!("Job with ID {} is not found.", &job_id).into()),
        }
    }

    pub fn get_jobs(&self, client_id: &str) -> Vec<Job> {
        let mut jobs = Vec::new();
        for scheduled_job in self.scheduled_jobs.values() {
            if scheduled_job.job.client_id == client_id {
                jobs.push(scheduled_job.job.clone());
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
        let scheduled_job = match self.scheduled_jobs.get_mut(&task.job_id) {
            Some(scheduled_job) => scheduled_job,
            None => return Err(format!("Job with ID {} is not found.", &task.job_id).into()),
        };

        scheduled_job.job.cpu_time += task.cpu_time;
        if task.status == TaskStatus::Complete {
            match task.task_type {
                TaskType::Map => {
                    scheduled_job.job.map_tasks_completed += 1;
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

    pub fn get_in_progress_job_count(&self) -> u32 {
        let mut job_count = 0;
        for scheduled_job in self.scheduled_jobs.values() {
            if scheduled_job.job.status == pb::Status::IN_PROGRESS {
                job_count += 1;
            }
        }
        job_count
    }
}
