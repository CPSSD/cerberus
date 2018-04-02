use std::path::Path;
use std::sync::Arc;

use iron;
use iron::prelude::*;
use mount::Mount;
use router::Router;
use staticfile::Static;

use errors::*;
use scheduling::Scheduler;
use worker_management::WorkerManager;
use util::output_error;

#[derive(Clone)]
struct ApiHandler {
    scheduler_arc: Arc<Scheduler>,
    worker_manager_arc: Arc<WorkerManager>,
}

impl ApiHandler {
    fn get_parameter(&self, req: &Request, param: &str) -> Result<String> {
        let value: String = {
            match req.extensions.get::<Router>() {
                Some(params) => {
                    match params.find(param) {
                        Some(value) => value.to_string(),
                        None => {
                            return Err(format!("No param found with name {}", param).into());
                        }
                    }
                }
                None => {
                    return Err("Failed to get Router for request".into());
                }
            }
        };

        Ok(value)
    }

    /// Returns information about the `Tasks` which are currently in progress.
    fn tasks(&self, _req: &mut Request) -> Result<Response> {
        let tasks_info = self.worker_manager_arc.get_tasks_info().chain_err(
            || "Failed to get tasks info",
        )?;

        Ok(Response::with((iron::status::Ok, tasks_info.to_string())))
    }

    /// Returns information about the `Workers` currently registered with the cluster.
    fn workers(&self, _req: &mut Request) -> Result<Response> {
        let workers_info = self.worker_manager_arc.get_workers_info().chain_err(
            || "Failed to get workers info",
        )?;

        Ok(Response::with((iron::status::Ok, workers_info.to_string())))
    }

    /// Returns information about the `Jobs` currently running on the cluster.
    fn jobs(&self, _req: &mut Request) -> Result<Response> {
        let jobs_info = self.scheduler_arc.get_jobs_info().chain_err(
            || "Failed to get jobs info",
        )?;

        Ok(Response::with((iron::status::Ok, jobs_info.to_string())))
    }

    fn cancel_job(&self, req: &mut Request) -> Result<Response> {
        let job_id = self.get_parameter(req, "param").chain_err(
            || "Could not get job_id in request",
        )?;

        self.scheduler_arc.cancel_job(&job_id).chain_err(|| {
            format!("Failed to cancel job with id {}", job_id)
        })?;

        Ok(Response::with((iron::status::Ok, job_id)))
    }

    fn handle_endpoint(&self, endpoint: &str, req: &mut Request) -> IronResult<Response> {
        let result = {
            match endpoint {
                "tasks" => self.tasks(req),
                "workers" => self.workers(req),
                "jobs" => self.jobs(req),
                "canceljob" => self.cancel_job(req),
                _ => Err("Invalid endpoint".into()),
            }
        };

        match result {
            Ok(response) => Ok(response),
            Err(err) => {
                let chained_err = err.chain_err(|| "Error serving Cluster Dashboard request");
                output_error(&chained_err);
                Err(IronError::new(chained_err, iron::status::BadRequest))
            }
        }
    }
}

impl iron::Handler for ApiHandler {
    fn handle(&self, req: &mut Request) -> IronResult<Response> {
        let param_result = self.get_parameter(req, "endpoint");

        match param_result {
            Ok(endpoint) => self.handle_endpoint(&endpoint, req),
            Err(err) => {
                let chained_err = err.chain_err(|| "Error parsing Cluster Dashboard request");
                output_error(&chained_err);
                Err(IronError::new(chained_err, iron::status::BadRequest))
            }
        }

    }
}

pub struct DashboardServer {
    pub iron_server: iron::Listening,
}

impl DashboardServer {
    pub fn new(
        serving_addr: &str,
        scheduler_arc: Arc<Scheduler>,
        worker_manager_arc: Arc<WorkerManager>,
    ) -> Result<Self> {
        let handler = ApiHandler {
            scheduler_arc: scheduler_arc,
            worker_manager_arc: worker_manager_arc,
        };

        let mut router = Router::new();
        router.get("/:endpoint", handler.clone(), "api");
        router.get("/:endpoint/:param", handler.clone(), "api_one_param");
        router.get("/:endpoint/*", handler, "api_query");

        let mut mount = Mount::new();

        mount
            .mount("/api/", router)
            .mount("/dashboard", Static::new(Path::new("content/index.html")))
            .mount("/", Static::new(Path::new("content/")));

        Ok(DashboardServer {
            iron_server: Iron::new(mount).http(serving_addr).chain_err(
                || "Failed to start cluster dashboard server.",
            )?,
        })
    }
}
