use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use std::sync::Arc;

use iron;
use iron::prelude::*;
use mount::Mount;
use router::Router;
use staticfile::Static;
use urlencoded::UrlEncodedQuery;

use common::{Job, JobOptions};
use dashboard::fetch_logs::fetch_worker_log;
use errors::*;
use scheduling::Scheduler;
use util::data_layer::AbstractionLayer;
use util::output_error;
use worker_management::WorkerManager;

// Default priority applied to scheduled jobs.
const DEFAULT_PRIORITY: u32 = 3;
const DEFAULT_MAP_SIZE: u32 = 64;

fn read_local_file<P: AsRef<Path>>(path: P) -> Result<String> {
    debug!("Attempting to read local file: {:?}", path.as_ref());
    let file = File::open(&path)
        .chain_err(|| format!("unable to open file {}", path.as_ref().to_string_lossy()))?;

    let mut buf_reader = BufReader::new(file);
    let mut value = String::new();
    buf_reader.read_to_string(&mut value).chain_err(|| {
        format!(
            "unable to read content of {}",
            path.as_ref().to_string_lossy()
        )
    })?;

    Ok(value)
}

#[derive(Clone)]
struct ApiHandler {
    scheduler_arc: Arc<Scheduler>,
    worker_manager_arc: Arc<WorkerManager>,
    data_abstraction_layer_arc: Arc<AbstractionLayer + Send + Sync>,
    log_file_path: String,
}

impl ApiHandler {
    fn get_query_param(&self, key: &str, hashmap: &HashMap<String, Vec<String>>) -> Result<String> {
        match hashmap.get(key) {
            Some(params_vec) => {
                if params_vec.len() != 1 {
                    Err(format!("Expected one value for query param {}", key).into())
                } else {
                    let value = params_vec.first().chain_err(|| {
                        format!("Could not get first element in params vector for {}", key)
                    })?;

                    Ok(value.to_string())
                }
            }
            None => Err("No binary_path in request".into()),
        }
    }

    fn get_parameter(&self, req: &mut Request, param_name: &str) -> Result<String> {
        match req.get_ref::<UrlEncodedQuery>() {
            Ok(hashmap) => {
                let param = self.get_query_param(param_name, hashmap)
                    .chain_err(|| format!("Failed to get param with name {} ", param_name))?;

                Ok(param)
            }
            Err(ref e) => Err(format!("Could not get query params, error: {:?}", e).into()),
        }
    }

    fn get_router_url_component(&self, req: &Request, component_name: &str) -> Result<String> {
        let value: String = {
            match req.extensions.get::<Router>() {
                Some(router_extension) => match router_extension.find(component_name) {
                    Some(value) => value.to_string(),
                    None => {
                        return Err(format!("No router value with name {}", component_name).into());
                    }
                },
                None => {
                    return Err("Failed to get Router for request".into());
                }
            }
        };

        Ok(value)
    }

    /// Returns information about the `Tasks` which are currently in progress.
    fn tasks(&self, _req: &mut Request) -> Result<Response> {
        let tasks_info = self.worker_manager_arc
            .get_tasks_info()
            .chain_err(|| "Failed to get tasks info")?;

        Ok(Response::with((iron::status::Ok, tasks_info.to_string())))
    }

    /// Returns information about the `Workers` currently registered with the cluster.
    fn workers(&self, _req: &mut Request) -> Result<Response> {
        let workers_info = self.worker_manager_arc
            .get_workers_info()
            .chain_err(|| "Failed to get workers info")?;

        Ok(Response::with((iron::status::Ok, workers_info.to_string())))
    }

    /// Returns information about the `Jobs` currently running on the cluster.
    fn jobs(&self, _req: &mut Request) -> Result<Response> {
        let jobs_info = self.scheduler_arc
            .get_jobs_info()
            .chain_err(|| "Failed to get jobs info")?;

        Ok(Response::with((iron::status::Ok, jobs_info.to_string())))
    }

    fn cancel_job(&self, req: &mut Request) -> Result<Response> {
        let job_id = self.get_parameter(req, "job_id")
            .chain_err(|| "Could not get job_id in request")?;

        let success = self.scheduler_arc
            .cancel_job(&job_id)
            .chain_err(|| format!("Failed to cancel job with id {}", job_id))?;

        Ok(Response::with((
            iron::status::Ok,
            format!("{{ job_id: {}, success: {} }}", job_id, success),
        )))
    }

    fn get_output_path(&self, req: &mut Request) -> Option<String> {
        let output_path = self.get_parameter(req, "output_path")
            .unwrap_or_else(|_| "".to_string());
        if output_path.is_empty() {
            None
        } else {
            Some(output_path)
        }
    }

    fn get_priority(&self, req: &mut Request) -> Result<u32> {
        let priority = self.get_parameter(req, "priority")
            .unwrap_or_else(|_| "".to_string());
        if priority.is_empty() {
            Ok(DEFAULT_PRIORITY)
        } else {
            priority
                .parse::<u32>()
                .chain_err(|| "Invalid priority when scheduling job")
        }
    }

    fn get_map_size(&self, req: &mut Request) -> Result<u32> {
        let map_size = self.get_parameter(req, "map_size")
            .unwrap_or_else(|_| "".to_string());
        let map_size = {
            if map_size.is_empty() {
                DEFAULT_MAP_SIZE
            } else {
                map_size
                    .parse::<u32>()
                    .chain_err(|| "Invalid map size when scheduling job")?
            }
        };
        if map_size < 1 {
            return Err("Map size must be greater than or equal to 1".into());
        }
        Ok(map_size)
    }

    fn schedule_job(&self, req: &mut Request) -> Result<Response> {
        let binary_path = self.get_parameter(req, "binary_path")
            .chain_err(|| "Failed to get binary_path")?;
        let input_path = self.get_parameter(req, "input_path")
            .chain_err(|| "Failed to get input_path")?;
        let priority = self.get_priority(req)
            .chain_err(|| "Failed to get priority")?;
        let map_size = self.get_map_size(req)
            .chain_err(|| "Failed to get map size")?;

        let job_options = JobOptions {
            client_id: req.remote_addr.to_string(),

            binary_path,
            input_directory: input_path,
            output_directory: self.get_output_path(req),
            validate_paths: true,

            priority,
            map_size,
        };

        let job = Job::new(job_options, &self.data_abstraction_layer_arc)
            .chain_err(|| "Error creating new job")?;

        self.scheduler_arc
            .schedule_job(job)
            .chain_err(|| "Error scheduling job")?;

        Ok(Response::with((iron::status::Ok, "{{ success: true }}")))
    }

    fn get_master_logs(&self, _req: &mut Request) -> Result<Response> {
        match read_local_file(&self.log_file_path) {
            Ok(log_file_contents) => Ok(Response::with((iron::status::Ok, log_file_contents))),
            Err(err) => Err(err.chain_err(|| "Unable to read master log file")),
        }
    }

    fn get_worker_logs(&self, req: &mut Request) -> Result<Response> {
        let worker_id = self.get_parameter(req, "worker_id")
            .chain_err(|| "Failed to get worker_id")?;

        let log_contents = fetch_worker_log(&worker_id, &self.worker_manager_arc)
            .chain_err(|| format!("Failed to get log for worker with id {}", worker_id))?;

        Ok(Response::with((iron::status::Ok, log_contents)))
    }

    fn handle_endpoint(&self, endpoint: &str, req: &mut Request) -> IronResult<Response> {
        let result = {
            match endpoint {
                "tasks" => self.tasks(req),
                "workers" => self.workers(req),
                "jobs" => self.jobs(req),
                "canceljob" => self.cancel_job(req),
                "schedule" => self.schedule_job(req),
                "logs" => self.get_master_logs(req),
                "workerlogs" => self.get_worker_logs(req),
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
        let endpoint_result = self.get_router_url_component(req, "endpoint");

        match endpoint_result {
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
        data_abstraction_layer_arc: Arc<AbstractionLayer + Send + Sync>,
        log_file_path: String,
    ) -> Result<Self> {
        let handler = ApiHandler {
            scheduler_arc,
            worker_manager_arc,
            data_abstraction_layer_arc,
            log_file_path,
        };

        let mut router = Router::new();
        router.get("/:endpoint", handler.clone(), "api");
        router.get("/:endpoint/:query", handler, "api_query");

        let mut mount = Mount::new();

        mount
            .mount("/api/", router)
            .mount("/dashboard", Static::new(Path::new("content/index.html")))
            .mount("/", Static::new(Path::new("content/")));

        Ok(DashboardServer {
            iron_server: Iron::new(mount)
                .http(serving_addr)
                .chain_err(|| "Failed to start cluster dashboard server.")?,
        })
    }
}
