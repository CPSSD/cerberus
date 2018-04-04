use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures::future;
use futures::prelude::*;
use libc::_SC_CLK_TCK;
use procinfo::pid::stat_self;
use serde_json;
use uuid::Uuid;

use cerberus_proto::worker as pb;
use errors::*;
use master_interface::MasterInterface;
use util::data_layer::AbstractionLayer;
use super::map;
use super::reduce;
use super::state::OperationState;

pub type PartitionMap = HashMap<u64, HashMap<String, Vec<serde_json::Value>>>;

/// `OperationResources` is used to hold resources passed to map and reduce operation functions.
#[derive(Clone)]
pub struct OperationResources {
    pub operation_state: Arc<Mutex<OperationState>>,
    pub master_interface: Arc<MasterInterface>,
    pub data_abstraction_layer: Arc<AbstractionLayer + Send + Sync>,
    pub binary_path: String,
}

/// `OperationHandler` is used for executing Map and Reduce operations queued by the Master
pub struct OperationHandler {
    operation_state: Arc<Mutex<OperationState>>,
    master_interface: Arc<MasterInterface>,

    output_dir_uuid: String,

    data_abstraction_layer: Arc<AbstractionLayer + Send + Sync>,
}

pub fn set_complete_status(operation_state_arc: &Arc<Mutex<OperationState>>, task_id: &str) {
    let mut operation_state = operation_state_arc.lock().unwrap();
    if operation_state.current_task_id == task_id {
        operation_state.current_task_id = String::new();
        operation_state.operation_status = pb::OperationStatus::COMPLETE;
    }
}

pub fn set_failed_status(operation_state_arc: &Arc<Mutex<OperationState>>, task_id: &str) {
    let mut operation_state = operation_state_arc.lock().unwrap();
    if operation_state.current_task_id == task_id {
        operation_state.current_task_id = String::new();
        operation_state.operation_status = pb::OperationStatus::FAILED;
    }
}

// Checks if the tasks is cancelled and handles this case. Returns true if the task was canceled.
pub fn check_task_cancelled(
    operation_state_arc: &Arc<Mutex<OperationState>>,
    task_id: &str,
) -> bool {
    let operation_state = operation_state_arc.lock().unwrap();
    operation_state.task_cancelled(task_id)
}

pub fn failure_details_from_error(err: &Error) -> String {
    let mut failure_details = format!("{}", err);

    for e in err.iter().skip(1) {
        failure_details.push_str("\n");
        failure_details.push_str(&format!("caused by: {}", e));
    }
    failure_details
}

pub fn get_cpu_time() -> u64 {
    // We can panic in this case. This is beyond our control and would mostly be caused by a very
    // critical error.
    let stat = stat_self().unwrap();
    (stat.utime + stat.stime + stat.cstime + stat.cutime) as u64 / (_SC_CLK_TCK as u64)
}

impl OperationHandler {
    pub fn new(
        master_interface: Arc<MasterInterface>,
        data_abstraction_layer: Arc<AbstractionLayer + Send + Sync>,
    ) -> Self {
        OperationHandler {
            operation_state: Arc::new(Mutex::new(OperationState::new())),
            master_interface,

            output_dir_uuid: Uuid::new_v4().to_string(),

            data_abstraction_layer,
        }
    }

    pub fn get_worker_status(&self) -> pb::WorkerStatus {
        let operation_state = self.operation_state.lock().unwrap();

        if operation_state.current_task_id == "" {
            return pb::WorkerStatus::AVAILABLE;
        }

        pb::WorkerStatus::BUSY
    }

    pub fn get_worker_operation_status(&self) -> pb::OperationStatus {
        let operation_state = self.operation_state.lock().unwrap();

        operation_state.operation_status
    }

    pub fn perform_map(
        &self,
        map_options: pb::PerformMapRequest,
    ) -> impl Future<Item = (), Error = Error> {
        let resources = OperationResources {
            operation_state: Arc::clone(&self.operation_state),
            master_interface: Arc::clone(&self.master_interface),
            data_abstraction_layer: Arc::clone(&self.data_abstraction_layer),
            binary_path: map_options.get_mapper_file_path().to_string(),
        };

        let output_dir_uuid = self.output_dir_uuid.clone();

        future::lazy(move || {
            let result = map::perform_map(&map_options, &resources, &output_dir_uuid);

            future::result(result)
        })
    }

    pub fn perform_reduce(
        &self,
        reduce_request: pb::PerformReduceRequest,
    ) -> impl Future<Item = (), Error = Error> {
        let resources = OperationResources {
            operation_state: Arc::clone(&self.operation_state),
            master_interface: Arc::clone(&self.master_interface),
            data_abstraction_layer: Arc::clone(&self.data_abstraction_layer),
            binary_path: reduce_request.get_reducer_file_path().to_string(),
        };

        let output_dir_uuid = self.output_dir_uuid.clone();

        future::lazy(move || {
            let result = reduce::perform_reduce(&reduce_request, &resources, &output_dir_uuid);

            future::result(result)
        })
    }

    pub fn cancel_task(&self, request: &pb::CancelTaskRequest) -> Result<()> {
        let mut operation_state = self.operation_state.lock().unwrap();
        if operation_state.current_task_id == request.task_id {
            operation_state.current_task_id = String::new();
            operation_state.operation_status = pb::OperationStatus::UNKNOWN;
        }

        Ok(())
    }

    pub fn update_worker_status(&self) -> Result<()> {
        let worker_status = self.get_worker_status();
        let operation_status = self.get_worker_operation_status();

        self.master_interface.update_worker_status(
            worker_status,
            operation_status,
        )
    }
}
