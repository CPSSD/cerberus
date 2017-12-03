use std::sync::{Arc, Mutex};

use futures::future;
use futures::prelude::*;
use libc::_SC_CLK_TCK;
use procinfo::pid::stat_self;
use uuid::Uuid;

use cerberus_proto::worker as pb;
use errors::*;
use master_interface::MasterInterface;
use super::map;
use super::reduce;
use super::state::OperationState;

/// `OperationHandler` is used for executing Map and Reduce operations queued by the Master
pub struct OperationHandler {
    operation_state: Arc<Mutex<OperationState>>,
    master_interface: Arc<MasterInterface>,

    output_dir_uuid: String,
}

pub fn get_worker_status(operation_state_arc: &Arc<Mutex<OperationState>>) -> pb::WorkerStatus {
    let operation_state = operation_state_arc.lock().unwrap();

    operation_state.worker_status
}

fn set_operation_handler_status(
    operation_state_arc: &Arc<Mutex<OperationState>>,
    worker_status: pb::WorkerStatus,
    operation_status: pb::OperationStatus,
) {
    let mut operation_state = operation_state_arc.lock().unwrap();
    operation_state.worker_status = worker_status;
    operation_state.operation_status = operation_status;
}

pub fn set_complete_status(operation_state_arc: &Arc<Mutex<OperationState>>) {
    set_operation_handler_status(
        operation_state_arc,
        pb::WorkerStatus::AVAILABLE,
        pb::OperationStatus::COMPLETE,
    );
}

pub fn set_failed_status(operation_state_arc: &Arc<Mutex<OperationState>>) {
    set_operation_handler_status(
        operation_state_arc,
        pb::WorkerStatus::AVAILABLE,
        pb::OperationStatus::FAILED,
    );
}

pub fn set_busy_status(operation_state_arc: &Arc<Mutex<OperationState>>) {
    let mut operation_state = operation_state_arc.lock().unwrap();

    operation_state.worker_status = pb::WorkerStatus::BUSY;
    operation_state.operation_status = pb::OperationStatus::IN_PROGRESS;
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
    pub fn new(master_interface: Arc<MasterInterface>) -> Self {
        OperationHandler {
            operation_state: Arc::new(Mutex::new(OperationState::new())),

            master_interface: master_interface,
            output_dir_uuid: Uuid::new_v4().to_string(),
        }
    }

    pub fn get_worker_status(&self) -> pb::WorkerStatus {
        let operation_state = self.operation_state.lock().unwrap();

        operation_state.worker_status
    }

    pub fn get_worker_operation_status(&self) -> pb::OperationStatus {
        let operation_state = self.operation_state.lock().unwrap();

        operation_state.operation_status
    }

    pub fn perform_map(
        &self,
        map_options: pb::PerformMapRequest,
    ) -> impl Future<Item = (), Error = Error> {
        let operation_state_arc = Arc::clone(&self.operation_state);
        let master_interface_arc = Arc::clone(&self.master_interface);
        let output_dir_uuid = self.output_dir_uuid.clone();

        future::lazy(move || {
            let result = map::perform_map(
                &map_options,
                &operation_state_arc,
                master_interface_arc,
                &output_dir_uuid,
            );

            future::result(result)
        })
    }

    pub fn perform_reduce(
        &self,
        reduce_request: pb::PerformReduceRequest,
    ) -> impl Future<Item = (), Error = Error> {
        let operation_state_arc = Arc::clone(&self.operation_state);
        let master_interface_arc = Arc::clone(&self.master_interface);

        let output_dir_uuid = self.output_dir_uuid.clone();

        future::lazy(move || {
            let result = reduce::perform_reduce(
                &reduce_request,
                &operation_state_arc,
                master_interface_arc,
                output_dir_uuid,
            );

            future::result(result)
        })
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
