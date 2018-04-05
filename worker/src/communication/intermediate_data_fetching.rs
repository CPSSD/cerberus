use futures::Future;
use futures::future;
use futures_cpupool::CpuPool;

use errors::*;
use super::worker_interface::WorkerInterface;
use operations::OperationResources;

pub fn fetch_reduce_inputs(
    input_files: Vec<String>,
    output_uuid: String,
    resources: OperationResources,
    task_id: String,
) -> Result<Vec<String>> {
    let cpu_pool = CpuPool::new_num_cpus();
    let mut input_futures = Vec::new();

    for reduce_input_file in input_files {
        let output_uuid = output_uuid.clone();
        let resources = resources.clone();
        let task_id = task_id.clone();

        let input_future = cpu_pool.spawn_fn(move || {
            let reduce_input_result =
                WorkerInterface::get_data(reduce_input_file, &output_uuid, &resources, &task_id)
                    .chain_err(|| "Couldn't read reduce input file");

            match reduce_input_result {
                Ok(input) => future::ok(input),
                Err(err) => future::err(err),
            }

        });

        input_futures.push(input_future);
    }

    let results_future = future::join_all(input_futures);
    results_future.wait().chain_err(
        || "Error running fetch reduce input futures",
    )
}
