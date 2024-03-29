use futures::future;
use futures::Future;
use futures_cpupool::CpuPool;

use super::worker_interface::WorkerInterface;
use errors::*;
use operations::OperationResources;

const INPUT_FETCHING_CPU_POOL_SIZE: usize = 20;

pub fn fetch_reduce_inputs(
    input_files: Vec<String>,
    output_uuid: &str,
    resources: &OperationResources,
    task_id: &str,
) -> Result<Vec<String>> {
    let cpu_pool = CpuPool::new(INPUT_FETCHING_CPU_POOL_SIZE);
    let mut input_futures = Vec::new();

    for reduce_input_file in input_files {
        let output_uuid = output_uuid.to_string();
        let resources = resources.to_owned();
        let task_id = task_id.to_string();

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
    results_future
        .wait()
        .chain_err(|| "Error running fetch reduce input futures")
}
