use std::collections::HashMap;
use std::io::{stdin, stdout};

use chrono::prelude::*;
use clap::{App, ArgMatches, SubCommand};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json;
use uuid::Uuid;

use combiner::Combine;
use emitter::IntermediateVecEmitter;
use errors::*;
use intermediate::IntermediateInputKV;
use io::*;
use mapper::Map;
use partition::{Partition, PartitionInputKV};
use reducer::Reduce;
use registry::UserImplRegistry;
use serialise::{FinalOutputObject, FinalOutputObjectEmitter, IntermediateOutputObject, VecEmitter,
                IntermediateOutputPair};
use super::VERSION;

/// `parse_command_line` uses `clap` to parse the command-line arguments passed to the payload.
///
/// The output of this function is required by the `run` function, to decide what subcommand to
/// run.
pub fn parse_command_line<'a>() -> ArgMatches<'a> {
    let current_time = Utc::now();
    let id = Uuid::new_v4();
    let payload_name = format!("{}_{}", current_time.format("%+"), id);
    let app = App::new(payload_name)
        .version(VERSION.unwrap_or("unknown"))
        .subcommand(SubCommand::with_name("map"))
        .subcommand(SubCommand::with_name("reduce"))
        .subcommand(SubCommand::with_name("combine"))
        .subcommand(SubCommand::with_name("has-combine"))
        .subcommand(SubCommand::with_name("sanity-check"));
    app.get_matches()
}

/// `run` begins the primary operations of the payload, and delegates to sub-functions.
///
/// # Arguments
///
/// `matches` - The output of the `parse_command_line` function.
/// `registry` - The output of the `register_mapper_reducer` function.
pub fn run<M, R, P, C>(matches: &ArgMatches, registry: &UserImplRegistry<M, R, P, C>) -> Result<()>
where
    M: Map,
    R: Reduce<M::Key, M::Value>,
    P: Partition<M::Key, M::Value>,
    C: Combine<M::Key, M::Value>,
{
    match matches.subcommand_name() {
        Some("map") => {
            run_map(registry.mapper, registry.partitioner, registry.combiner)
                .chain_err(|| "Error running map")?;
            Ok(())
        }
        Some("reduce") => {
            run_reduce(registry.reducer).chain_err(
                || "Error running reduce",
            )?;
            Ok(())
        }
        Some("combine") => {
            let combiner = registry.combiner.chain_err(
                || "Attempt to run combine command when combiner is not implemented",
            )?;
            run_combine(combiner).chain_err(|| "Error running combine")?;
            Ok(())
        }
        Some("has-combine") => {
            run_has_combine(registry.combiner);
            Ok(())
        }
        Some("sanity-check") => {
            run_sanity_check();
            Ok(())
        }
        None => {
            eprintln!("{}", matches.usage());
            Ok(())
        }
        Some(other) => Err(format!("Unknown command {}", other).into()),
    }
}

fn run_map<M, P, C>(mapper: &M, partitioner: &P, combiner_option: Option<&C>) -> Result<()>
where
    M: Map,
    P: Partition<M::Key, M::Value>,
    C: Combine<M::Key, M::Value>,
{
    let mut source = stdin();
    let mut sink = stdout();
    let input_kv = read_map_input(&mut source).chain_err(
        || "Error getting input to map.",
    )?;

    let mut pairs_vec: Vec<(M::Key, M::Value)> = Vec::new();

    mapper
        .map(input_kv, IntermediateVecEmitter::new(&mut pairs_vec))
        .chain_err(|| "Error running map operation.")?;

    if let Some(combiner) = combiner_option {
        let new_pairs_vec = run_internal_combine(combiner, &mut pairs_vec).chain_err(
            || "Error running combine on map results",
        )?;

        pairs_vec = new_pairs_vec;
    }

    let mut output_object = IntermediateOutputObject::<M::Key, M::Value>::default();

    for pair in pairs_vec.drain(0..) {
        let partition = partitioner
            .partition(PartitionInputKV::new(&pair.0, &pair.1))
            .chain_err(|| "Error partitioning map output")?;
        let output_array = output_object.partitions.entry(partition).or_insert_with(
            Default::default,
        );
        output_array.push(IntermediateOutputPair {
            key: pair.0,
            value: pair.1,
        });
    }

    write_intermediate_output(&mut sink, &output_object)
        .chain_err(|| "Error writing map output to stdout.")?;
    Ok(())
}

fn run_reduce<K, V, R: Reduce<K, V>>(reducer: &R) -> Result<()>
where
    K: Default + Serialize + DeserializeOwned,
    V: Default + Serialize + DeserializeOwned,
{
    let mut source = stdin();
    let mut sink = stdout();
    let input_kvs = read_intermediate_input(&mut source).chain_err(
        || "Error getting input to reduce.",
    )?;

    let mut output_objects = Vec::new();
    for input_kv in input_kvs {
        let mut output_object = FinalOutputObject::<V>::default();
        reducer
            .reduce(input_kv, FinalOutputObjectEmitter::new(&mut output_object))
            .chain_err(|| "Error running reduce operation.")?;
        output_objects.push(output_object);
    }

    write_reduce_output(&mut sink, &output_objects).chain_err(
        || "Error writing reduce output to stdout.",
    )?;
    Ok(())
}

fn run_combine<K, V, C>(combiner: &C) -> Result<()>
where
    K: Default + Serialize + DeserializeOwned,
    V: Default + Serialize + DeserializeOwned,
    C: Combine<K, V>,
{
    let mut source = stdin();
    let mut sink = stdout();
    let input_kvs = read_intermediate_input(&mut source).chain_err(
        || "Error getting input to combine.",
    )?;

    let mut output_objects = Vec::new();

    for input_kv in input_kvs {
        let mut output_object = Vec::<V>::new();
        combiner
            .combine(input_kv, VecEmitter::new(&mut output_object))
            .chain_err(|| "Error running combine operation.")?;
        output_objects.push(output_object);
    }

    write_intermediate_vectors(&mut sink, &output_objects)
        .chain_err(|| "Error writing combine output to stdout.")?;
    Ok(())
}

fn run_internal_combine<K, V, C>(combiner: &C, pairs: &mut Vec<(K, V)>) -> Result<Vec<(K, V)>>
where
    K: Default + Serialize + DeserializeOwned,
    V: Default + Serialize + DeserializeOwned,
    C: Combine<K, V>,
{
    let mut kv_map: HashMap<String, Vec<V>> = HashMap::new();

    for pair in pairs.drain(0..) {
        let mut key = pair.0;
        let mut value = pair.1;

        // Use serde_json to serialize the key to a string.
        let key_str = json!(key).to_string();

        let vec = kv_map.entry(key_str).or_insert_with(Vec::new);
        vec.push(value);
    }

    let mut results = Vec::new();

    for (key_str, mut values) in kv_map.drain() {
        let key_json: serde_json::Value = serde_json::from_str(&key_str).chain_err(
            || "Error parsing combine key.",
        )?;

        // Retrieve the original key from the serialized version.
        let key = serde_json::from_value(key_json.clone()).chain_err(
            || "Error converting combine key string to key type",
        )?;

        if values.len() > 1 {
            let input_kv = IntermediateInputKV { key, values };

            let mut result_values = Vec::<V>::new();

            combiner
                .combine(input_kv, VecEmitter::new(&mut result_values))
                .chain_err(|| "Error running combine operation.")?;

            for value in result_values {
                let key = serde_json::from_value(key_json.clone()).chain_err(
                    || "Error converting combine key string to key type",
                )?;

                results.push((key, value));
            }
        } else if values.len() == 1 {
            results.push((key, values.remove(0)));
        }
    }

    Ok(results)
}

fn run_has_combine<K, V, C>(combiner: Option<&C>)
where
    K: Default + Serialize + DeserializeOwned,
    V: Default + Serialize + DeserializeOwned,
    C: Combine<K, V>,
{
    match combiner {
        Some(_) => println!("yes"),
        None => println!("no"),
    }
}

fn run_sanity_check() {
    println!("sanity located");
}
