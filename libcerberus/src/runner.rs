use std::io::{stdin, stdout};

use chrono::prelude::*;
use clap::{App, ArgMatches, SubCommand};
use uuid::Uuid;

use emitter::IntermediateVecEmitter;
use errors::*;
use io::*;
use mapper::Map;
use partition::{Partition, PartitionInputPairs};
use reducer::Reduce;
use serialise::{FinalOutputObject, FinalOutputObjectEmitter, IntermediateOutputObject,
                IntermediateOutputObjectEmitter};
use super::VERSION;

/// `UserImplRegistry` tracks the user's implementations of Map, Reduce, etc.
///
/// The user should use the `UserImplRegistryBuilder` to create this and then pass it in to `run`.
pub struct UserImplRegistry<'a, M, R, P>
where
    M: Map + 'a,
    R: Reduce + 'a,
    P: Partition<M::Key, M::Value> + 'a,
{
    mapper: &'a M,
    reducer: &'a R,
    partitioner: &'a P,
}

/// `UserImplRegistryBuilder` is used to create a `UserImplRegistry`.
pub struct UserImplRegistryBuilder<'a, M, R, P>
where
    M: Map + 'a,
    R: Reduce + 'a,
    P: Partition<M::Key, M::Value> + 'a,
{
    mapper: Option<&'a M>,
    reducer: Option<&'a R>,
    partitioner: Option<&'a P>,
}

impl<'a, M, R, P> Default for UserImplRegistryBuilder<'a, M, R, P>
where
    M: Map + 'a,
    R: Reduce + 'a,
    P: Partition<M::Key, M::Value> + 'a,
{
    fn default() -> UserImplRegistryBuilder<'a, M, R, P> {
        UserImplRegistryBuilder {
            mapper: None,
            reducer: None,
            partitioner: None,
        }
    }
}

impl<'a, M, R, P> UserImplRegistryBuilder<'a, M, R, P>
where
    M: Map + 'a,
    R: Reduce + 'a,
    P: Partition<M::Key, M::Value> + 'a,
{
    pub fn new() -> UserImplRegistryBuilder<'a, M, R, P> {
        Default::default()
    }

    pub fn mapper(mut self, mapper: &'a M) -> UserImplRegistryBuilder<'a, M, R, P> {
        self.mapper = Some(mapper);
        self
    }

    pub fn reducer(mut self, reducer: &'a R) -> UserImplRegistryBuilder<'a, M, R, P> {
        self.reducer = Some(reducer);
        self
    }

    pub fn partitioner(mut self, partitioner: &'a P) -> UserImplRegistryBuilder<'a, M, R, P> {
        self.partitioner = Some(partitioner);
        self
    }

    pub fn build(&self) -> Result<UserImplRegistry<'a, M, R, P>> {
        let mapper = self.mapper.chain_err(
            || "Error building UserImplRegistry: No Mapper provided",
        )?;
        let reducer = self.reducer.chain_err(
            || "Error building UserImplRegistry: No Reducer provided",
        )?;
        let partitioner = self.partitioner.chain_err(
            || "Error building UserImplRegistry: No Partitioner provided",
        )?;

        Ok(UserImplRegistry {
            mapper: mapper,
            reducer: reducer,
            partitioner: partitioner,
        })
    }
}

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
        .subcommand(SubCommand::with_name("sanity-check"));
    app.get_matches()
}

/// `run` begins the primary operations of the payload, and delegates to sub-functions.
///
/// # Arguments
///
/// `matches` - The output of the `parse_command_line` function.
/// `registry` - The output of the `register_mapper_reducer` function.
pub fn run<M, R, P>(matches: &ArgMatches, registry: &UserImplRegistry<M, R, P>) -> Result<()>
where
    M: Map,
    R: Reduce,
    P: Partition<M::Key, M::Value>,
{
    match matches.subcommand_name() {
        Some("map") => Ok(run_map(registry.mapper, registry.partitioner)?),
        Some("reduce") => Ok(run_reduce(registry.reducer)?),
        Some("sanity-check") => {
            run_sanity_check();
            Ok(())
        }
        None => {
            eprintln!("{}", matches.usage());
            Ok(())
        }
        // This won't ever be reached, due to clap checking invalid commands before this.
        _ => Ok(()),
    }
}

fn run_map<M, P>(mapper: &M, partitioner: &P) -> Result<()>
where
    M: Map,
    P: Partition<M::Key, M::Value>,
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

    let mut output_object = IntermediateOutputObject::<M::Key, M::Value>::default();

    partitioner
        .partition(
            PartitionInputPairs::new(pairs_vec),
            IntermediateOutputObjectEmitter::new(&mut output_object),
        )
        .chain_err(|| "Error partitioning map output")?;

    write_map_output(&mut sink, &output_object).chain_err(
        || "Error writing map output to stdout.",
    )?;
    Ok(())
}

fn run_reduce<R: Reduce>(reducer: &R) -> Result<()> {
    let mut source = stdin();
    let mut sink = stdout();
    let input_kv = read_reduce_input(&mut source).chain_err(
        || "Error getting input to reduce.",
    )?;
    let mut output_object = FinalOutputObject::<R::Value>::default();

    reducer
        .reduce(input_kv, FinalOutputObjectEmitter::new(&mut output_object))
        .chain_err(|| "Error running reduce operation.")?;

    write_reduce_output(&mut sink, &output_object).chain_err(
        || "Error writing reduce output to stdout.",
    )?;
    Ok(())
}

fn run_sanity_check() {
    println!("sanity located");
}
