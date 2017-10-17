use chrono::prelude::*;
use clap::{App, ArgMatches, SubCommand};
use errors::*;
use io::*;
use mapper::Map;
use reducer::Reduce;
use serialise::{FinalOutputObject, FinalOutputObjectEmitter, IntermediateOutputObject,
                IntermediateOutputObjectEmitter};
use std::io::{stdin, stdout};
use super::VERSION;
use uuid::Uuid;

/// `UserImplRegistry` tracks the user's implementations of Map, Reduce, etc.
///
/// The user does not have to interact with this class directly. It is returned from
/// `register_mapper_reducer` and accepted by `run`.
pub struct UserImplRegistry<'a, M, R>
where
    M: Map + 'a,
    R: Reduce + 'a,
{
    mapper: &'a M,
    reducer: &'a R,
}

/// `register_mapper_reducer` registers instances of the user's Map and Reduce implementations.
///
/// One of the functions whose outputs are required by the `run` function.
pub fn register_mapper_reducer<'a, M, R>(
    mapper: &'a M,
    reducer: &'a R,
) -> UserImplRegistry<'a, M, R>
where
    M: Map,
    R: Reduce,
{
    UserImplRegistry {
        mapper: mapper,
        reducer: reducer,
    }
}

/// `parse_command_line` uses `clap` to parse the command-line arguments passed to the payload.
///
/// The output of this function is required by the `run` function, to decide what subcommand to
/// run.
pub fn parse_command_line<'a>() -> Result<ArgMatches<'a>> {
    let current_time = Utc::now();
    let id = Uuid::new_v4();
    let payload_name = format!("{}_{}", current_time.format("%+"), id);
    let app = App::new(payload_name)
        .version(VERSION.unwrap_or("unknown"))
        .subcommand(SubCommand::with_name("map"))
        .subcommand(SubCommand::with_name("reduce"))
        .subcommand(SubCommand::with_name("sanity-check"));
    let matches = app.get_matches_safe().chain_err(
        || "Error parsing command line arguments.",
    )?;
    Ok(matches)
}

/// `run` begins the primary operations of the payload, and delegates to sub-functions.
///
/// # Arguments
///
/// `matches` - The output of the `parse_command_line` function.
/// `registry` - The output of the `register_mapper_reducer` function.
pub fn run<M, R>(matches: &ArgMatches, registry: &UserImplRegistry<M, R>) -> Result<()>
where
    M: Map,
    R: Reduce,
{
    match matches.subcommand_name() {
        Some("map") => Ok(run_map(registry.mapper)?),
        Some("reduce") => Ok(run_reduce(registry.reducer)?),
        Some("sanity-check") => {
            run_sanity_check();
            Ok(())
        }
        Some(invalid) => Err(format!("Invalid command {}.", invalid).into()),
        None => {
            eprintln!("{}", matches.usage());
            Ok(())
        }
    }
}

fn run_map<M: Map>(mapper: &M) -> Result<()> {
    let mut source = stdin();
    let mut sink = stdout();
    let input_kv = read_map_input(&mut source).chain_err(
        || "Error getting input to map.",
    )?;
    let mut output_object = IntermediateOutputObject::<M::Key, M::Value>::default();

    mapper
        .map(
            input_kv,
            IntermediateOutputObjectEmitter::new(&mut output_object),
        )
        .chain_err(|| "Error running map operation.")?;

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
