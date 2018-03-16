extern crate cerberus;
extern crate env_logger;
#[macro_use]
extern crate error_chain;
extern crate regex;

use std::path::Path;

use regex::Regex;

use cerberus::*;

const MAP_OUTPUT_PARTITIONS: u64 = 15;
const REGEX: &str = r"\b\w{15,}\b";

struct GrepMapper;
impl Map for GrepMapper {
    type Key = String;
    type Value = String;
    fn map<E>(&self, input: MapInputKV, mut emitter: E) -> Result<()>
    where
        E: EmitIntermediate<Self::Key, Self::Value>,
    {
        let regex = Regex::new(REGEX).chain_err(
            || "Error creating regex object.",
        )?;
        let output_key = Path::new(&input.key)
            .file_name()
            .chain_err(|| "Error getting output key.")?
            .to_str()
            .chain_err(|| "Error getting output key.")?;

        for line in input.value.lines() {
            if regex.is_match(line) {
                emitter
                    .emit(output_key.to_owned(), line.to_owned())
                    .chain_err(|| "Error emitting map key-value pair.")?;
            }
        }
        Ok(())
    }
}

struct GrepReducer;
impl Reduce for GrepReducer {
    type Key = String;
    type Value = String;
    fn reduce<E>(
        &self,
        input: IntermediateInputKV<Self::Key, Self::Value>,
        mut emitter: E,
    ) -> Result<()>
    where
        E: EmitFinal<Self::Value>,
    {
        for val in input.values {
            emitter.emit(val).chain_err(|| "Error emitting value.")?;
        }
        Ok(())
    }
}

fn run() -> Result<()> {
    env_logger::init().chain_err(
        || "Failed to initialise logging.",
    )?;

    let grep_mapper = GrepMapper;
    let grep_reducer = GrepReducer;
    let grep_partitioner = HashPartitioner::new(MAP_OUTPUT_PARTITIONS);

    let matches = cerberus::parse_command_line();

    let registry = UserImplRegistryBuilder::new_no_combiner()
        .mapper(&grep_mapper)
        .reducer(&grep_reducer)
        .partitioner(&grep_partitioner)
        .build()
        .chain_err(|| "Error building UserImplRegistry.")?;

    cerberus::run(&matches, &registry)
}

// Macro to generate a quick error_chain main function.
// https://github.com/rust-lang-nursery/error-chain/blob/master/examples/quickstart.rs
quick_main!(run);
