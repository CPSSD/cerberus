extern crate cerberus;
extern crate env_logger;
#[macro_use]
extern crate error_chain;
extern crate regex;

use regex::Regex;

use cerberus::*;

const MAP_OUTPUT_PARTITIONS: u64 = 15;
const REGEX: &str = r"\b\w{15,}\b";

fn get_longest_word(line: &str) -> String {
    let mut longest: String = "".to_string();

    for word in line.split(|c: char| !c.is_alphabetic()) {
        if word.len() > longest.len() {
            longest = word.to_string();
        }
    }

    longest
}

struct GrepMapper;
impl Map for GrepMapper {
    type Key = usize;
    type Value = String;
    fn map<E>(&self, input: MapInputKV, mut emitter: E) -> Result<()>
    where
        E: EmitIntermediate<Self::Key, Self::Value>,
    {
        let regex = Regex::new(REGEX).chain_err(|| "Error creating regex object.")?;

        for line in input.value.lines() {
            if regex.is_match(line) {
                let word = get_longest_word(line);
                emitter
                    .emit(word.len(), line.to_owned())
                    .chain_err(|| "Error emitting map key-value pair.")?;
            }
        }
        Ok(())
    }
}

struct GrepReducer;
impl Reduce<usize, String> for GrepReducer {
    fn reduce<E>(&self, input: IntermediateInputKV<usize, String>, mut emitter: E) -> Result<()>
    where
        E: EmitFinal<String>,
    {
        for val in input.values {
            emitter.emit(val).chain_err(|| "Error emitting value.")?;
        }
        Ok(())
    }
}

fn run() -> Result<()> {
    env_logger::init().chain_err(|| "Failed to initialise logging.")?;

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
