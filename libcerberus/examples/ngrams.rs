extern crate cerberus;
extern crate env_logger;
#[macro_use]
extern crate error_chain;

use std::collections::HashSet;

use cerberus::*;

const MAP_OUTPUT_PARTITIONS: u64 = 200;

struct NGramsMapper;
impl Map for NGramsMapper {
    type Key = String;
    type Value = String;
    fn map<E>(&self, input: MapInputKV, mut emitter: E) -> Result<()>
    where
        E: EmitIntermediate<Self::Key, Self::Value>,
    {
        for line in input.value.lines() {
            let info: Vec<&str> = line.split_whitespace().collect();

            let word1: String = info[0].to_string();
            let word2: String = info[1].to_string();

            let year: u32 = info[2].parse().chain_err(|| "Error parsing year")?;

            /*
            let match_count: u32 = info[3].parse().chain_err(|| "Error parsing year")?;
            let volumne_count: u32 = info[4].parse().chain_err(|| "Error parsing year")?;
            */

            // Only care about the modern era
            if year >= 2000 {
                emitter
                    .emit(word2, word1)
                    .chain_err(|| "Error emitting map key-value pair.")?;
            }
        }
        Ok(())
    }
}

fn get_index(c: char) -> u64 {
    // Characters (0 -> 9, a -> z) can be converted to a digit. 
    // 36 different possible values.
    match c.to_digit(36) {
        Some(d) => u64::from(d),
        // Not a character that can be converted to a digit.
        None => 37,
    }
}

fn get_place_in_range(x: u64, max_x: u64, start_range: u64, end_range: u64) -> u64 {
    if x >= max_x {
        return end_range;
    }
    let range = end_range - start_range;
    let new_value = ((x * range) / max_x) + start_range;
    return new_value;
}

struct NGramsPartitioner;
impl Partition<String, String> for NGramsPartitioner {
    fn partition(&self, input: PartitionInputKV<String, String>) -> Result<u64> {
        let mut chars = input.key.chars();

        let first_index = match chars.next() {
            Some(c) => get_index(c),
            None => 0,
        };

        let second_index = match chars.next() {
            Some(c) => get_index(c),
            None => 0,
        };

        let start_first_char = get_place_in_range(first_index, 37, 0, MAP_OUTPUT_PARTITIONS);
        let end_first_char = get_place_in_range(first_index + 1, 37, 0, MAP_OUTPUT_PARTITIONS);
        let partition = get_place_in_range(second_index, 37, start_first_char, end_first_char);

        Ok(partition)
    }
}

struct NGramsCombiner;
impl Combine<String, String> for NGramsCombiner {
    fn combine<E>(&self, input: IntermediateInputKV<String, String>, mut emitter: E) -> Result<()>
    where
        E: EmitFinal<String>,
    {
        let mut words: HashSet<String> = HashSet::new();
        for word in input.values {
            words.insert(word);
        }

        for word in words {
            emitter.emit(word).chain_err(|| "Error emitting value.")?;
        }

        Ok(())
    }
}

struct NGramsReducer;
impl Reduce<String, String> for NGramsReducer {
    type Output = u32;
    fn reduce<E>(&self, input: IntermediateInputKV<String, String>, mut emitter: E) -> Result<()>
    where
        E: EmitFinal<Self::Output>,
    {
        let mut words: HashSet<String> = HashSet::new();
        for word in input.values {
            words.insert(word);
        }

        emitter
            .emit(words.len() as u32)
            .chain_err(|| "Error emitting value.")?;

        Ok(())
    }
}

fn run() -> Result<()> {
    env_logger::init().chain_err(|| "Failed to initialise logging.")?;

    let ng_mapper = NGramsMapper;
    let ng_reducer = NGramsReducer;
    let ng_partitioner = NGramsPartitioner;
    let ng_combiner = NGramsCombiner;

    let matches = cerberus::parse_command_line();

    let registry = UserImplRegistryBuilder::new()
        .mapper(&ng_mapper)
        .reducer(&ng_reducer)
        .partitioner(&ng_partitioner)
        .combiner(&ng_combiner)
        .build()
        .chain_err(|| "Error building UserImplRegistry.")?;

    cerberus::run(&matches, &registry)
}

// Macro to generate a quick error_chain main function.
// https://github.com/rust-lang-nursery/error-chain/blob/master/examples/quickstart.rs
quick_main!(run);
