extern crate cerberus;
extern crate env_logger;
#[macro_use]
extern crate error_chain;

use cerberus::*;

fn split_cvs_line(line: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut current_val = String::new();
    let mut in_quotes = false;

    for x in line.chars() {
        if !in_quotes && x == ',' {
            values.push(current_val);
            current_val = String::new();
        } else if x == '"' {
            in_quotes = !in_quotes;
        } else {
            current_val.push(x);
        }
    }
    values.push(current_val);

    values
}

struct RatingByYearMapper;
impl Map for RatingByYearMapper {
    type Key = String;
    type Value = f64;
    fn map<E>(&self, input: MapInputKV, mut emitter: E) -> Result<()>
    where
        E: EmitIntermediate<Self::Key, Self::Value>,
    {
        for mut line in input.value.lines() {
            if !line.contains(',') {
                continue;
            }

            line = line.trim();

            if !line.starts_with('"') || !line.ends_with('"') {
                continue;
            }

            // Ignore wrapping quotes.
            line = &line[1..(line.len() - 1)];

            let info = split_cvs_line(line);

            // Remove wrapping backslashes and trim unnecessary whitespace.
            let movie_title = info[0][1..(info[0].len() - 1)].trim().to_owned();
            let rating: f64 = info[2].parse().chain_err(|| "Error parsing movie rating")?;

            emitter
                .emit(movie_title, rating)
                .chain_err(|| "Error emitting map key-value pair.")?;
        }
        Ok(())
    }
}

struct RatingByYearPartitioner;
impl Partition<String, f64> for RatingByYearPartitioner {
    fn partition(&self, input: PartitionInputKV<String, f64>) -> Result<u64> {
        let key = input.key;
        let year_str = key[(key.len() - 5)..(key.len() - 1)].to_owned();
        let partition: u64 = year_str
            .parse()
            .chain_err(|| format!("Error getting year from movie title {}, {}", key, year_str))?;

        Ok(partition)
    }
}

struct RatingByYearReducer;
impl Reduce<String, f64> for RatingByYearReducer {
    type Output = f64;
    fn reduce<E>(&self, input: IntermediateInputKV<String, f64>, mut emitter: E) -> Result<()>
    where
        E: EmitFinal<Self::Output>,
    {
        for val in input.values {
            emitter.emit(val).chain_err(|| "Error emitting value.")?;
        }

        Ok(())
    }
}

fn run() -> Result<()> {
    env_logger::init().chain_err(|| "Failed to initialise logging.")?;

    let rby_mapper = RatingByYearMapper;
    let rby_reducer = RatingByYearReducer;
    let rby_partitioner = RatingByYearPartitioner;

    let matches = cerberus::parse_command_line();

    let registry = UserImplRegistryBuilder::new_no_combiner()
        .mapper(&rby_mapper)
        .reducer(&rby_reducer)
        .partitioner(&rby_partitioner)
        .build()
        .chain_err(|| "Error building UserImplRegistry.")?;

    cerberus::run(&matches, &registry)
}

// Macro to generate a quick error_chain main function.
// https://github.com/rust-lang-nursery/error-chain/blob/master/examples/quickstart.rs
quick_main!(run);
