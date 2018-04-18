extern crate cerberus;
extern crate env_logger;
#[macro_use]
extern crate error_chain;

use cerberus::*;

const MAP_OUTPUT_PARTITIONS: u64 = 15;

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

struct RatingAggregatorMapper;
impl Map for RatingAggregatorMapper {
    type Key = u32;
    type Value = String;
    fn map<E>(&self, input: MapInputKV, mut emitter: E) -> Result<()>
    where
        E: EmitIntermediate<Self::Key, Self::Value>,
    {
        // Two posibilities for input, a list of movie ratings or a list of movie information.
        for line in input.value.lines() {
            let info = split_cvs_line(line);

            if info.len() == 3 {
                // Movie info
                let movie_id: u32 = info[0].parse().chain_err(|| "Error parsing movie id")?;
                let movie_title = format!("T:{}", info[1]);
                let movie_genre = format!("G:{}", info[2]);

                emitter
                    .emit(movie_id, movie_title)
                    .chain_err(|| "Error emitting map key-value pair.")?;
                emitter
                    .emit(movie_id, movie_genre)
                    .chain_err(|| "Error emitting map key-value pair.")?;
            } else {
                // Rating info
                let movie_id: u32 = info[1].parse().chain_err(|| "Error parsing movie id")?;
                let rating: f64 = info[2].parse().chain_err(|| "Error parsing movie rating")?;

                emitter
                    .emit(movie_id, rating.to_string())
                    .chain_err(|| "Error emitting map key-value pair.")?;

                emitter
                    .emit(movie_id, "C:1".to_string())
                    .chain_err(|| "Error emitting map key-value pair.")?;
            }
        }
        Ok(())
    }
}

struct RatingCombineResult {
    rating: f64,
    rating_count: u32,

    title: String,
    genres: String,
}

fn do_rating_combine(input: IntermediateInputKV<u32, String>) -> Result<RatingCombineResult> {
    let mut rating_count: u32 = 0;
    let mut rating: f64 = 0.0;

    let mut movie_title = String::new();
    let mut movie_genres = String::new();

    for val in input.values {
        if val.starts_with("T:") {
            movie_title = val;
            continue;
        } else if val.starts_with("G:") {
            movie_genres = val;
            continue;
        } else if val.starts_with("C:") {
            let add_count: u32 = val[2..].parse().chain_err(|| "Error parsing movie count")?;
            rating_count += add_count;
            continue;
        }

        let rating_val: f64 = val.parse().chain_err(|| "Error parsing movie rating")?;

        rating += rating_val;
    }

    Ok(RatingCombineResult {
        rating,
        rating_count,

        title: movie_title,
        genres: movie_genres,
    })
}

struct RatingAggregatorCombiner;
impl Combine<u32, String> for RatingAggregatorCombiner {
    fn combine<E>(&self, input: IntermediateInputKV<u32, String>, mut emitter: E) -> Result<()>
    where
        E: EmitFinal<String>,
    {
        let combine_result = do_rating_combine(input)?;
        if !combine_result.title.is_empty() {
            emitter
                .emit(combine_result.title)
                .chain_err(|| "Error emitting value.")?;
        }

        if !combine_result.genres.is_empty() {
            emitter
                .emit(combine_result.genres)
                .chain_err(|| "Error emitting value.")?;
        }

        if combine_result.rating_count > 0 {
            emitter
                .emit(combine_result.rating.to_string())
                .chain_err(|| "Error emitting value.")?;

            emitter
                .emit(format!("C:{}", combine_result.rating_count.to_string()))
                .chain_err(|| "Error emitting value.")?;
        }

        Ok(())
    }
}

struct RatingAggregatorReducer;
impl Reduce<u32, String> for RatingAggregatorReducer {
    type Output = String;
    fn reduce<E>(&self, input: IntermediateInputKV<u32, String>, mut emitter: E) -> Result<()>
    where
        E: EmitFinal<Self::Output>,
    {
        let mut combine_result = do_rating_combine(input)?;

        if combine_result.rating_count > 0 {
            combine_result.rating /= f64::from(combine_result.rating_count);
        }

        if !combine_result.title.is_empty() {
            let output_str = format!(
                "\"{}\",\"{}\",{},{}",
                combine_result.title[2..].to_string(),
                combine_result.genres[2..].to_string(),
                combine_result.rating,
                combine_result.rating_count
            );

            emitter
                .emit(output_str)
                .chain_err(|| "Error emitting value.")?;
        }

        Ok(())
    }
}

fn run() -> Result<()> {
    env_logger::init().chain_err(|| "Failed to initialise logging.")?;

    let ra_mapper = RatingAggregatorMapper;
    let ra_reducer = RatingAggregatorReducer;
    let ra_partitioner = HashPartitioner::new(MAP_OUTPUT_PARTITIONS);
    let ra_combiner = RatingAggregatorCombiner;

    let matches = cerberus::parse_command_line();

    let registry = UserImplRegistryBuilder::new()
        .mapper(&ra_mapper)
        .reducer(&ra_reducer)
        .partitioner(&ra_partitioner)
        .combiner(&ra_combiner)
        .build()
        .chain_err(|| "Error building UserImplRegistry.")?;

    cerberus::run(&matches, &registry)
}

// Macro to generate a quick error_chain main function.
// https://github.com/rust-lang-nursery/error-chain/blob/master/examples/quickstart.rs
quick_main!(run);
