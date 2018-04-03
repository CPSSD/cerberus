extern crate cerberus;
extern crate env_logger;
#[macro_use]
extern crate error_chain;

use cerberus::*;

const MIN_RATING_COUNT: u64 = 100;

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

struct RatingByGenreMapper;
impl Map for RatingByGenreMapper {
    type Key = String;
    type Value = String;
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
            let movie_genre_str = info[1][1..(info[1].len() - 1)].trim().to_owned();

            let movie_genres = movie_genre_str.split('|');
            let rating: f64 = info[2].parse().chain_err(|| "Error parsing movie rating")?;
            let rating_count: u64 = info[3].parse().chain_err(
                || "Error parsing movie rating count",
            )?;

            if rating_count > MIN_RATING_COUNT {
                let rating_pair = format!("\"{}\",{}", movie_title, rating);
                for genre in movie_genres {
                    emitter
                        .emit(genre.to_owned(), rating_pair.to_owned())
                        .chain_err(|| "Error emitting map key-value pair.")?;
                }
            }
        }
        Ok(())
    }
}

struct CombineResult {
    best_rating: f64,
    best_title: String,
}

fn do_genre_combine(input: IntermediateInputKV<String, String>) -> Result<CombineResult> {
    let mut best_rating: f64 = 0.0;
    let mut best_title = String::new();

    for val in input.values {
        let info = split_cvs_line(&val);

        let movie_title = info[0].trim().to_owned();
        let rating: f64 = info[1].parse().chain_err(|| "Error parsing movie rating")?;

        if rating > best_rating {
            best_rating = rating;
            best_title = movie_title;
        }
    }

    Ok(CombineResult {
        best_rating,
        best_title,
    })
}

struct RatingByGenreCombiner;
impl Combine<String, String> for RatingByGenreCombiner {
    fn combine<E>(&self, input: IntermediateInputKV<String, String>, mut emitter: E) -> Result<()>
    where
        E: EmitFinal<String>,
    {
        let combine_result = do_genre_combine(input)?;

        let rating_pair = format!(
            "\"{}\",{}",
            combine_result.best_title,
            combine_result.best_rating
        );
        emitter.emit(rating_pair).chain_err(
            || "Error emitting value",
        )?;

        Ok(())
    }
}

struct RatingByGenreReducer;
impl Reduce<String, String> for RatingByGenreReducer {
    fn reduce<E>(&self, input: IntermediateInputKV<String, String>, mut emitter: E) -> Result<()>
    where
        E: EmitFinal<String>,
    {
        let combine_result = do_genre_combine(input)?;

        emitter.emit(combine_result.best_title).chain_err(
            || "Error emitting value",
        )?;
        emitter
            .emit(combine_result.best_rating.to_string())
            .chain_err(|| "Error emitting value")?;

        Ok(())
    }
}

fn run() -> Result<()> {
    env_logger::init().chain_err(
        || "Failed to initialise logging.",
    )?;

    let rbg_mapper = RatingByGenreMapper;
    let rbg_reducer = RatingByGenreReducer;
    let rbg_combiner = RatingByGenreCombiner;
    let rbg_partitioner = HashPartitioner::new(1);

    let matches = cerberus::parse_command_line();

    let registry = UserImplRegistryBuilder::new()
        .mapper(&rbg_mapper)
        .reducer(&rbg_reducer)
        .combiner(&rbg_combiner)
        .partitioner(&rbg_partitioner)
        .build()
        .chain_err(|| "Error building UserImplRegistry.")?;

    cerberus::run(&matches, &registry)
}

// Macro to generate a quick error_chain main function.
// https://github.com/rust-lang-nursery/error-chain/blob/master/examples/quickstart.rs
quick_main!(run);
