use clap::ArgMatches;
use errors::*;

pub fn run(matches: &ArgMatches) -> Result<()> {
    println!("Starting MapReduce run...");

    let input = matches.value_of("input").chain_err(
        || "Input directory cannot be empty",
    )?;

    let binary = matches.value_of("binary").chain_err(
        || "Binary cannot be empty",
    )?;

    let output = matches.value_of("output").chain_err(|| "")?;

    println!("input: {}", input);
    println!("binary: {}", binary);
    println!("output: {}", output);

    Ok(())
}

pub fn cluster_status() -> Result<()> {
    println!("Getting cluster status...");
    Ok(())
}

pub fn status(matches: &ArgMatches) -> Result<()> {
    println!("Getting results...");
    let mr_id = matches.value_of("mapreduce_id").unwrap_or("").to_owned();
    println!("{}", mr_id);

    Ok(())
}
