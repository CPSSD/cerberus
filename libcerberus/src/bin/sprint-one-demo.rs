extern crate cerberus;
extern crate env_logger;
#[macro_use]
extern crate error_chain;

use cerberus::*;
use std::str::FromStr;

struct WordCountMapper;
impl Map for WordCountMapper {
    type Key = String;
    type Value = String;
    fn map<E>(&self, input: MapInputKV, mut emitter: E) -> Result<()>
    where
        E: EmitIntermediate<Self::Key, Self::Value>,
    {
        for token in input.value.split(char::is_whitespace) {
            if !token.is_empty() {
                emitter.emit(token.to_owned(), "1".to_owned()).chain_err(
                    || "Error emitting map key-value pair.",
                )?;
            }
        }
        Ok(())
    }
}

struct WordCountReducer;
impl Reduce for WordCountReducer {
    type Value = String;
    fn reduce<E>(&self, input: ReduceInputKV, mut emitter: E) -> Result<()>
    where
        E: EmitFinal<Self::Value>,
    {
        let mut total: u64 = 0;
        for val in &input.values {
            let int_val = u64::from_str(val).chain_err(|| {
                format!("Unable to convert value {} to string.", val)
            })?;
            total += int_val;
        }
        emitter.emit(total.to_string()).chain_err(|| {
            format!("Error emitting value {:?}.", total)
        })?;
        Ok(())
    }
}

fn run() -> Result<()> {
    env_logger::init().chain_err(
        || "Failed to initialise logging.",
    )?;

    let wc_mapper = WordCountMapper;
    let wc_reducer = WordCountReducer;

    let matches = cerberus::parse_command_line();
    let registry = cerberus::register_mapper_reducer(&wc_mapper, &wc_reducer);

    cerberus::run(&matches, &registry)
}

// Macro to generate a quick error_chain main function.
// https://github.com/rust-lang-nursery/error-chain/blob/master/examples/quickstart.rs
quick_main!(run);
