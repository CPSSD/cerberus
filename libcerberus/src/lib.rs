#![recursion_limit = "1024"]

#[macro_use]
extern crate error_chain;

mod errors {
    error_chain!{}
}

use errors::*;

pub fn run_mapreduce() -> Result<String> {
    Ok("Success!".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_mapreduce_expect_success() {
        assert_eq!("Success!", run_mapreduce().unwrap());
    }
}
