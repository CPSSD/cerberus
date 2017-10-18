#[macro_use]
extern crate clap;
#[macro_use]
extern crate error_chain;

use errors::*;

mod errors {
    error_chain!{
        foreign_links {
            Clap(::clap::Error);
        }
    }
}

mod parser;
mod runner;

fn main() {
    let matches = parser::parse_command_line();

    if let Err(ref e) = run(matches) {
        for e in e.iter().skip(1) {
            println!("{}", e)
        }

        if let Some(backtrace) = e.backtrace() {
            println!("backtrace: {:?}", backtrace);
        }

        ::std::process::exit(1);
    }

    ::std::process::exit(0);
}

fn run(matches: clap::ArgMatches) -> Result<()> {
    let master_addr = matches.value_of("master").chain_err(
        || "Master address must be specified",
    )?;
    println!("Contacting master on {}", master_addr);

    match matches.subcommand() {
        ("run", Some(sub)) => runner::run(sub),
        ("cluster_status", _) => runner::cluster_status(),
        ("status", Some(sub)) => runner::status(sub),
        _ => {
            println!("{}", matches.usage());
            Err("unknown command".into())
        },
    }
}
