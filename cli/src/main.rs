#[macro_use]
extern crate clap;
#[macro_use]
extern crate error_chain;
extern crate grpc;
extern crate chrono;
extern crate cerberus_proto;

use errors::*;
use std::net::SocketAddr;
use std::str::FromStr;

use cerberus_proto::mapreduce_grpc::*;

mod errors {
    error_chain!{
        foreign_links {
            Clap(::clap::Error);
            Net(::std::net::AddrParseError);
            Grpc(::grpc::Error);
        }
    }
}

mod parser;
mod runner;

quick_main!(run);

fn run() -> Result<()> {
    let matches = parser::parse_command_line();
    let master_addr = SocketAddr::from_str(matches.value_of("master").unwrap_or(""))
        .chain_err(|| "Error parsing master address")?;

    let client = MapReduceServiceClient::new_plain(
        &master_addr.ip().to_string(),
        master_addr.port(),
        Default::default(),
    ).chain_err(|| "Cannot create client")?;

    match matches.subcommand() {
        ("run", Some(sub)) => {
            println!("Scheduling MapReduce...");
            runner::run(&client, sub)
        }
        ("cluster_status", _) => {
            println!("Getting Cluster Status...");
            runner::cluster_status(&client)
        }
        ("status", Some(sub)) => runner::status(&client, sub),
        _ => Err(matches.usage().into()),
    }
}