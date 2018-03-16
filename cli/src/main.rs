extern crate chrono;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate error_chain;
extern crate grpc;
#[macro_use]
extern crate prettytable;
extern crate uuid;
extern crate util;

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

mod commands;
mod common;
mod parser;

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
            commands::run(&client, sub)
        }
        ("cluster_status", _) => {
            println!("Getting Cluster Status...");
            commands::cluster_status(&client)
        }
        ("cancel", sub) => commands::cancel(&client, sub),
        ("status", Some(sub)) => commands::status(&client, sub),
        ("upload", Some(sub)) => commands::upload(&master_addr, sub),
        ("download", Some(sub)) => commands::download(&master_addr, sub),
        _ => Err(matches.usage().into()),
    }
}
