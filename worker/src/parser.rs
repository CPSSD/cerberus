use clap::{App, Arg, ArgMatches};

pub fn parse_command_line<'a>() -> ArgMatches<'a> {
    App::new("worker")
        .version(crate_version!())
        .author("Cerberus Authors <cerberus@cpssd.net>")
        .about("Responsible for running MapReduce jobs")
        .arg(
            Arg::with_name("master")
                .long("master")
                .short("m")
                .help("Address of the master")
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("port")
                .long("port")
                .short("p")
                .help("Set the worker port")
                .takes_value(true)
                .required(false),
        )
        .get_matches()
}
