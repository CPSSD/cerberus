use clap::{App, SubCommand, Arg, ArgMatches};

pub fn parse_command_line<'a>() -> ArgMatches<'a> {
    App::new("cli")
        .version(crate_version!())
        .author("Cerberus Authors <cerberus@cpssd.net>")
        .about("Schedule MapReduces and get their status")
        .arg(
            Arg::with_name("master")
                .long("master")
                .help("Address of the master")
                .takes_value(true)
                .required(true),
        )
        .subcommand(
            SubCommand::with_name("run")
                .about("Queue a MapReduce on the cluster")
                .arg(
                    Arg::with_name("input")
                        .long("input")
                        .help("Input Directory")
                        .takes_value(true)
                        .required(true),
                )
                .arg(
                    Arg::with_name("binary")
                        .long("binary")
                        .help("MapReduce binary to run")
                        .takes_value(true)
                        .required(true),
                )
                .arg(
                    Arg::with_name("output")
                        .long("output")
                        .help("Output Directory, optional")
                        .takes_value(true)
                        .default_value(""),
                ),
        )
        .subcommand(SubCommand::with_name("cluster_status").about(
            "Status of the cluster",
        ))
        .subcommand(
            SubCommand::with_name("status")
                .about("Status of the all MapReduces scheduled with this CLI")
                .arg(
                    Arg::with_name("mapreduce_id")
                        .long("mapreduce_id")
                        .short("id")
                        .help("Optional: Limit the results to single MapReduce")
                        .required(false),
                ),
        )
        .get_matches()
}
