use clap::{App, SubCommand, Arg, ArgMatches};

pub fn parse_command_line<'a>() -> ArgMatches<'a> {
    App::new("cli")
        .version(crate_version!())
        .author("Cerberus Authors <cerberus@cpssd.net>")
        .about("Schedule MapReduces and get their status")
        .arg(
            Arg::with_name("master")
                .long("master")
                .short("m")
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
                        .short("i")
                        .help("Input Directory")
                        .takes_value(true)
                        .required(true),
                )
                .arg(
                    Arg::with_name("binary")
                        .long("binary")
                        .short("b")
                        .help("MapReduce binary to run")
                        .takes_value(true)
                        .required(true),
                )
                .arg(
                    Arg::with_name("output")
                        .long("output")
                        .short("o")
                        .help("Output Directory, optional")
                        .takes_value(true)
                        .default_value("")
                        .required(false),
                )
                .arg(
                    Arg::with_name("priority")
                        .long("priority")
                        .short("p")
                        .help("Priority of the MapReduce")
                        .takes_value(true)
                        .default_value("1")
                        .required(false),
                )
        )
        .subcommand(
            SubCommand::with_name("cancel")
                .about("Cancels a running or queued MapReduce. If no id is provided it will cancel the most recently schceduled job")
                .arg(
                    Arg::with_name("id")
                        .short("i")
                        .help("ID of the MapReduce to cancel")
                        .required(false)
                        .takes_value(true),
                ),
        )
        .subcommand(SubCommand::with_name("cluster_status").about(
            "Status of the cluster",
        ))
        .subcommand(
            SubCommand::with_name("status")
                .about("Status of the all MapReduces scheduled with this CLI")
                .arg(
                    Arg::with_name("job_id")
                        .long("job_id")
                        .short("j")
                        .help("Optional: Limit the results to single MapReduce job")
                        .required(false),
                ),
        )
        .get_matches()
}
