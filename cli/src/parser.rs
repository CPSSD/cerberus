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
                        .help("Priority of the MapReduce. Valid values are 1 to 10")
                        .takes_value(true)
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
        .subcommand(
            SubCommand::with_name("upload")
                .about("Uploads a file or directory to the cluster distributed filesystem")
                .arg(
                    Arg::with_name("local_path")
                        .short("l")
                        .long("local_path")
                        .help("Path of the file or directory on the local machine")
                        .required(true)
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("remote_path")
                        .short("r")
                        .long("remote_path")
                        .help("Path of the file or directory on the cluster. The local file path will be used if this is not provided")
                        .required(false)
                        .takes_value(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("download")
                .about("Downloads a file or directory from the cluster distributed filesystem")
                .arg(
                    Arg::with_name("remote_path")
                        .short("r")
                        .long("remote_path")
                        .help("Path of the file or directory on the distributed filesystem")
                        .required(true)
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("local_path")
                        .short("l")
                        .long("local_path")
                        .help("Directory or file to store the file or directory")
                        .required(true)
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
