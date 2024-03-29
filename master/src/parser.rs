use clap::{App, Arg, ArgMatches};

pub fn parse_command_line<'a>() -> ArgMatches<'a> {
    App::new("master")
        .version(crate_version!())
        .author("Cerberus Authors <cerberus@cpssd.net>")
        .about("Responsible for scheduling MapReduce jobs and managing workers")
        .arg(
            Arg::with_name("port")
                .long("port")
                .short("p")
                .help("Port which the master will use to communicate with the client and workers")
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("fresh")
                .long("fresh")
                .short("f")
                .help("Skips state loading")
                .takes_value(false)
                .required(false),
        )
        .arg(
            Arg::with_name("nodump")
                .long("nodump")
                .short("n")
                .help("Skips state dumping")
                .takes_value(false)
                .required(false),
        )
        .arg(
            Arg::with_name("verbose-logging")
                .long("verbose-logging")
                .short("v")
                .help("Removes all log filters")
                .takes_value(false)
                .required(false),
        )
        .arg(
            Arg::with_name("log-file-path")
                .long("log-file-path")
                .short("l")
                .help("Location to write log file")
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("state-location")
                .long("state-location")
                .short("s")
                .help("The location that state is saved to and loaded from")
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("nfs")
                .long("nfs")
                .help("Sets the NFS path for the master")
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("dashboard-address")
                .long("dashboard-address")
                .short("d")
                .help("The address to serve the cluster dashboard to")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("dfs")
                .long("dfs")
                .help("Makes the master run using the distributed file system for data access.")
                .takes_value(false)
                .required(false),
        )
        .arg(
            Arg::with_name("storage-location")
                .long("storage-location")
                .help("Directory to store dfs and s3 files.")
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("s3")
                .long("s3")
                .help("The Amazon S3 bucket to use for data access.")
                .takes_value(true)
                .required(false),
        )
        .get_matches()
}
