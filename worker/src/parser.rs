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
        .arg(
            Arg::with_name("ip")
                .long("ip")
                .short("i")
                .help("Set the IP address that can be used to communicate with this worker")
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("nfs")
                .long("nfs")
                .short("n")
                .help("Sets the NFS path for the given worker.")
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("dfs")
                .long("dfs")
                .help("Makes the worker run using the distributed file system for data access.")
                .takes_value(false)
                .required(false),
        )
        .arg(
            Arg::with_name("storage-location")
                .long("storage-location")
                .help("Directory to store dfs or s3 files.")
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
        .get_matches()
}
