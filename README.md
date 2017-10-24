# cerberus
CA4019 Project

[![Build Status](http://cpssd1-drone.computing.dcu.ie/api/badges/CPSSD/cerberus/status.svg)](http://cpssd1-drone.computing.dcu.ie/CPSSD/cerberus)

## Building the project

Build everything by running:

```
$ cargo build --all
```

## Logging

The [env_logger](https://doc.rust-lang.org/log/env_logger/index.html) crate is used for logging.
The log level is configured by an enviroment variable, it defaults to `error`. 
When changing the log level to `info` it may be necessary to keep the grpc log level at `error`, to avoid extraneous output.
This can be done in the following manner: `RUST_LOG=info,grpc=error ./master`
