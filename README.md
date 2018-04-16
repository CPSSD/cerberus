# cerberus
CA4019 Project

[![Build Status](https://travis-ci.com/CPSSD/cerberus.svg?token=Ty8HySwL3To4YV7AZfi2&branch=develop)](https://travis-ci.com/CPSSD/cerberus)

---

## Requirements:

#### Development
- Rust Nightly
- Protobuf Compiler

#### Deployment
- Docker
- Docker Compose

---

## Building the project

Build everything by running:

```
$ cargo build --all
```

---

## Running benchmarks

The following dependancies are required to run the benchmarking script:
- python3-tk
- matplotlib

Ubuntu dependancy installation:
```
apt-get install python3-pip python3-tk
pip3 install numpy matplotlib
```

Run the benchmarking script with:
```
python3 benchmarks.py
```

---

## System Requirements
The project currently only works on Linux. macOS and other platforms are planned for the future.

OpenSSL is required - See http://github.com/sfackler/rust-openssl#building for installation instructions.

---

## Setting up deployment on AWS

#### Requirements:
```
pip install boto3
```

#### Deployment steps:
1. **Add AWS credentials to ~/.aws/credentials**

	A sample file is located in aws/credentials. Simply replace *ACCESS_KEY_ID* and *SECRET_ACCESS_KEY* with their respective values.

2. **Update parameters in aws.py script**

3. **Ensure that you push the latest version of the master/worker containers to DockerHub**

	This can be done by running `./production-deployment.sh` in the cerberus root directory.

5. **Configure Launch Templates**

	You need to create two Launch Templates for EC2.

	Each template **MUST** have the same name as described below and must have the given tag associated with it. Any other settings can be changed as you see fit.

	| Template Name | Tag |
	|----|----|
	| Master | Key: "type", Value: "master" |
	| Worker | Key: "type", Value: "worker" |

6. **Deploy Instances**

	To create 1 master and N workers and deploy our containers to them we can run the following  command: `python aws.py --create N --deploy`
	
_Useful commands:_
* To restart currently running instances we can run `python aws.py --terminate --deploy`
* To kill all of the instances we can use `python aws.py --kill`

---
