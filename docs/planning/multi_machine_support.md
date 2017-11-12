Multiple Machine Support
========================

## Overview:
This document outlines the requirements for making the project support multiple
machines.

## Problems fixed:
After intitial testing using docker-compose, the biggest issue was how the
workers communicate with the master.
 - It was fixed in https://github.com/CPSSD/cerberus/pull/215

## Remaining problems:
The test was ran with the presumption that the code was running on a shared
filesystem. It was implemented using shared host-mounted volumes. This however
will not the case in the final product, and the workers will have to communicate
with eachother in other to download the intermediate values. For output we might
have to stick to off-the-shelf solution for managing data - like NFS, S3 or
Google Cloud Storage.

Another problem is with assumption made by addressing the worker address issue.
If the machine has multiple IP addresses, and the first one selected is not
accessible to other machines, it will be impossible to contact the host. We
might have to look at some service discovery tools, like etcd or Consul to
solve this issue.

## Intermediate target platforms:
Docker, and docker swarm seems like a good first target platform. The code
however will also need to work on bare-metal machines, therefore we cannot make
any assumptions that both worker and master will be ran on cloud.

## Other notes:
- It appears we are fully IPv6 compatible.
