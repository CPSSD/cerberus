Multiple Machine Support
========================

## Overview:
This document outlines the requirements for making the project support multiple
machines.


## Current Problems:
After testing using docker-compose, it appears that the biggest issue that how
the workers are communicating with the master.
  - The worker tells the master that its IP is [::]:PORT, which is not a network
    accessible address. There are 2 ways of overcoming this:
      1. Try to get the network IP from the worker itself, and report that to
         master
      2. Allow the master to use the address that the machine communicated from.

    There are however disadvantages to both approach.

      1. A machine might be having multiple addresses, some of them might only
         be available on specific subnets not accessible to the entire cluster.
      2. The worker might wish to have all its communication on a different IP
         than the one it contacted the master on, say contacted using IPv4 and
         wishes to have the communications over IPv6

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
