#!/bin/bash

dockerhub="cerberuscpssd/cerberus-production"
label="latest"

# Build the project for release.
cargo build --all --release

# Build the docker images and push them to DockerHub.
for section in "master" "worker"
do
	docker build -t cpssd/cerberus-production:${label}-${section} -f ${section}/ProdDockerfile . \
	&& docker tag cpssd/cerberus-production:${label}-${section} ${dockerhub}:${label}-${section} \
	&& docker push ${dockerhub}
done
