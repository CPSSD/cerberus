# Dockerfile for testing the project in a Docker container.

FROM rust

MAINTAINER Cerberus Developers

RUN apt-get update -y && \
    apt-get install -y cmake protobuf-compiler golang
