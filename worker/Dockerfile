FROM rust:jessie

# TODO: consider alternatives.
# Required thanks to local-ip package...
RUN apt update && apt install -y net-tools

EXPOSE 3000

ADD target/release/worker /bin/worker

CMD ["/bin/worker"]
