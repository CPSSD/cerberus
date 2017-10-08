#[macro_use]
extern crate error_chain;
extern crate grpcio;

fn main() {
    println!("Cerberus Worker!");
}

mod errors {
    error_chain! {
        foreign_links {
            Grpc(::grpcio::Error);
        }
    }
}

pub mod worker_interface;
