#[macro_use]
extern crate error_chain;
extern crate grpcio;
extern crate uuid;

fn main() {
    println!("Cerberus Master!");
}

mod errors {
    error_chain! {
        foreign_links {
            Grpc(::grpcio::Error);
        }
    }
}

pub mod client_interface;
pub mod scheduler;
pub mod queued_work_store;
pub mod mapreduce_job;
