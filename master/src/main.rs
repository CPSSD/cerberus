#[macro_use]
extern crate error_chain;
extern crate grpcio;

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
