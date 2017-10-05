#[macro_use]
extern crate error_chain;
extern crate grpcio;

fn main() {
    println!("Cerberus Master!");
}

mod errors {
    error_chain!{}
}

pub mod client_interface;
pub mod scheduler;
