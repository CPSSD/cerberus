extern crate cerberus;
#[macro_use]
extern crate error_chain;

use cerberus::*;

struct TestMapper;
impl Map for TestMapper {
    type Key = String;
    type Value = String;
    fn map<E>(&self, input: MapInputKV, mut emitter: E) -> Result<()>
    where
        E: EmitIntermediate<Self::Key, Self::Value>,
    {
        emitter.emit(input.value, "test".to_owned())?;
        Ok(())
    }
}

struct TestReducer;
impl Reduce for TestReducer {
    type Value = String;
    fn reduce<E>(&self, input: ReduceInputKV, mut emitter: E) -> Result<()>
    where
        E: EmitFinal<Self::Value>,
    {
        emitter.emit(input.values.iter().fold(
            String::new(),
            |acc, x| acc + x,
        ))?;
        Ok(())
    }
}

fn run() -> Result<()> {
    let test_mapper = TestMapper;
    let test_reducer = TestReducer;

    let matches = cerberus::parse_command_line();
    let registry = cerberus::register_mapper_reducer(&test_mapper, &test_reducer);

    cerberus::run(&matches, &registry)
}

// Macro to generate a quick error_chain main function.
// https://github.com/rust-lang-nursery/error-chain/blob/master/examples/quickstart.rs
quick_main!(run);
