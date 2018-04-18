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
        for word in input.value.split_whitespace() {
            emitter.emit(word.to_owned(), "test".to_owned())?;
        }
        Ok(())
    }
}

struct TestReducer;
impl Reduce<String, String> for TestReducer {
    type Output = String;
    fn reduce<E>(&self, input: IntermediateInputKV<String, String>, mut emitter: E) -> Result<()>
    where
        E: EmitFinal<Self::Output>,
    {
        emitter.emit(input.values.iter().fold(String::new(), |acc, x| acc + x))?;
        Ok(())
    }
}

struct TestPartitioner;
impl Partition<String, String> for TestPartitioner {
    fn partition(&self, input: PartitionInputKV<String, String>) -> Result<u64> {
        let key = input.key;
        let first_char = key.chars()
            .nth(0)
            .chain_err(|| "Cannot partition key of empty string.")?;
        let partition = {
            if first_char.is_lowercase() {
                if first_char > 'm' {
                    1
                } else {
                    0
                }
            } else if first_char > 'M' {
                1
            } else {
                0
            }
        };

        Ok(partition)
    }
}

fn run() -> Result<()> {
    let test_mapper = TestMapper;
    let test_reducer = TestReducer;
    let test_partitioner = TestPartitioner;

    let matches = cerberus::parse_command_line();

    let registry = UserImplRegistryBuilder::new_no_combiner()
        .mapper(&test_mapper)
        .reducer(&test_reducer)
        .partitioner(&test_partitioner)
        .build()
        .chain_err(|| "Error building UserImplRegistry.")?;

    cerberus::run(&matches, &registry)
}

// Macro to generate a quick error_chain main function.
// https://github.com/rust-lang-nursery/error-chain/blob/master/examples/quickstart.rs
quick_main!(run);
