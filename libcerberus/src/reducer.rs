use emitter::EmitFinal;
use errors::*;
use serde::Serialize;

/// The `ReduceInputKV` is a struct for passing input data to a `Reduce`.
///
/// `ReduceInputKV` is a thin wrapper around a `(String, Vec<String>)`, used for creating a clearer API.
/// It can be constructed normally or using `ReduceInputKV::new()`.
#[derive(Debug, Default, Deserialize, PartialEq)]
pub struct ReduceInputKV {
    pub key: String,
    pub values: Vec<String>,
}

impl ReduceInputKV {
    pub fn new(key: String, values: Vec<String>) -> Self {
        ReduceInputKV {
            key: key,
            values: values,
        }
    }
}

/// The `Reduce` trait defines a function for performing a reduce operation.
///
/// The output types are decided by the implementation of this trait.
///
/// # Arguments
///
/// * `input` - A `ReduceInputKV` containing the input data for the reduce operation.
/// * `emitter` - A struct implementing the `EmitFinal` trait, provided by the reduce runner.
///
/// # Outputs
///
/// An empty result used for returning an error. Outputs of the reduce operation are sent out
/// through the `emitter`.
pub trait Reduce {
    type Value: Serialize;
    fn reduce<E>(&self, input: ReduceInputKV, emitter: E) -> Result<()>
    where
        E: EmitFinal<Self::Value>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use emitter::FinalVecEmitter;

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

    #[test]
    fn test_reducer_test_strings() {
        let test_vector = vec!["foo".to_owned(), "bar".to_owned()];
        let test_kv = ReduceInputKV::new("test_vector".to_owned(), test_vector);
        let mut sink: Vec<String> = Vec::new();
        let reducer = TestReducer;

        reducer
            .reduce(test_kv, FinalVecEmitter::new(&mut sink))
            .unwrap();

        assert_eq!("foobar", sink[0]);
    }

    #[test]
    fn reduce_input_kv_construction() {
        let test_vector = vec!["foo".to_owned(), "bar".to_owned()];

        let test_kv = ReduceInputKV::new("test_vector".to_owned(), test_vector);

        assert_eq!("foo", test_kv.values[0]);
        assert_eq!("bar", test_kv.values[1]);
    }
}
