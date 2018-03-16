use serde::Serialize;
use serde::de::DeserializeOwned;

use emitter::EmitFinal;
use errors::*;
use intermediate::IntermediateInputKV;

/// The `Reduce` trait defines a function for performing a reduce operation.
///
/// The output types are decided by the implementation of this trait.
///
/// # Arguments
///
/// * `input` - A `IntermediateInputKV` containing the input data for the reduce operation.
/// * `emitter` - A struct implementing the `EmitFinal` trait, provided by the reduce runner.
///
/// # Outputs
///
/// An empty result used for returning an error. Outputs of the reduce operation are sent out
/// through the `emitter`.
pub trait Reduce {
    type Key: Default + Serialize + DeserializeOwned;
    type Value: Default + Serialize + DeserializeOwned;
    fn reduce<E>(
        &self,
        input: IntermediateInputKV<Self::Key, Self::Value>,
        emitter: E,
    ) -> Result<()>
    where
        E: EmitFinal<Self::Value>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use emitter::FinalVecEmitter;

    struct TestReducer;
    impl Reduce for TestReducer {
        type Key = String;
        type Value = String;
        fn reduce<E>(
            &self,
            input: IntermediateInputKV<Self::Key, Self::Value>,
            mut emitter: E,
        ) -> Result<()>
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
        let test_kv = IntermediateInputKV::new("test_vector".to_owned(), test_vector);
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

        let test_kv = IntermediateInputKV::new("test_vector".to_owned(), test_vector);

        assert_eq!("foo", test_kv.values[0]);
        assert_eq!("bar", test_kv.values[1]);
    }
}
