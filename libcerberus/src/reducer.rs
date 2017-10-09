use emitter::EmitFinal;
use errors::*;
use serde::Serialize;

/// The `ReduceInputKV` is a struct for passing input data to a `Reduce`.
///
/// `ReduceInputKV` is a thin wrapper around a `(String, Vec<String>)`, used for creating a clearer API.
/// It can be constructed normally or using `ReduceInputKV::new()`.
#[derive(Debug, Default)]
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

pub trait Reduce {
    type Value: Serialize;
    fn reduce<E>(input: ReduceInputKV, emitter: E) -> Result<()>
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
        fn reduce<E>(input: ReduceInputKV, mut emitter: E) -> Result<()>
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

        TestReducer::reduce(test_kv, FinalVecEmitter::new(&mut sink)).unwrap();

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
