use emitter::EmitIntermediate;
use errors::*;
use serde::Serialize;

/// The `MapInputKV` is a struct for passing input data to a `Map`.
///
/// `MapInputKV` is a thin wrapper around a `(String, String)`, used for creating a clearer API.
/// It can be constructed normally or using `MapInputKV::new()`.
#[derive(Debug, Default)]
pub struct MapInputKV {
    pub key: String,
    pub value: String,
}

impl MapInputKV {
    pub fn new(key: String, value: String) -> Self {
        MapInputKV {
            key: key,
            value: value,
        }
    }
}

/// The `Map` trait defines a function for performing a map operation.
///
/// The output types are decided by the implementation of this trait.
///
/// # Arguments
///
/// * `input` - A `MapInputKV` containing the input data for the map operation.
/// * `emitter` - A struct implementing the `EmitIntermediate` trait, provided by the map runner.
///
/// # Outputs
///
/// An empty result used for returning an error. Outputs of the map operation are sent out through
/// the `emitter`.
pub trait Map {
    type Key: Serialize;
    type Value: Serialize;
    fn map<E>(input: MapInputKV, emitter: E) -> Result<()>
    where
        E: EmitIntermediate<Self::Key, Self::Value>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use emitter::IntermediateVecEmitter;

    struct TestMapper;
    impl Map for TestMapper {
        type Key = String;
        type Value = String;
        fn map<E>(input: MapInputKV, mut emitter: E) -> Result<()>
        where
            E: EmitIntermediate<Self::Key, Self::Value>,
        {
            emitter.emit(input.value, "test".to_owned())?;
            Ok(())
        }
    }

    #[test]
    fn test_mapper_test_interface() {
        let mut vec: Vec<(String, String)> = Vec::new();
        let test_input = MapInputKV::new("test_key".to_owned(), "this is a".to_owned());

        TestMapper::map(test_input, IntermediateVecEmitter::new(&mut vec)).unwrap();

        assert_eq!("this is a", vec[0].0);
        assert_eq!("test", vec[0].1);
    }

    #[test]
    fn test_mapper_with_associated_types() {
        let mut vec: Vec<(<TestMapper as Map>::Key, <TestMapper as Map>::Value)> = Vec::new();
        let test_input = MapInputKV::new("test_key".to_owned(), "this is a".to_owned());

        TestMapper::map(test_input, IntermediateVecEmitter::new(&mut vec)).unwrap();

        assert_eq!("this is a", vec[0].0);
        assert_eq!("test", vec[0].1);
    }
}
