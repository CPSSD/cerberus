use emitter::Emit;
use errors::*;
use serde::Serialize;

/// The `Map` trait defines a function for performing a map operation.
///
/// The output types are decided by the implementation of this trait.
///
/// # Arguments
///
/// * `input` - The input data for the map operation.
/// * `emitter` - A struct implementing the `Emit` trait, provided by the map runner.
///
/// # Outputs
///
/// An empty result used for returning an error. Outputs of the map operation are sent out through
/// the `emitter`.
pub trait Map {
    type Key: Serialize;
    type Value: Serialize;
    fn map<S, E>(input: S, emitter: E) -> Result<()>
    where
        S: Into<String>,
        E: Emit<Self::Key, Self::Value>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use emitter::VecEmitter;

    struct TestMapper;
    impl Map for TestMapper {
        type Key = String;
        type Value = String;
        fn map<S, E>(input: S, mut emitter: E) -> Result<()>
        where
            S: Into<String>,
            E: Emit<Self::Key, Self::Value>,
        {
            emitter.emit(input.into(), "test".to_owned())?;
            Ok(())
        }
    }

    #[test]
    fn test_mapper_test_interface() {
        let mut vec: Vec<(String, String)> = Vec::new();

        TestMapper::map("this is a", VecEmitter::new(&mut vec)).unwrap();

        assert_eq!("this is a", vec[0].0);
        assert_eq!("test", vec[0].1);
    }

    #[test]
    fn test_mapper_with_associated_types() {
        let mut vec: Vec<(<TestMapper as Map>::Key, <TestMapper as Map>::Value)> = Vec::new();

        TestMapper::map("this is a", VecEmitter::new(&mut vec)).unwrap();

        assert_eq!("this is a", vec[0].0);
        assert_eq!("test", vec[0].1);
    }
}
