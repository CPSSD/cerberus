use errors::*;
use serde::Serialize;

/// The `EmitIntermediate` trait specifies structs which can send key-value pairs to an in-memory
/// data structure.
///
/// `EmitIntermediate` is intended for use in `Map` operations, for emitting an intermediate
/// key-value pair. Since these in-memory data structures will eventually be serialised to disk,
/// they must implement the `serde::Serialize` trait.
pub trait EmitIntermediate<K: Serialize, V: Serialize> {
    /// Takes ownership of a key-value pair and stores it in a sink.
    ///
    /// Returns an empty `Result` used for error handling.
    fn emit(&mut self, key: K, value: V) -> Result<()>;
}

/// The `EmitPartitionedIntermediate` trait specifies structs which can send partitioned key-value
/// pairs to an in-memory data structure.
///
/// `EmitPartitionedIntermediate` is intended for use by the `MapPartitioner` during the key
/// partitioning phase, for emitting key value pairs in their coresponding partition.
/// Since these in-memory data structures will eventually be serialised to disk,
/// they must implement the `serde::Serialize` trait.
pub trait EmitPartitionedIntermediate<K: Serialize, V: Serialize> {
    /// Takes ownership of a key-value pair and stores it in a sink based on its partition.
    ///
    /// Returns an empty `Result` used for error handling.
    fn emit(&mut self, partition: u64, key: K, value: V) -> Result<()>;
}

/// The `EmitFinal` trait specifies structs which can send values to an in-memory data structure.
///
/// `EmitFinal` is intended for use in `Reduce` operations, for emitting an intermediate key-value
/// pair. Since these in-memory data structures will eventually be serialised to disk, they must
/// implement the `serde::Serialize` trait.
pub trait EmitFinal<V: Serialize> {
    /// Takes ownership of a value and stores it in a sink.
    ///
    /// Returns an empty `Result` used for error handling.
    fn emit(&mut self, value: V) -> Result<()>;
}

/// A struct implementing `EmitFinal` which emits to a `std::vec::Vec`.
pub struct FinalVecEmitter<'a, V: Serialize + 'a> {
    sink: &'a mut Vec<V>,
}

impl<'a, V: Serialize> FinalVecEmitter<'a, V> {
    /// Constructs a new `FinalVecEmitter` with a mutable reference to a given `Vec`.
    ///
    /// # Arguments
    ///
    /// * `sink` - A mutable reference to the `Vec` to receive the emitted values.
    pub fn new(sink: &'a mut Vec<V>) -> Self {
        FinalVecEmitter { sink: sink }
    }
}

impl<'a, V: Serialize> EmitFinal<V> for FinalVecEmitter<'a, V> {
    fn emit(&mut self, value: V) -> Result<()> {
        self.sink.push(value);
        Ok(())
    }
}

/// A struct implementing `EmitIntermediate` which emits to a `std::vec::Vec`.
pub struct IntermediateVecEmitter<'a, K, V>
where
    K: Serialize + 'a,
    V: Serialize + 'a,
{
    sink: &'a mut Vec<(K, V)>,
}

impl<'a, K, V> IntermediateVecEmitter<'a, K, V>
where
    K: Serialize,
    V: Serialize,
{
    /// Constructs a new `IntermediateVecEmitter` with a mutable reference to a given `Vec`.
    ///
    /// # Arguments
    ///
    /// * `sink` - A mutable reference to the `Vec` to receive the emitted values.
    pub fn new(sink: &'a mut Vec<(K, V)>) -> Self {
        IntermediateVecEmitter { sink: sink }
    }
}

impl<'a, K, V> EmitIntermediate<K, V> for IntermediateVecEmitter<'a, K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn emit(&mut self, key: K, value: V) -> Result<()> {
        self.sink.push((key, value));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intermediate_vec_emitter_with_string_string() {
        let mut vec: Vec<(String, String)> = Vec::new();

        {
            let mut emitter: IntermediateVecEmitter<String, String> =
                IntermediateVecEmitter::new(&mut vec);
            emitter.emit("foo".to_owned(), "bar".to_owned()).unwrap();
        }

        assert_eq!("foo", vec[0].0);
        assert_eq!("bar", vec[0].1);
    }

    #[test]
    fn intermediate_vec_emitter_with_duplicate_keys() {
        let mut vec: Vec<(u16, u16)> = Vec::new();

        {
            let mut emitter: IntermediateVecEmitter<u16, u16> =
                IntermediateVecEmitter::new(&mut vec);
            emitter.emit(0xDEAD, 0xBEEF).unwrap();
            emitter.emit(0xDEAD, 0xBABE).unwrap();
        }

        assert_eq!((0xDEAD, 0xBEEF), vec[0]);
        assert_eq!((0xDEAD, 0xBABE), vec[1]);
    }

    #[test]
    fn intermediate_vec_emitter_with_vec_inside_box() {
        let mut boxed_vec = Box::new(Vec::<(u16, u16)>::new());

        {
            let mut emitter: IntermediateVecEmitter<u16, u16> =
                IntermediateVecEmitter::new(&mut boxed_vec);
            emitter.emit(1337, 1338).unwrap();
        }

        assert_eq!(1337, boxed_vec[0].0);
        assert_eq!(1338, boxed_vec[0].1);
    }

    #[test]
    fn final_vec_emitter_emit_value() {
        let mut vec: Vec<String> = Vec::new();

        {
            let mut emitter: FinalVecEmitter<String> = FinalVecEmitter::new(&mut vec);
            emitter.emit("foo".to_owned()).unwrap();
        }

        assert_eq!("foo", vec[0]);
    }

    #[test]
    fn final_vec_emitter_with_vec_inside_box() {
        let mut boxed_vec = Box::new(Vec::<u16>::new());

        {
            let mut emitter: FinalVecEmitter<u16> = FinalVecEmitter::new(&mut boxed_vec);
            emitter.emit(1337).unwrap();
        }

        assert_eq!(1337, boxed_vec[0]);
    }
}
