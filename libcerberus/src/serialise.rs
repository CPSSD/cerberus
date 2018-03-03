use std::collections::HashMap;

use serde::Serialize;

use emitter::{EmitIntermediate, EmitFinal, EmitPartitionedIntermediate};
use errors::*;

/// `IntermediateOutputPair` is a struct representing an intermediate key-value pair as outputted
/// from a map operation.
#[derive(Debug, Default, PartialEq, Serialize)]
pub struct IntermediateOutputPair<K, V>
where
    K: Default + Serialize,
    V: Default + Serialize,
{
    pub key: K,
    pub value: V,
}

/// `IntermediateOutputObject` is a struct comprising a collection of `IntermediateOutputArray`s,
/// representing a partition of the output of a map operation, ready to be serialised to JSON.
#[derive(Debug, Default, PartialEq, Serialize)]
pub struct IntermediateOutputObject<K, V>
where
    K: Default + Serialize,
    V: Default + Serialize,
{
    pub partitions: HashMap<u64, Vec<IntermediateOutputPair<K, V>>>,
}

/// `FinalOutputObject` is a struct comprising a collection of serialisable values representing the
/// entire output of a reduce operation, ready to be serialised to JSON.
#[derive(Debug, Default, PartialEq, Serialize)]
pub struct FinalOutputObject<V: Default + Serialize> {
    pub values: Vec<V>,
}

/// A struct implementing `EmitIntermediate` which emits to an `IntermediateOutputPair`.
pub struct IntermediateOutputPairEmitter<'a, K, V>
where
    K: Default + Serialize + 'a,
    V: Default + Serialize + 'a,
{
    sink: &'a mut IntermediateOutputPair<K, V>,
}

impl<'a, K, V> IntermediateOutputPairEmitter<'a, K, V>
where
    K: Default + Serialize,
    V: Default + Serialize,
{
    /// Constructs a new `IntermediateOutputPairEmitter` with a mutable reference to a given
    /// `IntermediateOutputPair`.
    ///
    /// # Arguments
    ///
    /// * `sink` - A mutable reference to the `IntermediateOutputPair`
    /// to receive the emitted values.
    pub fn new(sink: &'a mut IntermediateOutputPair<K, V>) -> Self {
        IntermediateOutputPairEmitter { sink: sink }
    }
}

impl<'a, K, V> EmitIntermediate<K, V> for IntermediateOutputPairEmitter<'a, K, V>
where
    K: Default + Serialize,
    V: Default + Serialize,
{
    fn emit(&mut self, key: K, value: V) -> Result<()> {
        self.sink.key = key;
        self.sink.value = value;
        Ok(())
    }
}

/// A struct implementing `EmitPartitionedIntermediate`
/// which emits to an `IntermediateOutputObject`.
pub struct IntermediateOutputObjectEmitter<'a, K, V>
where
    K: Default + Serialize + 'a,
    V: Default + Serialize + 'a,
{
    sink: &'a mut IntermediateOutputObject<K, V>,
}

impl<'a, K, V> IntermediateOutputObjectEmitter<'a, K, V>
where
    K: Default + Serialize,
    V: Default + Serialize,
{
    /// Constructs a new `IntermediateOutputObjectEmitter` with a mutable reference to a given
    /// `IntermediateOutputObject`.
    ///
    /// # Arguments
    ///
    /// * `sink` - A mutable reference to the `IntermediateOutputObject`
    /// to receive the emitted values.
    pub fn new(sink: &'a mut IntermediateOutputObject<K, V>) -> Self {
        IntermediateOutputObjectEmitter { sink: sink }
    }
}

impl<'a, K, V> EmitPartitionedIntermediate<K, V> for IntermediateOutputObjectEmitter<'a, K, V>
where
    K: Default + Serialize,
    V: Default + Serialize,
{
    fn emit(&mut self, partition: u64, key: K, value: V) -> Result<()> {
        let output_array = self.sink.partitions.entry(partition).or_insert_with(Default::default);
        output_array.push(IntermediateOutputPair {
            key: key,
            value: value,
        });
        Ok(())
    }
}

/// A struct implementing `EmitFinal` which emits to a `FinalOutputObject`.
pub struct FinalOutputObjectEmitter<'a, V: Default + Serialize + 'a> {
    sink: &'a mut FinalOutputObject<V>,
}

impl<'a, V: Default + Serialize> FinalOutputObjectEmitter<'a, V> {
    /// Constructs a new `FinalOutputObjectEmitter` with a mutable reference to a given
    /// `FinalOutputObject`.
    ///
    /// # Arguments
    ///
    /// * `sink` - A mutable reference to the `FinalOutputObject` to receive the emitted
    /// values.
    pub fn new(sink: &'a mut FinalOutputObject<V>) -> Self {
        FinalOutputObjectEmitter { sink: sink }
    }
}

impl<'a, V: Default + Serialize> EmitFinal<V> for FinalOutputObjectEmitter<'a, V> {
    fn emit(&mut self, value: V) -> Result<()> {
        self.sink.values.push(value);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json;
    use std::collections::HashSet;
    use super::*;

    // Test that the JSON serialisation of IntermediateOutputObject matches the libcerberus JSON
    // API.
    #[test]
    fn intermediate_output_object_json_format() {
        let mut partitions = HashMap::new();
        partitions.insert(
            0,
            vec![
                IntermediateOutputPair {
                    key: "foo_intermediate",
                    value: "bar",
                },
                IntermediateOutputPair {
                    key: "foo_intermediate",
                    value: "baz",
                },
            ],
        );
        partitions.insert(
            1,
            vec![
                IntermediateOutputPair {
                    key: "foo_intermediate2",
                    value: "bar",
                },
            ],
        );

        let output = IntermediateOutputObject { partitions: partitions };
        let mut output_set = HashSet::new();
        let expected_string1 =
            r#"{"partitions":{"0":[{"key":"foo_intermediate","value":"bar"},{"key":"foo_intermediate","value":"baz"}],"1":[{"key":"foo_intermediate2","value":"bar"}]}}"#;
        let expected_string2 =
            r#"{"partitions":{"1":[{"key":"foo_intermediate2","value":"bar"}],"0":[{"key":"foo_intermediate","value":"bar"},{"key":"foo_intermediate","value":"baz"}]}}"#;
        output_set.insert(expected_string1.to_owned());
        output_set.insert(expected_string2.to_owned());

        let json_string = serde_json::to_string(&output).unwrap();

        assert!(output_set.contains(&json_string))
    }

    // Test that the JSON serialisation of FinalOutputObject matches the libcerberus JSON API.
    #[test]
    fn final_output_object_json_format() {
        let output = FinalOutputObject { values: vec!["barbaz", "bazbar"] };
        let expected_json_string = r#"{"values":["barbaz","bazbar"]}"#;

        let json_string = serde_json::to_string(&output).unwrap();

        assert_eq!(expected_json_string, json_string);
    }

    #[test]
    fn intermediate_output_emitter_works() {
        let mut output = IntermediateOutputObject::default();
        let mut partitions = HashMap::new();
        partitions.insert(
            0,
            vec![
                IntermediateOutputPair {
                    key: "foo",
                    value: "bar",
                },
            ],
        );

        let expected_output = IntermediateOutputObject { partitions: partitions };

        {
            let mut emitter = IntermediateOutputObjectEmitter::new(&mut output);
            emitter.emit(0, "foo", "bar").unwrap();
        }

        assert_eq!(expected_output, output);
    }

    #[test]
    fn final_output_emitter_works() {
        let mut output = FinalOutputObject::default();
        let expected_output = FinalOutputObject { values: vec!["foo", "bar"] };

        {
            let mut emitter = FinalOutputObjectEmitter::new(&mut output);
            emitter.emit("foo").unwrap();
            emitter.emit("bar").unwrap();
        }

        assert_eq!(expected_output, output);
    }
}
