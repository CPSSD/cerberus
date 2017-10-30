use std::collections::HashMap;

use emitter::{EmitIntermediate, EmitFinal};
use errors::*;
use serde::Serialize;

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

/// `IntermediateOutputArray` is a struct comprising a collection of `IntermediateOutputPair`s,
/// representing a partition of the output of a map operation, ready to be serialised to JSON.
#[derive(Debug, Default, PartialEq, Serialize)]
pub struct IntermediateOutputArray<K, V>
where
    K: Default + Serialize,
    V: Default + Serialize,
{
    pub pairs: Vec<IntermediateOutputPair<K, V>>,
}

pub struct IntermediateOutputObject<K, V>
where
    K: Default + Serialize,
    V: Default + Serialize,
{
    pub partitions: HashMap<u32, IntermediateOutputArray<K, V>>,
}

/// `FinalOutputObject` is a struct comprising a collection of serialisable values representing the
/// entire output of a reduce operation, ready to be serialised to JSON.
#[derive(Debug, Default, PartialEq, Serialize)]
pub struct FinalOutputObject<V: Default + Serialize> {
    pub values: Vec<V>,
}

/// A struct implementing `EmitIntermediate` which emits to an `IntermediateOutputObject`.
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
    /// * `sink` - A mutable reference to the `IntermediateOutputObject` to receive the emitted values.
    pub fn new(sink: &'a mut IntermediateOutputObject<K, V>) -> Self {
        IntermediateOutputObjectEmitter { sink: sink }
    }
}

impl<'a, K, V> EmitIntermediate<K, V> for IntermediateOutputObjectEmitter<'a, K, V>
where
    K: Default + Serialize,
    V: Default + Serialize,
{
    fn emit(&mut self, key: K, value: V) -> Result<()> {
        self.sink.pairs.push(IntermediateOutputPair {
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
    use super::*;

    // Test that the JSON serialisation of IntermediateOutputObject matches the libcerberus JSON
    // API.
    #[test]
    fn intermediate_output_object_json_format() {
        let output = IntermediateOutputObject {
            pairs: vec![
                IntermediateOutputPair {
                    key: "foo_intermediate",
                    value: "bar",
                },
                IntermediateOutputPair {
                    key: "foo_intermediate",
                    value: "baz",
                },
            ],
        };
        let expected_json_string =
            r#"{"pairs":[{"key":"foo_intermediate","value":"bar"},{"key":"foo_intermediate","value":"baz"}]}"#;

        let json_string = serde_json::to_string(&output).unwrap();

        assert_eq!(expected_json_string, json_string)
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
        let expected_output = IntermediateOutputObject {
            pairs: vec![
                IntermediateOutputPair {
                    key: "foo",
                    value: "bar",
                },
            ],
        };

        {
            let mut emitter = IntermediateOutputObjectEmitter::new(&mut output);
            emitter.emit("foo", "bar").unwrap();
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
