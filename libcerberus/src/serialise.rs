use std::collections::HashMap;

use serde::Serialize;

use emitter::EmitFinal;
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

/// A struct implementing `EmitFinal` which emits to a `Vec` of intermediate values.
pub struct VecEmitter<'a, V>
where
    V: Default + Serialize + 'a,
{
    sink: &'a mut Vec<V>,
}

impl<'a, V> VecEmitter<'a, V>
where
    V: Default + Serialize,
{
    /// Constructs a new `VecEmitter` with a mutable reference to a given `Vec`.
    ///
    /// # Arguments
    ///
    /// * `sink` - A mutable reference to the `Vec`
    /// to receive the emitted values.
    pub fn new(sink: &'a mut Vec<V>) -> Self {
        VecEmitter { sink }
    }
}

impl<'a, V> EmitFinal<V> for VecEmitter<'a, V>
where
    V: Default + Serialize,
{
    fn emit(&mut self, value: V) -> Result<()> {
        self.sink.push(value);
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
        FinalOutputObjectEmitter { sink }
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
    use super::*;
    use serde_json;
    use std::collections::HashSet;

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
            vec![IntermediateOutputPair {
                key: "foo_intermediate2",
                value: "bar",
            }],
        );

        let output = IntermediateOutputObject { partitions };
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
        let output = FinalOutputObject {
            values: vec!["barbaz", "bazbar"],
        };
        let expected_json_string = r#"{"values":["barbaz","bazbar"]}"#;

        let json_string = serde_json::to_string(&output).unwrap();

        assert_eq!(expected_json_string, json_string);
    }

    #[test]
    fn final_output_emitter_works() {
        let mut output = FinalOutputObject::default();
        let expected_output = FinalOutputObject {
            values: vec!["foo", "bar"],
        };

        {
            let mut emitter = FinalOutputObjectEmitter::new(&mut output);
            emitter.emit("foo").unwrap();
            emitter.emit("bar").unwrap();
        }

        assert_eq!(expected_output, output);
    }
}
