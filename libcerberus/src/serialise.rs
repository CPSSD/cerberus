use serde::Serialize;

/// `IntermediateOutputPair` is a struct representing an intermediate key-value pair as outputted
/// from a map operation.
#[derive(Serialize)]
pub struct IntermediateOutputPair<K: Serialize, V: Serialize> {
    pub key: K,
    pub value: V,
}

/// `IntermediateOutputObject` is a struct comprising a collection of `IntermediateOutputPair`s,
/// representing the entire output of a map operation, ready to be serialised to JSON.
#[derive(Serialize)]
pub struct IntermediateOutputObject<K: Serialize, V: Serialize> {
    pub pairs: Vec<IntermediateOutputPair<K, V>>,
}

/// `FinalOutputObject` is a struct comprising a collection of serialisable values representing the
/// entire output of a reduce operation, ready to be serialised to JSON.
#[derive(Serialize)]
pub struct FinalOutputObject<V: Serialize> {
    pub values: Vec<V>,
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
        let expected_json_string = "{\"pairs\":[{\"key\":\"foo_intermediate\",\
                                    \"value\":\"bar\"},{\"key\":\"foo_intermediate\",\
                                    \"value\":\"baz\"}]}";

        let json_string = serde_json::to_string(&output).unwrap();

        assert_eq!(expected_json_string, json_string)
    }

    // Test that the JSON serialisation of FinalOutputObject matches the libcerberus JSON API.
    #[test]
    fn final_output_object_json_format() {
        let output = FinalOutputObject { values: vec!["barbaz", "bazbar"] };
        let expected_json_string = "{\"values\":[\"barbaz\",\"bazbar\"]}";

        let json_string = serde_json::to_string(&output).unwrap();

        assert_eq!(expected_json_string, json_string);
    }
}
