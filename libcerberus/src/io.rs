use errors::*;
use mapper::MapInputKV;
use reducer::ReduceInputKV;
use serde::Serialize;
use serde_json;
use serialise::{FinalOutputObject, IntermediateOutputObject};
use std::io::{Read, Write};

/// `read_map_input` reads a string from a source and returns a `MapInputKV`.
///
/// It attempts to parse the string from the input source as JSON and returns an `errors::Error` if
/// the attempt fails.
pub fn read_map_input<R: Read>(source: &mut R) -> Result<MapInputKV> {
    let mut input_string = String::new();
    let bytes_read = source.read_to_string(&mut input_string).chain_err(
        || "Error reading from source.",
    )?;
    if bytes_read == 0 {
        error!("bytes_read is 0");
    }
    let result = serde_json::from_str(input_string.as_str()).chain_err(
        || "Error parsing input JSON to MapInputKV.",
    )?;
    Ok(result)
}

/// `read_reduce_input` reads a string from a source and returns a `ReduceInputKV`.
///
/// It attempts to parse the string from the input source as JSON and returns an `errors::Error` if
/// the attempt fails.
pub fn read_reduce_input<R: Read>(source: &mut R) -> Result<ReduceInputKV> {
    let mut input_string = String::new();
    let bytes_read = source.read_to_string(&mut input_string).chain_err(
        || "Error reading from source.",
    )?;
    if bytes_read == 0 {
        warn!("bytes_read is 0");
    }
    let result = serde_json::from_str(input_string.as_str()).chain_err(
        || "Error parsing input JSON to ReduceInputKV.",
    )?;
    Ok(result)
}

/// `write_map_output` attempts to serialise an `IntermediateOutputObject` to a given sink.
pub fn write_map_output<W, K, V>(
    sink: &mut W,
    output: &IntermediateOutputObject<K, V>,
) -> Result<()>
where
    W: Write,
    K: Serialize,
    V: Serialize,
{
    serde_json::to_writer(sink, &output).chain_err(
        || "Error writing to sink.",
    )?;
    Ok(())
}

/// `write_reduce_output` attempts to serialise a `FinalOutputObject` to a given sink.
pub fn write_reduce_output<W, V>(sink: &mut W, output: &FinalOutputObject<V>) -> Result<()>
where
    W: Write,
    V: Serialize,
{

    serde_json::to_writer(sink, &output).chain_err(
        || "Error writing to sink.",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use serialise::IntermediateOutputPair;
    use std::io::Cursor;
    use super::*;

    #[test]
    fn read_valid_map_input_kv() {
        let test_string = r#"{"key":"foo","value":"bar"}"#;
        let mut cursor = Cursor::new(test_string);
        let expected_result = MapInputKV {
            key: "foo".to_owned(),
            value: "bar".to_owned(),
        };

        let result = read_map_input(&mut cursor).unwrap();

        assert_eq!(expected_result, result);
    }

    #[test]
    #[should_panic]
    fn read_invalid_map_input_kv() {
        let test_string = "";
        let mut cursor = Cursor::new(test_string);

        read_map_input(&mut cursor).unwrap();
    }

    #[test]
    fn read_valid_reduce_input_kv() {
        let test_string = r#"{"key":"foo","values":["bar","baz"]}"#;
        let mut cursor = Cursor::new(test_string);
        let expected_result = ReduceInputKV {
            key: "foo".to_owned(),
            values: vec!["bar".to_owned(), "baz".to_owned()],
        };

        let result = read_reduce_input(&mut cursor).unwrap();

        assert_eq!(expected_result, result);
    }

    #[test]
    #[should_panic]
    fn read_invalid_reduce_input_kv() {
        let test_string = "";
        let mut cursor = Cursor::new(test_string);

        read_reduce_input(&mut cursor).unwrap();
    }

    #[test]
    fn write_intermediate_output_object() {
        let test_object = IntermediateOutputObject {
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
        let output_vector: Vec<u8> = Vec::new();
        let mut cursor = Cursor::new(output_vector);

        write_map_output(&mut cursor, &test_object).unwrap();

        let output_string = String::from_utf8(cursor.into_inner()).unwrap();
        assert_eq!(expected_json_string, output_string);
    }

    #[test]
    fn write_final_output_object() {
        let test_object = FinalOutputObject { values: vec!["barbaz", "bazbar"] };
        let expected_json_string = r#"{"values":["barbaz","bazbar"]}"#;
        let output_vector: Vec<u8> = Vec::new();
        let mut cursor = Cursor::new(output_vector);

        write_reduce_output(&mut cursor, &test_object).unwrap();

        let output_string = String::from_utf8(cursor.into_inner()).unwrap();
        assert_eq!(expected_json_string, output_string);
    }
}
