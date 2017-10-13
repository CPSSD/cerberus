use errors::*;
use mapper::MapInputKV;
use reducer::ReduceInputKV;
use serde_json;
use std::io::Read;

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
        warn!("bytes_read is 0");
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

#[cfg(test)]
mod tests {
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
}
