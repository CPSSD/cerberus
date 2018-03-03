use serde::Serialize;

/// The `IntermediateInputKV` is a struct for passing input data to a `Reduce` or `Combine`.
///
/// `IntermediateInputKV` is a thin wrapper around a `(String, Vec<Value>)`,
/// used for creating a clearer API.
/// It can be constructed normally or using `IntermediateInputKV::new()`.
#[derive(Debug, Default, Deserialize, PartialEq)]
pub struct IntermediateInputKV<V>
where
    V: Default + Serialize,
{
    pub key: String,
    pub values: Vec<V>,
}

impl<V> IntermediateInputKV<V>
where
    V: Default + Serialize,
{
    pub fn new(key: String, values: Vec<V>) -> Self {
        IntermediateInputKV {
            key: key,
            values: values,
        }
    }
}
