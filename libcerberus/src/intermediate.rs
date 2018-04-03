use serde::Serialize;

/// The `IntermediateInputKV` is a struct for passing input data to a `Reduce` or `Combine`.
///
/// `IntermediateInputKV` is a thin wrapper around a `(String, Vec<Value>)`,
/// used for creating a clearer API.
/// It can be constructed normally or using `IntermediateInputKV::new()`.
#[derive(Debug, Default, Deserialize, PartialEq)]
pub struct IntermediateInputKV<K, V>
where
    K: Default + Serialize,
    V: Default + Serialize,
{
    pub key: K,
    pub values: Vec<V>,
}

impl<K, V> IntermediateInputKV<K, V>
where
    K: Default + Serialize,
    V: Default + Serialize,
{
    pub fn new(key: K, values: Vec<V>) -> Self {
        IntermediateInputKV { key, values }
    }
}
