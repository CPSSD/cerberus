use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use serde::Serialize;

use errors::*;

/// The `PartitionInputKV` is a struct for passing input data to a `Partition`.
///
/// `PartitionInputKV` is a thin wrapper around a `(Key, Value)`,
/// used for creating a clearer API.
/// It can be constructed normally or using `PartitionInputKV::new()`.
#[derive(Debug, PartialEq)]
pub struct PartitionInputKV<'a, K, V>
where
    K: Default + Serialize + 'a,
    V: Default + Serialize + 'a,
{
    pub key: &'a K,
    pub value: &'a V,
}

impl<'a, K, V> PartitionInputKV<'a, K, V>
where
    K: Default + Serialize + 'a,
    V: Default + Serialize + 'a,
{
    pub fn new(key: &'a K, value: &'a V) -> Self {
        PartitionInputKV { key, value }
    }
}

/// The `Partition` trait defines a function for partitioning the results of a `Map` operation..
///
/// # Arguments
///
/// * `input` - A `ParitionInputKV` containing an output pair of a map operation.
///
/// # Outputs
///
/// A Result<u64>, representing the output partition for the given key and value.
pub trait Partition<K, V>
where
    K: Default + Serialize,
    V: Default + Serialize,
{
    fn partition(&self, input: PartitionInputKV<K, V>) -> Result<u64>;
}

/// `HashPartitioner` implements the `Partition` for any Key that can be hashed.
pub struct HashPartitioner {
    partition_count: u64,
}

impl HashPartitioner {
    pub fn new(partition_count: u64) -> Self {
        HashPartitioner { partition_count }
    }

    fn calculate_hash<T: Hash>(&self, t: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        t.hash(&mut hasher);
        hasher.finish()
    }
}

impl<K, V> Partition<K, V> for HashPartitioner
where
    K: Default + Serialize + Hash,
    V: Default + Serialize,
{
    fn partition(&self, input: PartitionInputKV<K, V>) -> Result<u64> {
        let hash: u64 = self.calculate_hash(input.key);
        let partition_count: u64 = self.partition_count;
        let partition = hash % partition_count;
        Ok(partition)
    }
}
