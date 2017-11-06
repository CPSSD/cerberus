use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use serde::Serialize;

use emitter::EmitPartitionedIntermediate;
use errors::*;

/// The `PartitionInputPairs` is a struct for passing input data to a `Partition`.
///
/// `PartitionInputPairs` is a thin wrapper around a `Vec<(Key, Value)>`, used for creating a clearer API.
/// It can be constructed normally or using `PartitionInputPairs::new()`.
#[derive(Debug, Default, Deserialize, PartialEq)]
pub struct PartitionInputPairs<K, V>
where
    K: Default + Serialize,
    V: Default + Serialize,
{
    pub pairs: Vec<(K, V)>,
}

impl<K, V> PartitionInputPairs<K, V>
where
    K: Default + Serialize,
    V: Default + Serialize,
{
    pub fn new(pairs: Vec<(K, V)>) -> Self {
        PartitionInputPairs { pairs: pairs }
    }
}

/// The `Partition` trait defines a function for partitioning the results of a `Map` operation..
///
/// # Arguments
///
/// * `input` - A `Vec` containing the output pairs of a map operation.
/// * `emitter` - A struct implementing the `EmitPartitionedIntermediate` trait, provided by the map runner.
///
/// # Outputs
///
/// An empty result used for returning an error. Outputs of the map operation are sent out through
/// the `emitter`.
pub trait Partition<K, V>
where
    K: Default + Serialize,
    V: Default + Serialize,
{
    fn partition<E>(&self, input: PartitionInputPairs<K, V>, emitter: E) -> Result<()>
    where
        E: EmitPartitionedIntermediate<K, V>;
}

/// `HashPartitioner` implements the `Partition` for any Key that can be hashed.
pub struct HashPartitioner {
    partition_count: u64,
}

impl HashPartitioner {
    pub fn new(partition_count: u64) -> Self {
        HashPartitioner { partition_count: partition_count }
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
    fn partition<E>(&self, input: PartitionInputPairs<K, V>, mut emitter: E) -> Result<()>
    where
        E: EmitPartitionedIntermediate<K, V>,
    {
        for (key, value) in input.pairs {
            let hash: u64 = self.calculate_hash(&key);
            let partition_count: u64 = self.partition_count;
            let partition = hash % partition_count;
            emitter.emit(partition, key, value).chain_err(
                || "Error partitioning map output.",
            )?;
        }
        Ok(())
    }
}
