use errors::*;
use multimap::MultiMap;
use serde::Serialize;
use std::cmp::Eq;
use std::hash::Hash;

/// The Emit trait specifies structs which can key-value pairs to an in-memory data structure.
///
/// Since these in-memory data structures will eventually be serialised to disk, they must
/// implement the `serde::Serialize` trait.
pub trait Emit {
    type Key: Serialize;
    type Value: Serialize;

    /// Takes ownership of a key-value pair and moves it somewhere else.
    ///
    /// Returns an empty `Result` used for error handling.
    fn emit(&mut self, key: Self::Key, value: Self::Value) -> Result<()>;
}

/// A struct implementing `Emit` which emits to a `multimap::MultiMap`.
pub struct MultiMapEmitter<'a, K: 'a, V: 'a>
where
    K: Serialize + Eq + Hash,
    V: Serialize + Eq,
{
    sink: &'a mut MultiMap<K, V>,
}

impl<'a, K, V> MultiMapEmitter<'a, K, V>
where
    K: Serialize + Eq + Hash,
    V: Serialize + Eq,
{
    /// Constructs a new `MultiMapEmitter` with a mutable reference to a given MultiMap.
    ///
    /// # Arguments
    ///
    /// * `sink` - A mutable reference to the `MultiMap` to receive the emitted values.
    pub fn new(sink: &'a mut MultiMap<K, V>) -> Self {
        MultiMapEmitter { sink: sink }
    }
}

impl<'a, K, V> Emit for MultiMapEmitter<'a, K, V>
where
    K: Serialize + Eq + Hash,
    V: Serialize + Eq,
{
    type Key = K;
    type Value = V;

    fn emit(&mut self, key: Self::Key, value: Self::Value) -> Result<()> {
        self.sink.insert(key, value);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multimap_emitter_with_string_string() {
        let mut map: MultiMap<String, String> = MultiMap::new();

        {
            let mut emitter: MultiMapEmitter<String, String> = MultiMapEmitter::new(&mut map);
            emitter.emit("foo".to_owned(), "bar".to_owned()).unwrap();
        }

        let pair = map.into_iter().next().unwrap();
        assert_eq!("foo", pair.0);
        assert_eq!("bar", pair.1[0]);
    }

    #[test]
    fn multimap_emitter_with_duplicate_keys() {
        let mut map: MultiMap<u16, u16> = MultiMap::new();

        {
            let mut emitter: MultiMapEmitter<u16, u16> = MultiMapEmitter::new(&mut map);
            emitter.emit(0xDEAD, 0xBEEF).unwrap();
            emitter.emit(0xDEAD, 0xBABE).unwrap();
        }

        let mut pair = map.into_iter().next().unwrap();
        pair.1.sort();
        let expected_values = vec![0xBABE, 0xBEEF];
        assert_eq!(0xDEAD, pair.0);
        assert_eq!(expected_values.as_slice(), pair.1.as_slice());
    }
}
