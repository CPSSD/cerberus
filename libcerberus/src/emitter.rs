use errors::*;
use multimap::MultiMap;
use serde::Serialize;
use std::cmp::Eq;
use std::hash::Hash;

/// The `Emit` trait specifies structs which can send key-value pairs to an in-memory data structure.
///
/// Since these in-memory data structures will eventually be serialised to disk, they must
/// implement the `serde::Serialize` trait.
pub trait Emit<K: Serialize, V: Serialize> {
    /// Takes ownership of a key-value pair and moves it somewhere else.
    ///
    /// Returns an empty `Result` used for error handling.
    fn emit(&mut self, key: K, value: V) -> Result<()>;
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
    /// Constructs a new `MultiMapEmitter` with a mutable reference to a given `MultiMap`.
    ///
    /// # Arguments
    ///
    /// * `sink` - A mutable reference to the `MultiMap` to receive the emitted values.
    pub fn new(sink: &'a mut MultiMap<K, V>) -> Self {
        MultiMapEmitter { sink: sink }
    }
}

impl<'a, K, V> Emit<K, V> for MultiMapEmitter<'a, K, V>
where
    K: Serialize + Eq + Hash,
    V: Serialize + Eq,
{
    fn emit(&mut self, key: K, value: V) -> Result<()> {
        self.sink.insert(key, value);
        Ok(())
    }
}
/// A struct implementing `Emit` which emits to a `std::vec::Vec`.
///
/// Unlike the `MultiMapEmitter`, this does not automatically group together duplicate keys.
pub struct VecEmitter<'a, K: 'a, V: 'a>
where
    K: Serialize,
    V: Serialize,
{
    sink: &'a mut Vec<(K, V)>,
}

impl<'a, K, V> VecEmitter<'a, K, V>
where
    K: Serialize,
    V: Serialize,
{
    /// Constructs a new `VecEmitter` with a mutable reference to a given `Vec`.
    ///
    /// # Arguments
    ///
    /// * `sink` - A mutable reference to the `Vec` to receive the emitted values.
    pub fn new(sink: &'a mut Vec<(K, V)>) -> Self {
        VecEmitter { sink: sink }
    }
}

impl<'a, K, V> Emit<K, V> for VecEmitter<'a, K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn emit(&mut self, key: K, value: V) -> Result<()> {
        self.sink.push((key, value));
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

    #[test]
    fn vec_emitter_with_string_string() {
        let mut vec: Vec<(String, String)> = Vec::new();

        {
            let mut emitter: VecEmitter<String, String> = VecEmitter::new(&mut vec);
            emitter.emit("foo".to_owned(), "bar".to_owned()).unwrap();
        }

        assert_eq!("foo", vec[0].0);
        assert_eq!("bar", vec[0].1);
    }

    #[test]
    fn vec_emitter_with_duplicate_keys() {
        let mut vec: Vec<(u16, u16)> = Vec::new();

        {
            let mut emitter: VecEmitter<u16, u16> = VecEmitter::new(&mut vec);
            emitter.emit(0xDEAD, 0xBEEF).unwrap();
            emitter.emit(0xDEAD, 0xBABE).unwrap();
        }

        assert_eq!((0xDEAD, 0xBEEF), vec[0]);
        assert_eq!((0xDEAD, 0xBABE), vec[1]);
    }
}
