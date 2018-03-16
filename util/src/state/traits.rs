use std::result;

use error_chain::ChainedError;
use serde_json;

/// The `StateHandling` trait defines an object that can have it's state saved
/// and subsequently loaded from a file.
pub trait StateHandling<E>
where
    E: ChainedError,
{
    // Creates a new object from the JSON data provided.
    fn new_from_json(data: serde_json::Value) -> result::Result<Self, E>
    where
        Self: Sized;

    // Returns a JSON representation of the object.
    fn dump_state(&self) -> result::Result<serde_json::Value, E>;

    // Updates the object to match the JSON state provided.
    fn load_state(&mut self, data: serde_json::Value) -> result::Result<(), E>;
}

/// The `SimpleStateHandling` trait defines an object that can have it's state saved
/// and subsequently loaded from a file but doesn't have a new from json method. A mutable
/// reference is not needed to load it's state.
pub trait SimpleStateHandling<E>
where
    E: ChainedError,
{
    // Returns a JSON representation of the object.
    fn dump_state(&self) -> result::Result<serde_json::Value, E>;

    // Updates the object to match the JSON state provided.
    fn load_state(&self, data: serde_json::Value) -> result::Result<(), E>;
}
