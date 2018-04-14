use serde::de::DeserializeOwned;
use serde::Serialize;

use emitter::EmitFinal;
use errors::*;
use intermediate::IntermediateInputKV;

/// The `Combine` trait defines a function for performing a combine operation.
///
/// The output types are decided by the implementation of this trait.
///
/// # Arguments
///
/// * `input`   - A `IntermediateInputKV` containing the input data for the combine operation.
/// * `emitter` - A struct implementing the `EmitFinal` trait,
///               provided by the combine runner.
///
/// # Outputs
///
/// An empty result used for returning an error. Outputs of the reduce operation are sent out
/// through the `emitter`.
pub trait Combine<K, V>
where
    K: Default + Serialize + DeserializeOwned,
    V: Default + Serialize + DeserializeOwned,
{
    fn combine<E>(&self, input: IntermediateInputKV<K, V>, emitter: E) -> Result<()>
    where
        E: EmitFinal<V>;
}
