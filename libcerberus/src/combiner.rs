use emitter::EmitIntermediate;
use errors::*;
use intermediate::IntermediateInputKV;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// The `Combine` trait defines a function for performing a combine operation.
///
/// The output types are decided by the implementation of this trait.
///
/// # Arguments
///
/// * `input`   - A `IntermediateInputKV` containing the input data for the combine operation.
/// * `emitter` - A struct implementing the `EmitIntermediate` trait,
///               provided by the combine runner.
///
/// # Outputs
///
/// An empty result used for returning an error. Outputs of the reduce operation are sent out
/// through the `emitter`.
pub trait Combine<V>
where
    V: Default + Serialize + DeserializeOwned,
{
    fn combine<E>(&self, input: IntermediateInputKV<V>, emitter: E) -> Result<()>
    where
        E: EmitIntermediate<String, V>;
}

// A null implementation for `Combine` as this is optional component.
pub struct NullCombiner;
impl<V> Combine<V> for NullCombiner
where
    V: Default + Serialize + DeserializeOwned,
{
    fn combine<E>(&self, _input: IntermediateInputKV<V>, _emitter: E) -> Result<()>
    where
        E: EmitIntermediate<String, V>,
    {
        Ok(())
    }
}
