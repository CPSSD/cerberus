use serde::de::DeserializeOwned;
use serde::Serialize;

use combiner::Combine;
use emitter::EmitFinal;
use errors::*;
use intermediate::IntermediateInputKV;
use mapper::Map;
use partition::Partition;
use reducer::Reduce;

/// `UserImplRegistry` tracks the user's implementations of Map, Reduce, etc.
///
/// The user should use the `UserImplRegistryBuilder` to create this and then pass it in to `run`.
pub struct UserImplRegistry<'a, M, R, P, C>
where
    M: Map + 'a,
    R: Reduce<M::Key, M::Value> + 'a,
    P: Partition<M::Key, M::Value> + 'a,
    C: Combine<M::Key, M::Value> + 'a,
{
    pub mapper: &'a M,
    pub reducer: &'a R,
    pub partitioner: &'a P,
    pub combiner: Option<&'a C>,
}

/// `UserImplRegistryBuilder` is used to create a `UserImplRegistry`.
pub struct UserImplRegistryBuilder<'a, M, R, P, C>
where
    M: Map + 'a,
    R: Reduce<M::Key, M::Value> + 'a,
    P: Partition<M::Key, M::Value> + 'a,
    C: Combine<M::Key, M::Value> + 'a,
{
    mapper: Option<&'a M>,
    reducer: Option<&'a R>,
    partitioner: Option<&'a P>,
    combiner: Option<&'a C>,
}

impl<'a, M, R, P, C> Default for UserImplRegistryBuilder<'a, M, R, P, C>
where
    M: Map + 'a,
    R: Reduce<M::Key, M::Value> + 'a,
    P: Partition<M::Key, M::Value> + 'a,
    C: Combine<M::Key, M::Value> + 'a,
{
    fn default() -> UserImplRegistryBuilder<'a, M, R, P, C> {
        UserImplRegistryBuilder {
            mapper: None,
            reducer: None,
            partitioner: None,
            combiner: None,
        }
    }
}

impl<'a, M, R, P, C> UserImplRegistryBuilder<'a, M, R, P, C>
where
    M: Map + 'a,
    R: Reduce<M::Key, M::Value> + 'a,
    P: Partition<M::Key, M::Value> + 'a,
    C: Combine<M::Key, M::Value> + 'a,
{
    pub fn new() -> UserImplRegistryBuilder<'a, M, R, P, C> {
        Default::default()
    }

    pub fn mapper(&mut self, mapper: &'a M) -> &mut UserImplRegistryBuilder<'a, M, R, P, C> {
        self.mapper = Some(mapper);
        self
    }

    pub fn reducer(&mut self, reducer: &'a R) -> &mut UserImplRegistryBuilder<'a, M, R, P, C> {
        self.reducer = Some(reducer);
        self
    }

    pub fn partitioner(
        &mut self,
        partitioner: &'a P,
    ) -> &mut UserImplRegistryBuilder<'a, M, R, P, C> {
        self.partitioner = Some(partitioner);
        self
    }

    pub fn combiner(&mut self, combiner: &'a C) -> &mut UserImplRegistryBuilder<'a, M, R, P, C> {
        self.combiner = Some(combiner);
        self
    }

    pub fn build(&self) -> Result<UserImplRegistry<'a, M, R, P, C>> {
        let mapper = self.mapper
            .chain_err(|| "Error building UserImplRegistry: No Mapper provided")?;
        let reducer = self.reducer
            .chain_err(|| "Error building UserImplRegistry: No Reducer provided")?;
        let partitioner = self.partitioner
            .chain_err(|| "Error building UserImplRegistry: No Partitioner provided")?;

        Ok(UserImplRegistry {
            mapper,
            reducer,
            partitioner,
            combiner: self.combiner,
        })
    }
}

/// A null implementation for `Combine` as this is optional component.
/// This should not be used by user code.
pub struct NullCombiner;
impl<K, V> Combine<K, V> for NullCombiner
where
    K: Default + Serialize + DeserializeOwned,
    V: Default + Serialize + DeserializeOwned,
{
    fn combine<E>(&self, _input: IntermediateInputKV<K, V>, _emitter: E) -> Result<()>
    where
        E: EmitFinal<V>,
    {
        Err("This code should never run".into())
    }
}

/// Construct a `UserImplRegistryBuilder` that does not need a `Combine` implementation
impl<'a, M, R, P> UserImplRegistryBuilder<'a, M, R, P, NullCombiner>
where
    M: Map + 'a,
    R: Reduce<M::Key, M::Value> + 'a,
    P: Partition<M::Key, M::Value> + 'a,
{
    pub fn new_no_combiner() -> UserImplRegistryBuilder<'a, M, R, P, NullCombiner> {
        Default::default()
    }
}
