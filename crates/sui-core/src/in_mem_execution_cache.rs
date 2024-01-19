// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::authority::authority_notify_read::EffectsNotifyRead;
use crate::authority::AuthorityStore;
use crate::transaction_outputs::TransactionOutputs;
use async_trait::async_trait;

use dashmap::DashMap;
use either::Either;
use futures::{
    future::{join_all, BoxFuture},
    FutureExt,
};
use moka::sync::Cache as MokaCache;
use mysten_common::sync::notify_read::NotifyRead;
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use sui_types::digests::{
    ObjectDigest, TransactionDigest, TransactionEffectsDigest, TransactionEventsDigest,
};
use sui_types::effects::{TransactionEffects, TransactionEvents};
use sui_types::error::{SuiError, SuiResult, UserInputError};
use sui_types::message_envelope::Message;
use sui_types::object::Object;
use sui_types::storage::{MarkerValue, ObjectKey, ObjectOrTombstone, ObjectStore, PackageObject};
use sui_types::sui_system_state::{get_sui_system_state, SuiSystemState};
use sui_types::transaction::VerifiedTransaction;
use sui_types::{
    base_types::{EpochId, ObjectID, ObjectRef, SequenceNumber},
    storage::InputKey,
};
use tracing::instrument;
use typed_store::Map;

pub trait ExecutionCacheRead: Send + Sync {
    fn get_package_object(&self, id: &ObjectID) -> SuiResult<Option<PackageObject>>;
    fn force_reload_system_packages(&self, system_package_ids: &[ObjectID]);

    fn get_object(&self, id: &ObjectID) -> SuiResult<Option<Object>>;

    fn get_objects(&self, objects: &[ObjectID]) -> SuiResult<Vec<Option<Object>>> {
        let mut ret = Vec::with_capacity(objects.len());
        for object_id in objects {
            ret.push(self.get_object(object_id)?);
        }
        Ok(ret)
    }

    fn get_latest_object_ref_or_tombstone(
        &self,
        object_id: ObjectID,
    ) -> SuiResult<Option<ObjectRef>>;

    fn get_latest_object_or_tombstone(
        &self,
        object_id: ObjectID,
    ) -> SuiResult<Option<(ObjectKey, ObjectOrTombstone)>>;

    fn get_object_by_key(
        &self,
        object_id: &ObjectID,
        version: SequenceNumber,
    ) -> SuiResult<Option<Object>>;

    fn multi_get_object_by_key(&self, object_keys: &[ObjectKey]) -> SuiResult<Vec<Option<Object>>>;

    /// Variant of multi_get_object_by_key used by transaction signing that returns better error messages
    /// when objects are not found. Returns an error if any object is not found.
    fn multi_get_object_by_objref(&self, objrefs: &[ObjectRef]) -> SuiResult<Vec<Object>>;

    fn object_exists_by_key(
        &self,
        object_id: &ObjectID,
        version: SequenceNumber,
    ) -> SuiResult<bool>;

    fn multi_object_exists_by_key(&self, object_keys: &[ObjectKey]) -> SuiResult<Vec<bool>>;

    /// Used by transaction manager to determine if input objects are ready. Distinct from multi_get_object_by_key
    /// because it also consults markers to handle the case where an object will never become available (e.g.
    /// because it has been received by some other transaction already).
    fn multi_input_objects_available(
        &self,
        keys: &[InputKey],
        receiving_objects: HashSet<InputKey>,
        epoch: EpochId,
    ) -> Result<Vec<bool>, SuiError> {
        let (keys_with_version, keys_without_version): (Vec<_>, Vec<_>) =
            keys.partition(|key| key.version().is_some());

        let mut versioned_results = vec![];
        for ((idx, input_key), has_key) in keys_with_version.iter().zip(
            self.perpetual_tables
                .objects
                .multi_contains_keys(
                    keys_with_version
                        .iter()
                        .map(|(_, k)| ObjectKey(k.id(), k.version().unwrap())),
                )?
                .into_iter(),
        ) {
            // If the key exists at the specified version, then the object is available.
            if has_key {
                versioned_results.push((*idx, true))
            } else if receiving_objects.contains(input_key) {
                // There could be a more recent version of this object, and the object at the
                // specified version could have already been pruned. In such a case `has_key` will
                // be false, but since this is a receiving object we should mark it as available if
                // we can determine that an object with a version greater than or equal to the
                // specified version exists or was deleted. We will then let mark it as available
                // to let the the transaction through so it can fail at execution.
                let is_available = self
                    .get_object(&input_key.id())?
                    .map(|obj| obj.version() >= input_key.version().unwrap())
                    .unwrap_or(false)
                    || self.have_deleted_owned_object_at_version_or_after(
                        &input_key.id(),
                        input_key.version().unwrap(),
                        epoch_store.epoch(),
                    )?;
                versioned_results.push((*idx, is_available));
            } else if self
                .get_deleted_shared_object_previous_tx_digest(
                    &input_key.id(),
                    &input_key.version().unwrap(),
                    epoch_store.epoch(),
                )?
                .is_some()
            {
                // If the object is an already deleted shared object, mark it as available if the
                // version for that object is in the shared deleted marker table.
                versioned_results.push((*idx, true));
            } else {
                versioned_results.push((*idx, false));
            }
        }

        let unversioned_results = keys_without_version.into_iter().map(|(idx, key)| {
            (
                idx,
                match self
                    .get_latest_object_ref_or_tombstone(key.id())
                    .expect("read cannot fail")
                {
                    None => false,
                    Some(entry) => entry.2.is_alive(),
                },
            )
        });

        let mut results = versioned_results
            .into_iter()
            .chain(unversioned_results)
            .collect::<Vec<_>>();
        results.sort_by_key(|(idx, _)| *idx);
        Ok(results.into_iter().map(|(_, result)| result).collect())
    }

    fn multi_get_transaction_blocks(
        &self,
        digests: &[TransactionDigest],
    ) -> SuiResult<Vec<Option<Arc<VerifiedTransaction>>>>;

    fn get_transaction_block(
        &self,
        digest: &TransactionDigest,
    ) -> SuiResult<Option<Arc<VerifiedTransaction>>> {
        self.multi_get_transaction_blocks(&[*digest])
            .map(|mut blocks| {
                blocks
                    .pop()
                    .expect("multi-get must return correct number of items")
            })
    }

    fn multi_get_executed_effects_digests(
        &self,
        digests: &[TransactionDigest],
    ) -> SuiResult<Vec<Option<TransactionEffectsDigest>>>;

    fn is_tx_already_executed(&self, digest: &TransactionDigest) -> SuiResult<bool> {
        self.multi_get_executed_effects_digests(&[*digest])
            .map(|mut digests| {
                digests
                    .pop()
                    .expect("multi-get must return correct number of items")
                    .is_some()
            })
    }

    fn multi_get_executed_effects(
        &self,
        digests: &[TransactionDigest],
    ) -> SuiResult<Vec<Option<TransactionEffects>>> {
        let effects_digests = self.multi_get_executed_effects_digests(digests)?;
        assert_eq!(effects_digests.len(), digests.len());

        let mut results = vec![None; digests.len()];
        let mut fetch_digests = Vec::with_capacity(digests.len());
        let mut fetch_indices = Vec::with_capacity(digests.len());

        for (i, digest) in effects_digests.into_iter().enumerate() {
            if let Some(digest) = digest {
                fetch_digests.push(digest);
                fetch_indices.push(i);
            }
        }

        let effects = self.multi_get_effects(&fetch_digests)?;
        for (i, effects) in fetch_indices.into_iter().zip(effects.into_iter()) {
            results[i] = effects;
        }

        Ok(results)
    }

    fn get_executed_effects(
        &self,
        digest: &TransactionDigest,
    ) -> SuiResult<Option<TransactionEffects>> {
        self.multi_get_executed_effects(&[*digest])
            .map(|mut effects| {
                effects
                    .pop()
                    .expect("multi-get must return correct number of items")
            })
    }

    fn multi_get_effects(
        &self,
        digests: &[TransactionEffectsDigest],
    ) -> SuiResult<Vec<Option<TransactionEffects>>>;

    fn get_effects(
        &self,
        digest: &TransactionEffectsDigest,
    ) -> SuiResult<Option<TransactionEffects>> {
        self.multi_get_effects(&[*digest]).map(|mut effects| {
            effects
                .pop()
                .expect("multi-get must return correct number of items")
        })
    }

    fn multi_get_events(
        &self,
        event_digests: &[TransactionEventsDigest],
    ) -> SuiResult<Vec<Option<TransactionEvents>>>;

    fn get_events(&self, digest: &TransactionEventsDigest) -> SuiResult<Option<TransactionEvents>> {
        self.multi_get_events(&[*digest]).map(|mut events| {
            events
                .pop()
                .expect("multi-get must return correct number of items")
        })
    }

    fn notify_read_executed_effects_digests<'a>(
        &'a self,
        digests: &'a [TransactionDigest],
    ) -> BoxFuture<'a, SuiResult<Vec<TransactionEffectsDigest>>>;

    fn notify_read_executed_effects<'a>(
        &'a self,
        digests: &'a [TransactionDigest],
    ) -> BoxFuture<'a, SuiResult<Vec<TransactionEffects>>> {
        async move {
            let digests = self.notify_read_executed_effects_digests(digests).await?;
            // once digests are available, effects must be present as well
            self.multi_get_effects(&digests).map(|effects| {
                effects
                    .into_iter()
                    .map(|e| e.expect("digests must exist"))
                    .collect()
            })
        }
        .boxed()
    }

    fn get_sui_system_state_object_unsafe(&self) -> SuiResult<SuiSystemState>;

    // Marker methods

    /// Get the marker at a specific version
    fn get_marker_value(
        &self,
        object_id: &ObjectID,
        version: &SequenceNumber,
        epoch_id: EpochId,
    ) -> SuiResult<Option<MarkerValue>>;

    /// Get the latest marker for a given object.
    fn get_latest_marker(
        &self,
        object_id: &ObjectID,
        epoch_id: EpochId,
    ) -> SuiResult<Option<(SequenceNumber, MarkerValue)>>;

    /// If the shared object was deleted, return deletion info for the current live version
    fn get_last_shared_object_deletion_info(
        &self,
        object_id: &ObjectID,
        epoch_id: EpochId,
    ) -> SuiResult<Option<(SequenceNumber, TransactionDigest)>> {
        match self.get_latest_marker(object_id, epoch_id)? {
            Some((version, MarkerValue::SharedDeleted(digest))) => Ok(Some((version, digest))),
            _ => Ok(None),
        }
    }

    /// If the shared object was deleted, return deletion info for the specified version.
    fn get_deleted_shared_object_previous_tx_digest(
        &self,
        object_id: &ObjectID,
        version: &SequenceNumber,
        epoch_id: EpochId,
    ) -> SuiResult<Option<TransactionDigest>> {
        match self.get_marker_value(object_id, version, epoch_id)? {
            Some(MarkerValue::SharedDeleted(digest)) => Ok(Some(digest)),
            _ => Ok(None),
        }
    }

    fn have_received_object_at_version(
        &self,
        object_id: &ObjectID,
        version: SequenceNumber,
        epoch_id: EpochId,
    ) -> SuiResult<bool> {
        match self.get_marker_value(object_id, &version, epoch_id)? {
            Some(MarkerValue::Received) => Ok(true),
            _ => Ok(false),
        }
    }
}

pub trait ExecutionCacheWrite: Send + Sync {
    /// Write the output of a transaction.
    ///
    /// Because of the child object consistency rule (readers that observe parents must observe all
    /// children of that parent, up to the parent's version bound), implementations of this method
    /// must not write any top-level (address-owned or shared) objects before they have written all
    /// of the object-owned objects (i.e. child objects) in the `objects` list.
    ///
    /// In the future, we may modify this method to expose finer-grained information about
    /// parent/child relationships. (This may be especially necessary for distributed object
    /// storage, but is unlikely to be an issue before we tackle that problem).
    ///
    /// This function may evict the mutable input objects (and successfully received objects) of
    /// transaction from the cache, since they cannot be read by any other transaction.
    ///
    /// Any write performed by this method immediately notifies any waiter that has previously
    /// called notify_read_objects_for_execution or notify_read_objects_for_signing for the object
    /// in question.
    fn write_transaction_outputs(&self, epoch_id: EpochId, tx_outputs: TransactionOutputs);
}

enum ObjectEntry {
    Object(Object),
    Deleted,
    Wrapped,
}

impl From<Object> for ObjectEntry {
    fn from(object: Object) -> Self {
        ObjectEntry::Object(object)
    }
}

type MarkerKey = (EpochId, ObjectID);

enum CacheResult<T> {
    /// Entry is in the cache
    Hit(T),
    /// Entry is not in the cache and is known to not exist
    NegativeHit,
    /// Entry is not in the cache and may or may not exist in the store
    Miss,
}

pub struct InMemoryCache {
    // Objects are not cached using an LRU because we manage cache evictions manually due to sui
    // semantics.
    //
    // The main consistency rule is that there cannot be a more recent version of the object in
    // the db than the most recent one in the cache.  This is because all writes go through the
    // cache, and we evict older versions first.
    //
    // Data in this table may be either dirty or clean (committed to db). It is dirty after a
    // transaction has been executed but before it has been committed to the db. After data has
    // been committed to the db, the object may or may not be evicted. Dead objects are always
    // evicted as soon as possible. Live objects may remain in the cache in a non-dirty state
    // if we expect them to be read again soon. Or they can be evicted in order to manage the size
    // of the cache.

    // The object dirty set. All writes go into this table first. After we flush the data to the
    // db, the data is removed from this table and inserted into the object_cache. This table
    // may contain both live and dead objects, since we flush both live and dead objects to the
    // db in order to support past object queries on fullnodes.
    // When we move data into the object_cache we only retain the live objects.
    objects: DashMap<ObjectID, BTreeMap<SequenceNumber, ObjectEntry>>,

    // Contains live, non-package objects that have been committed to the db.
    // TODO(cache): this is not populated yet, we will populate it when we implement flushing.
    object_cache: MokaCache<ObjectID, Arc<Mutex<BTreeMap<SequenceNumber, ObjectEntry>>>>,

    // Packages are cached separately from objects because they are immutable and can be used by any
    // number of transactions. Additionally, many operations require loading large numbers of packages
    // (due to dependencies), so we want to try to keep all packages in memory.
    // Note that, like any other dirty object, all packages are also stored in `objects` until they are
    // flushed to disk.
    packages: MokaCache<ObjectID, PackageObject>,

    // Markers for received objects and deleted shared objects. This contains all of the dirty
    // marker state, which is committed to the db at the same time as other transaction data.
    // After markers are committed to the db we remove them from this table and insert them into
    // marker_cache.
    markers: DashMap<MarkerKey, BTreeMap<SequenceNumber, MarkerValue>>,

    // Because markers (e.g. received markers) can be read by many transactions, we also cache
    // them. Markers are added to this cache in two ways:
    // 1. When they are committed to the db and removed from the `markers` table.
    // 2. After a cache miss in which we retrieve the marker from the db.

    // Note that MokaCache can only return items by value, so we store the map as an Arc<Mutex>.
    // (There should be no contention on the inner mutex, it is used only for interior mutability.)
    marker_cache: MokaCache<MarkerKey, Arc<Mutex<BTreeMap<SequenceNumber, MarkerValue>>>>,

    // Objects that were read at transaction signing time - allows us to access them again at
    // execution time with a single lock / hash lookup
    _transaction_objects: MokaCache<TransactionDigest, Vec<Object>>,

    transaction_effects: DashMap<TransactionEffectsDigest, TransactionEffects>,

    executed_effects_digests: DashMap<TransactionDigest, TransactionEffectsDigest>,

    // Transaction outputs that have not yet been written to the DB. Items are removed from this
    // table as they are flushed to the db.
    pending_transaction_writes: DashMap<TransactionDigest, TransactionOutputs>,

    executed_effects_digests_notify_read: NotifyRead<TransactionDigest, TransactionEffectsDigest>,

    store: Arc<AuthorityStore>,
}

impl InMemoryCache {
    pub fn new(store: Arc<AuthorityStore>) -> Self {
        let object_cache = MokaCache::builder()
            .max_capacity(10000)
            .initial_capacity(10000)
            .build();
        let packages = MokaCache::builder()
            .max_capacity(10000)
            .initial_capacity(10000)
            .build();
        let marker_cache = MokaCache::builder()
            .max_capacity(10000)
            .initial_capacity(10000)
            .build();
        let transaction_objects = MokaCache::builder()
            .max_capacity(10000)
            .initial_capacity(10000)
            .build();

        Self {
            objects: DashMap::new(),
            object_cache,
            packages,
            markers: DashMap::new(),
            marker_cache,
            _transaction_objects: transaction_objects,
            transaction_effects: DashMap::new(),
            executed_effects_digests: DashMap::new(),
            pending_transaction_writes: DashMap::new(),
            executed_effects_digests_notify_read: NotifyRead::new(),
            store,
        }
    }

    fn insert_object(&self, object_id: &ObjectID, object: &Object) {
        let version = object.version();
        tracing::debug!("inserting object {:?}: {:?}", object_id, version);
        self.objects
            .entry(*object_id)
            .or_default()
            .insert(object.version(), object.clone().into());
    }

    fn insert_deleted_tombstone(&self, object_id: &ObjectID, version: SequenceNumber) {
        tracing::debug!("inserting deleted tombstone {:?}: {:?}", object_id, version);
        self.objects
            .entry(*object_id)
            .or_default()
            .insert(version, ObjectEntry::Deleted);
    }

    fn insert_wrapped_tombstone(&self, object_id: &ObjectID, version: SequenceNumber) {
        tracing::debug!("inserting wrapped tombstone {:?}: {:?}", object_id, version);
        self.objects
            .entry(*object_id)
            .or_default()
            .insert(version, ObjectEntry::Wrapped);
    }

    fn get_object_by_key_cache_only(
        &self,
        object_id: &ObjectID,
        version: SequenceNumber,
    ) -> CacheResult<Object> {
        macro_rules! check_cache_entry {
            ($objects: expr) => {
                if let Some(object) = $objects.get(&version) {
                    if let ObjectEntry::Object(object) = object {
                        return CacheResult::Hit(object.clone());
                    }
                }

                if get_last(&*$objects).0 < &version {
                    // If the version is greater than the last version in the cache, then we know
                    // that the object does not exist anywhere
                    return CacheResult::NegativeHit;
                }
            };
        }

        if let Some(objects) = self.objects.get(object_id) {
            check_cache_entry!(objects);
        }

        if let Some(objects) = self.object_cache.get(object_id) {
            let objects = objects.lock();
            check_cache_entry!(objects);
        }

        CachResult::Miss
    }

    pub fn as_notify_read_wrapper(self: Arc<Self>) -> NotifyReadWrapper {
        NotifyReadWrapper(self)
    }
}

fn get_last<K, V>(map: &BTreeMap<K, V>) -> (&K, &V) {
    map.iter().next_back().expect("map cannot be empty")
}

impl ExecutionCacheRead for InMemoryCache {
    fn get_package_object(&self, package_id: &ObjectID) -> SuiResult<Option<PackageObject>> {
        if let Some(p) = self.packages.get(package_id) {
            #[cfg(debug_assertions)]
            {
                assert_eq!(
                    self.store.get_object(package_id).unwrap().unwrap().digest(),
                    p.object().digest(),
                    "Package object cache is inconsistent for package {:?}",
                    package_id
                )
            }
            return Ok(Some(p));
        }

        // We try the dirty objects cache as well before going to the database. This is necessary
        // because the package could be evicted from the package cache before it is committed
        // to the database.
        if let Some(p) = self.get_object(package_id)? {
            if p.is_package() {
                let p = PackageObject::new(p);
                self.packages.insert(*package_id, p.clone());
                Ok(Some(p))
            } else {
                Err(SuiError::UserInputError {
                    error: UserInputError::MoveObjectAsPackage {
                        object_id: *package_id,
                    },
                })
            }
        } else {
            Ok(None)
        }
    }

    fn force_reload_system_packages(&self, system_package_ids: &[ObjectID]) {
        for package_id in system_package_ids {
            if let Some(p) = self
                .store
                .get_object(package_id)
                .expect("Failed to update system packages")
            {
                assert!(p.is_package());
                self.packages.insert(*package_id, PackageObject::new(p));
            }
            // It's possible that a package is not found if it's newly added system package ID
            // that hasn't got created yet. This should be very very rare though.
        }
    }

    fn get_object(&self, id: &ObjectID) -> SuiResult<Option<Object>> {
        if let Some(objects) = self.objects.get(id) {
            // If any version of the object is in the cache, it must be the most recent version.
            return match get_last(&*objects).1 {
                ObjectEntry::Object(object) => Ok(Some(object.clone())),
                _ => Ok(None),
            };
        }

        if let Some(objects) = self.object_cache.get(id) {
            let objects = objects.lock();
            // If any version of the object is in the cache, it must be the most recent version.
            return match get_last(&*objects).1 {
                ObjectEntry::Object(object) => Ok(Some(object.clone())),
                _ => Ok(None),
            };
        }

        // We don't insert objects into the cache because they are usually only
        // read once.
        // TODO: we might want to cache immutable reads (RO shared objects and immutable objects)
        self.store.get_object(id)
    }

    fn get_object_by_key(
        &self,
        object_id: &ObjectID,
        version: SequenceNumber,
    ) -> SuiResult<Option<Object>> {
        match self.get_object_by_key_cache_only(object_id, version) {
            CacheResult::Hit(object) => Ok(Some(object)),
            CacheResult::NegativeHit => Ok(None),
            // We don't insert objects into the cache after a miss because they are usually only
            // read once.
            CacheResult::Miss => self.store.get_object_by_key(object_id, version),
        }
    }

    fn multi_get_object_by_key(
        &self,
        object_keys: &[ObjectKey],
    ) -> Result<Vec<Option<Object>>, SuiError> {
        let mut results = vec![None; object_keys.len()];
        let mut fallback_keys = Vec::with_capacity(object_keys.len());
        let mut fetch_indices = Vec::with_capacity(object_keys.len());

        for (i, key) in object_keys.iter().enumerate() {
            match self.get_object_by_key_cache_only(object_id, version) {
                CacheResult::Hit(object) => results[i] = Some(object),
                CacheResult::NegativeHit => (),
                CacheResult::Miss => {
                    fallback_keys.push(*key);
                    fetch_indices.push(i);
                }
            }
        }

        let store_results = self.store.multi_get_object_by_key(&fallback_keys)?;
        assert_eq!(store_results.len(), fetch_indices.len());
        assert_eq!(store_results.len(), fallback_keys.len());

        for (i, result) in fetch_indices.into_iter().zip(store_results.into_iter()) {
            results[i] = result;
        }

        Ok(results)
    }

    fn multi_get_object_by_objref(&self, objrefs: &[ObjectRef]) -> SuiResult<Vec<Object>> {
        let mut results = vec![None; objrefs.len()];
        let mut fallback_keys = Vec::with_capacity(objrefs.len());
        let mut fetch_indices = Vec::with_capacity(objrefs.len());

        for (i, objref) in objrefs.iter().enumerate() {
            match self.get_object_by_key_cache_only(&objref.0, objref.1) {
                CacheResult::Hit(object) => results[i] = Some(object),
                CacheResult::NegativeHit => {
                    return Err(SuiError::UserInputError {
                        error: UserInputError::ObjectNotFound {
                            object_id: objref.0,
                            version: objref.1,
                        },
                    })
                }
                CacheResult::Miss => {
                    fallback_keys.push(*objref);
                    fetch_indices.push(i);
                }
            }
        }

        let store_results = self
            .store
            .multi_get_object_with_more_accurate_error_return(&fallback_keys)?;
        assert_eq!(store_results.len(), fetch_indices.len());
        assert_eq!(store_results.len(), fallback_keys.len());

        for (i, result) in fetch_indices.into_iter().zip(store_results.into_iter()) {
            results[i] = Some(result);
        }

        Ok(results.into_iter().map(|r| r.unwrap()).collect())
    }

    fn object_exists_by_key(
        &self,
        object_id: &ObjectID,
        version: SequenceNumber,
    ) -> SuiResult<bool> {
        if self
            .get_object_by_key_cache_only(object_id, version)
            .is_some()
        {
            return Ok(true);
        }
    }

    fn multi_object_exists_by_key(&self, object_keys: &[ObjectKey]) -> SuiResult<Vec<bool>> {
        let mut results = vec![false; object_keys.len()];
        let mut fallback_keys = Vec::with_capacity(object_keys.len());
        let mut fetch_indices = Vec::with_capacity(object_keys.len());

        for (i, key) in object_keys.iter().enumerate() {
            if self.object_exists_by_key(&key.0, key.1)? {
                results[i] = true;
            } else {
                fallback_keys.push(*key);
                fetch_indices.push(i);
            }
        }

        let store_results = self.store.multi_object_exists_by_key(&fallback_keys)?;
        assert_eq!(store_results.len(), fetch_indices.len());
        assert_eq!(store_results.len(), fallback_keys.len());

        for (i, result) in fetch_indices.into_iter().zip(store_results.into_iter()) {
            results[i] = result;
        }

        Ok(results)
    }

    fn get_latest_object_ref_or_tombstone(
        &self,
        object_id: ObjectID,
    ) -> SuiResult<Option<ObjectRef>> {
        if let Some(objects) = self.objects.get(&object_id) {
            let (version, object) = get_last(&*objects);
            let objref = match object {
                ObjectEntry::Object(object) => object.compute_object_reference(),
                ObjectEntry::Deleted => (object_id, *version, ObjectDigest::OBJECT_DIGEST_DELETED),
                ObjectEntry::Wrapped => (object_id, *version, ObjectDigest::OBJECT_DIGEST_WRAPPED),
            };
            return Ok(Some(objref));
        }

        self.store.get_latest_object_ref_or_tombstone(object_id)
    }

    fn get_latest_object_or_tombstone(
        &self,
        object_id: ObjectID,
    ) -> Result<Option<(ObjectKey, ObjectOrTombstone)>, SuiError> {
        if let Some(objref) = self.get_latest_object_ref_or_tombstone(object_id)? {
            if !objref.2.is_alive() {
                return Ok(Some((objref.into(), ObjectOrTombstone::Tombstone(objref))));
            } else {
                let key: ObjectKey = objref.into();
                let object = self.get_object_by_key(&objref.0, objref.1)?;
                return Ok(object.map(|o| (key, o.into())));
            }
        }

        self.store.get_latest_object_or_tombstone(object_id)
    }

    fn multi_get_transaction_blocks(
        &self,
        digests: &[TransactionDigest],
    ) -> SuiResult<Vec<Option<Arc<VerifiedTransaction>>>> {
        let mut results = vec![None; digests.len()];
        let mut fetch_indices = Vec::with_capacity(digests.len());
        let mut fetch_digests = Vec::with_capacity(digests.len());

        for (i, digest) in digests.iter().enumerate() {
            if let Some(tx) = self.pending_transaction_writes.get(digest) {
                results[i] = Some(tx.transaction.clone());
            } else {
                fetch_indices.push(i);
                fetch_digests.push(*digest);
            }
        }

        let multiget_results = self.store.multi_get_transaction_blocks(&fetch_digests)?;
        assert_eq!(multiget_results.len(), fetch_indices.len());
        assert_eq!(multiget_results.len(), fetch_digests.len());

        for (i, result) in fetch_indices.into_iter().zip(multiget_results.into_iter()) {
            results[i] = result.map(Arc::new);
        }

        Ok(results)
    }

    fn multi_get_executed_effects_digests(
        &self,
        digests: &[TransactionDigest],
    ) -> SuiResult<Vec<Option<TransactionEffectsDigest>>> {
        let mut results = vec![None; digests.len()];
        let mut fetch_indices = Vec::with_capacity(digests.len());
        let mut fetch_digests = Vec::with_capacity(digests.len());

        for (i, digest) in digests.iter().enumerate() {
            if let Some(digest) = self.executed_effects_digests.get(digest) {
                results[i] = Some(*digest);
            } else {
                fetch_indices.push(i);
                fetch_digests.push(*digest);
            }
        }

        let multiget_results = self
            .store
            .multi_get_executed_effects_digests(&fetch_digests)?;
        assert_eq!(multiget_results.len(), fetch_indices.len());
        assert_eq!(multiget_results.len(), fetch_digests.len());

        for (i, result) in fetch_indices.into_iter().zip(multiget_results.into_iter()) {
            results[i] = result;
        }

        Ok(results)
    }

    fn multi_get_effects(
        &self,
        digests: &[TransactionEffectsDigest],
    ) -> SuiResult<Vec<Option<TransactionEffects>>> {
        let mut results = vec![None; digests.len()];
        let mut fetch_indices = Vec::with_capacity(digests.len());
        let mut fetch_digests = Vec::with_capacity(digests.len());

        for (i, digest) in digests.iter().enumerate() {
            if let Some(effects) = self.transaction_effects.get(digest) {
                results[i] = Some(effects.clone());
            } else {
                fetch_indices.push(i);
                fetch_digests.push(*digest);
            }
        }

        let fetch_results = self.store.perpetual_tables.effects.multi_get(digests)?;
        for (i, result) in fetch_indices.into_iter().zip(fetch_results.into_iter()) {
            results[i] = result;
        }

        Ok(results)
    }

    fn notify_read_executed_effects_digests<'a>(
        &'a self,
        digests: &'a [TransactionDigest],
    ) -> BoxFuture<'a, SuiResult<Vec<TransactionEffectsDigest>>> {
        async move {
            let registrations = self
                .executed_effects_digests_notify_read
                .register_all(digests);

            let executed_effects_digests = self.multi_get_executed_effects_digests(digests)?;

            let results = executed_effects_digests
                .into_iter()
                .zip(registrations)
                .map(|(a, r)| match a {
                    // Note that Some() clause also drops registration that is already fulfilled
                    Some(ready) => Either::Left(futures::future::ready(ready)),
                    None => Either::Right(r),
                });

            Ok(join_all(results).await)
        }
        .boxed()
    }

    fn multi_get_events(
        &self,
        event_digests: &[TransactionEventsDigest],
    ) -> SuiResult<Vec<Option<TransactionEvents>>> {
        // TODO: use cache?
        self.store.multi_get_events(event_digests)
    }

    fn get_sui_system_state_object_unsafe(&self) -> SuiResult<SuiSystemState> {
        get_sui_system_state(&ObjectStoreWrapper(self))
    }

    fn get_marker_value(
        &self,
        object_id: &ObjectID,
        version: &SequenceNumber,
        epoch_id: EpochId,
    ) -> SuiResult<Option<MarkerValue>> {
        // first check the dirty markers
        if let Some(markers) = self.markers.get(&(epoch_id, *object_id)) {
            if let Some(marker) = markers.get(version) {
                return Ok(Some(*marker));
            }
        }

        // now check the cache
        if let Some(markers) = self.marker_cache.get(&(epoch_id, *object_id)) {
            if let Some(marker) = markers.lock().get(version) {
                return Ok(Some(*marker));
            }
        }

        // fall back to the db
        // NOTE: we cannot insert this marker into the cache, because the cache
        // must always contain the latest marker version if it contains any marker
        // for an object.
        self.store.get_marker_value(object_id, version, epoch_id)
    }

    fn get_latest_marker(
        &self,
        object_id: &ObjectID,
        epoch_id: EpochId,
    ) -> SuiResult<Option<(SequenceNumber, MarkerValue)>> {
        // Note: the reads from the dirty set and the cache are both safe, because both
        // of these structures are guaranteed to contain the latest marker if they have
        // an entry at all for a given object.

        if let Some(markers) = self.markers.get(&(epoch_id, *object_id)) {
            let (k, v) = get_last(&*markers);
            return Ok(Some((*k, *v)));
        }

        if let Some(markers) = self.marker_cache.get(&(epoch_id, *object_id)) {
            let markers = markers.lock();
            let (k, v) = get_last(&*markers);
            return Ok(Some((*k, *v)));
        }

        // TODO: we could insert this marker into the cache since it is the latest
        self.store.get_latest_marker(object_id, epoch_id)
    }
}

impl ExecutionCacheWrite for InMemoryCache {
    #[instrument(level = "debug", skip_all)]
    fn write_transaction_outputs(&self, epoch_id: EpochId, tx_outputs: TransactionOutputs) {
        let TransactionOutputs {
            transaction,
            effects,
            markers,
            written,
            deleted,
            wrapped,
            locks_to_delete,
            new_locks_to_init,
            ..
        } = &tx_outputs;

        // Update all markers
        for (object_key, marker_value) in markers.iter() {
            self.markers
                .entry((epoch_id, object_key.0))
                .or_default()
                .value_mut()
                .insert(object_key.1, *marker_value);
        }

        // Write children before parents to ensure that readers do not observe a parent object
        // before its most recent children are visible.
        for (object_id, object) in written.iter() {
            if object.is_child_object() {
                self.insert_object(object_id, object);
            }
        }
        for (object_id, object) in written.iter() {
            if !object.is_child_object() {
                self.insert_object(object_id, object);
                if object.is_package() {
                    self.packages
                        .insert(*object_id, PackageObject::new(object.clone()));
                }
            }
        }

        for ObjectKey(id, version) in deleted.iter() {
            self.insert_deleted_tombstone(id, *version);
        }
        for ObjectKey(id, version) in wrapped.iter() {
            self.insert_wrapped_tombstone(id, *version);
        }

        // TODO(cache): remove dead objects from cache - this cannot actually be done
        // until until objects are committed to the db, because
        /*
        for (id, version) in effects.modified_at_versions().iter() {
            // delete the given id, version from self.objects. if no versions remain, remove the
            // entry from self.objects
            match self.objects.entry(*id) {
                dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                    entry.get_mut().remove(version);
                    if entry.get().is_empty() {
                        entry.remove();
                    }
                }
                dashmap::mapref::entry::Entry::Vacant(_) => panic!("object not found"),
            }
        }
        */

        write_locks(&self.store, locks_to_delete, new_locks_to_init);

        let tx_digest = *transaction.digest();
        let effects_digest = effects.digest();

        self.transaction_effects
            .insert(effects_digest, effects.clone());

        self.executed_effects_digests
            .insert(tx_digest, effects_digest);

        self.pending_transaction_writes
            .insert(tx_digest, tx_outputs);

        self.executed_effects_digests_notify_read
            .notify(&tx_digest, &effects_digest);
    }
}

// TODO(cache): this is not safe, because now we are committing persistent state to the db, while the rest
// of the transaction outputs could simply be lost if the validator restarts. To fix this we must
// cache locks (which we want to do anyway) but that's a big enough change for it to be worth
// saving for a separate PR.
#[instrument(level = "trace", skip_all)]
fn write_locks(
    store: &AuthorityStore,
    locks_to_delete: &[ObjectRef],
    new_locks_to_init: &[ObjectRef],
) {
    store
        .check_owned_object_locks_exist(locks_to_delete)
        .expect("locks must exist for certificate to be executed");
    let lock_table = &store.perpetual_tables.owned_object_transaction_locks;
    let mut batch = lock_table.batch();
    AuthorityStore::initialize_locks(lock_table, &mut batch, new_locks_to_init, false)
        .expect("Failed to initialize locks");
    store
        .delete_locks(&mut batch, locks_to_delete)
        .expect("Failed to delete locks");
    batch.write().expect("Failed to write locks");
}

// TODO: Remove EffectsNotifyRead trait and just use ExecutionCacheRead directly everywhere.
/// This wrapper is used so that we don't have to disambiguate traits at every callsite.
pub struct NotifyReadWrapper(Arc<InMemoryCache>);

#[async_trait]
impl EffectsNotifyRead for NotifyReadWrapper {
    async fn notify_read_executed_effects(
        &self,
        digests: Vec<TransactionDigest>,
    ) -> SuiResult<Vec<TransactionEffects>> {
        self.0.notify_read_executed_effects(&digests).await
    }

    async fn notify_read_executed_effects_digests(
        &self,
        digests: Vec<TransactionDigest>,
    ) -> SuiResult<Vec<TransactionEffectsDigest>> {
        self.0.notify_read_executed_effects_digests(&digests).await
    }

    fn multi_get_executed_effects(
        &self,
        digests: &[TransactionDigest],
    ) -> SuiResult<Vec<Option<TransactionEffects>>> {
        self.0.multi_get_executed_effects(digests)
    }
}

// Wrapper to avoid having to disambiguate traits everywhere
struct ObjectStoreWrapper<'a>(&'a InMemoryCache);

impl<'a> ObjectStore for ObjectStoreWrapper<'a> {
    fn get_object(&self, object_id: &ObjectID) -> Result<Option<Object>, SuiError> {
        self.0.get_object(object_id)
    }

    fn get_object_by_key(
        &self,
        object_id: &ObjectID,
        version: sui_types::base_types::VersionNumber,
    ) -> Result<Option<Object>, SuiError> {
        self.0.get_object_by_key(object_id, version)
    }
}
