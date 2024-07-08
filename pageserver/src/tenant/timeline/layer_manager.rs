use anyhow::{bail, ensure, Context, Result};
use itertools::Itertools;
use pageserver_api::shard::TenantShardId;
use std::{collections::HashMap, sync::Arc};
use tracing::trace;
use utils::{
    id::TimelineId,
    lsn::{AtomicLsn, Lsn},
};

use crate::{
    config::PageServerConf,
    context::RequestContext,
    metrics::TimelineMetrics,
    tenant::{
        layer_map::{BatchedUpdates, LayerMap},
        storage_layer::{
            AsLayerDesc, InMemoryLayer, Layer, PersistentLayerDesc, PersistentLayerKey,
            ResidentLayer,
        },
    },
};

use super::TimelineWriterState;

/// Provides semantic APIs to manipulate the layer map.
pub(crate) enum LayerManager {
    /// Open as in not shutdown layer manager; we still have in-memory layers and we can manipulate
    /// the layers.
    Open(OpenLayerManager),
    /// Shutdown layer manager where there are no more in-memory layers and persistent layers are
    /// read-only.
    Closed {
        layers: HashMap<PersistentLayerKey, Layer>,
    },
}

impl Default for LayerManager {
    fn default() -> Self {
        LayerManager::Open(OpenLayerManager::default())
    }
}

impl LayerManager {
    pub(crate) fn get_from_desc(&self, desc: &PersistentLayerDesc) -> Layer {
        use LayerManager::*;
        let maybe = match self {
            Open(open) => open.get_from_desc(desc),
            Closed { layers } => layers.get(&desc.key()),
        };

        maybe
            .with_context(|| format!("get layer from desc: {}", desc.layer_name()))
            .expect("not found")
            .clone()
    }

    /// Get an immutable reference to the layer map.
    ///
    /// We expect users only to be able to get an immutable layer map. If users want to make modifications,
    /// they should use the below semantic APIs. This design makes us step closer to immutable storage state.
    pub(crate) fn layer_map(&self) -> Result<&LayerMap, Shutdown> {
        use LayerManager::*;
        match self {
            Open(OpenLayerManager { layer_map, .. }) => Ok(layer_map),
            Closed { .. } => Err(Shutdown),
        }
    }

    /// Called from `load_layer_map`. Initialize the layer manager with:
    /// 1. all on-disk layers
    /// 2. next open layer (with disk disk_consistent_lsn LSN)
    pub(crate) fn initialize_local_layers(&mut self, layers: Vec<Layer>, next_open_layer_at: Lsn) {
        use LayerManager::*;
        match self {
            Open(open) => open.initialize_local_layers(layers, next_open_layer_at),
            Closed { .. } => {
                panic!("cannot initialize shutdown layer manager");
            }
        }
    }

    /// Initialize when creating a new timeline, called in `init_empty_layer_map`.
    pub(crate) fn initialize_empty(&mut self, next_open_layer_at: Lsn) {
        use LayerManager::*;
        match self {
            Open(open) => open.initialize_empty(next_open_layer_at),
            Closed { .. } => {
                panic!("cannot initialize shutdown layer manager");
            }
        }
    }

    /// Open a new writable layer to append data if there is no open layer, otherwise return the
    /// current open layer, called within `get_layer_for_write`.
    pub(crate) async fn get_layer_for_write(
        &mut self,
        lsn: Lsn,
        conf: &'static PageServerConf,
        timeline_id: TimelineId,
        tenant_shard_id: TenantShardId,
        gate_guard: utils::sync::gate::GateGuard,
        ctx: &RequestContext,
    ) -> anyhow::Result<Arc<InMemoryLayer>> {
        use LayerManager::*;
        match self {
            Open(open) => {
                open.get_layer_for_write(lsn, conf, timeline_id, tenant_shard_id, gate_guard, ctx)
                    .await
            }
            Closed { .. } => Err(Shutdown.into()),
        }
    }

    /// Tries to freeze an open layer and also manages clearing the TimelineWriterState.
    ///
    /// Returns true if anything was frozen, fails if layer manager was shutdown already.
    pub(super) async fn try_freeze_in_memory_layer(
        &mut self,
        lsn: Lsn,
        last_freeze_at: &AtomicLsn,
        write_lock: &mut tokio::sync::MutexGuard<'_, Option<TimelineWriterState>>,
    ) -> Result<bool, Shutdown> {
        use LayerManager::*;
        match self {
            Open(open) => Ok(open
                .try_freeze_in_memory_layer(lsn, last_freeze_at, write_lock)
                .await),
            Closed { .. } => Err(Shutdown),
        }
    }

    /// Add image layers to the layer map, called from `create_image_layers`.
    pub(crate) fn track_new_image_layers(
        &mut self,
        image_layers: &[ResidentLayer],
        metrics: &TimelineMetrics,
    ) {
        use LayerManager::*;
        match self {
            Open(open) => open.track_new_image_layers(image_layers, metrics),
            Closed { .. } => tracing::warn!("tracked new image layers on closed layer manager"),
        }
    }

    /// Flush a frozen layer and add the written delta layer to the layer map.
    pub(crate) fn finish_flush_l0_layer(
        &mut self,
        delta_layer: Option<&ResidentLayer>,
        frozen_layer_for_check: &Arc<InMemoryLayer>,
        metrics: &TimelineMetrics,
    ) {
        use LayerManager::*;
        match self {
            Open(open) => open.finish_flush_l0_layer(delta_layer, frozen_layer_for_check, metrics),
            Closed { .. } => tracing::warn!("finished flushing L0 layer on closed"),
        }
    }

    /// LayerManager shutdown. The in-memory layers do cleanup on drop, so we must drop them in
    /// order to allow shutdown to complete.
    ///
    /// If there was a want to flush in-memory layers, it must have happened earlier.
    pub(crate) fn shutdown(&mut self, writer_state: &mut Option<TimelineWriterState>) {
        use LayerManager::*;
        match self {
            Open(OpenLayerManager {
                layer_map,
                layer_fmgr: LayerFileManager(hashmap),
                ..
            }) => {
                let open = layer_map.open_layer.take();
                let frozen = layer_map.frozen_layers.len();
                let taken_writer_state = writer_state.take();
                tracing::info!(open = open.is_some(), frozen, "dropped inmemory layers");
                let layers = std::mem::take(hashmap);
                *self = Closed { layers };
                assert_eq!(open.is_some(), taken_writer_state.is_some());
            }
            Closed { .. } => tracing::warn!("ignoring multiple shutdowns on layer manager"),
        }
    }

    /// Called when compaction is completed.
    pub(crate) fn finish_compact_l0(
        &mut self,
        compact_from: &[Layer],
        compact_to: &[ResidentLayer],
        metrics: &TimelineMetrics,
    ) {
        use LayerManager::*;
        match self {
            Open(open) => open.finish_compact_l0(compact_from, compact_to, metrics),
            Closed { .. } => tracing::warn!("ignoring finish_compact_l0"),
        }
    }

    /// Called when a GC-compaction is completed.
    pub(crate) fn finish_gc_compaction(
        &mut self,
        compact_from: &[Layer],
        compact_to: &[ResidentLayer],
        metrics: &TimelineMetrics,
    ) -> Result<(), Shutdown> {
        self.open_mut()?
            .finish_gc_compaction(compact_from, compact_to, metrics);
        Ok(())
    }

    /// Called post-compaction when some previous generation image layers were trimmed.
    pub(crate) fn rewrite_layers(
        &mut self,
        rewrite_layers: &[(Layer, ResidentLayer)],
        drop_layers: &[Layer],
        metrics: &TimelineMetrics,
    ) -> Result<(), Shutdown> {
        self.open_mut()?
            .rewrite_layers(rewrite_layers, drop_layers, metrics);
        Ok(())
    }

    /// Called when garbage collect has selected the layers to be removed.
    pub(crate) fn finish_gc_timeline(&mut self, gc_layers: &[Layer]) -> Result<(), Shutdown> {
        self.open_mut()?.finish_gc_timeline(gc_layers);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn force_insert_layer(&mut self, layer: ResidentLayer) {
        self.open_mut().unwrap().force_insert_layer(layer)
    }

    fn open_mut(&mut self) -> Result<&mut OpenLayerManager, Shutdown> {
        use LayerManager::*;

        match self {
            Open(open) => Ok(open),
            Closed { .. } => Err(Shutdown),
        }
    }

    /// Sum up the historic layer sizes
    pub(crate) fn layer_size_sum(&self) -> u64 {
        self.layers()
            .values()
            .map(|l| l.layer_desc().file_size)
            .sum()
    }

    pub(crate) fn likely_resident_layers(&self) -> impl Iterator<Item = &'_ Layer> + '_ {
        self.layers().values().filter(|l| l.is_likely_resident())
    }

    pub(crate) fn contains(&self, layer: &Layer) -> bool {
        self.layers().contains_key(&layer.layer_desc().key())
    }

    pub(crate) fn all_persistent_layers(&self) -> Vec<PersistentLayerKey> {
        self.layers().keys().cloned().collect_vec()
    }

    fn layers(&self) -> &HashMap<PersistentLayerKey, Layer> {
        use LayerManager::*;
        match self {
            Open(OpenLayerManager { layer_fmgr, .. }) => &layer_fmgr.0,
            Closed { layers } => layers,
        }
    }
}

#[derive(Default)]
pub(crate) struct OpenLayerManager {
    layer_map: LayerMap,
    layer_fmgr: LayerFileManager<Layer>,
}

#[derive(Debug, thiserror::Error)]
#[error("layer manager has been shutdown")]
pub(crate) struct Shutdown;

impl OpenLayerManager {
    pub(crate) fn get_from_desc(&self, desc: &PersistentLayerDesc) -> Option<&Layer> {
        self.layer_fmgr.get_from_desc(desc)
    }

    pub(crate) fn initialize_local_layers(
        &mut self,
        on_disk_layers: Vec<Layer>,
        next_open_layer_at: Lsn,
    ) {
        let mut updates = self.layer_map.batch_update();
        for layer in on_disk_layers {
            Self::insert_historic_layer(layer, &mut updates, &mut self.layer_fmgr);
        }
        updates.flush();
        self.layer_map.next_open_layer_at = Some(next_open_layer_at);
    }

    pub(crate) fn initialize_empty(&mut self, next_open_layer_at: Lsn) {
        self.layer_map.next_open_layer_at = Some(next_open_layer_at);
    }

    pub(crate) async fn get_layer_for_write(
        &mut self,
        lsn: Lsn,
        conf: &'static PageServerConf,
        timeline_id: TimelineId,
        tenant_shard_id: TenantShardId,
        gate_guard: utils::sync::gate::GateGuard,
        ctx: &RequestContext,
    ) -> Result<Arc<InMemoryLayer>> {
        ensure!(lsn.is_aligned());

        // Do we have a layer open for writing already?
        let layer = if let Some(open_layer) = &self.layer_map.open_layer {
            if open_layer.get_lsn_range().start > lsn {
                bail!(
                    "unexpected open layer in the future: open layers starts at {}, write lsn {}",
                    open_layer.get_lsn_range().start,
                    lsn
                );
            }

            Arc::clone(open_layer)
        } else {
            // No writeable layer yet. Create one.
            let start_lsn = self
                .layer_map
                .next_open_layer_at
                .context("No next open layer found")?;

            trace!(
                "creating in-memory layer at {}/{} for record at {}",
                timeline_id,
                start_lsn,
                lsn
            );

            let new_layer = InMemoryLayer::create(
                conf,
                timeline_id,
                tenant_shard_id,
                start_lsn,
                gate_guard,
                ctx,
            )
            .await?;
            let layer = Arc::new(new_layer);

            self.layer_map.open_layer = Some(layer.clone());
            self.layer_map.next_open_layer_at = None;

            layer
        };

        Ok(layer)
    }

    pub(super) async fn try_freeze_in_memory_layer(
        &mut self,
        lsn: Lsn,
        last_freeze_at: &AtomicLsn,
        write_lock: &mut tokio::sync::MutexGuard<'_, Option<TimelineWriterState>>,
    ) -> bool {
        let Lsn(last_record_lsn) = lsn;
        let end_lsn = Lsn(last_record_lsn + 1);

        let froze = if let Some(open_layer) = &self.layer_map.open_layer {
            let open_layer_rc = Arc::clone(open_layer);
            open_layer.freeze(end_lsn).await;

            // The layer is no longer open, update the layer map to reflect this.
            // We will replace it with on-disk historics below.
            self.layer_map.frozen_layers.push_back(open_layer_rc);
            self.layer_map.open_layer = None;
            self.layer_map.next_open_layer_at = Some(end_lsn);

            true
        } else {
            false
        };

        // Even if there was no layer to freeze, advance last_freeze_at to last_record_lsn+1: this
        // accounts for regions in the LSN range where we might have ingested no data due to sharding.
        last_freeze_at.store(end_lsn);

        // the writer state must no longer have a reference to the frozen layer
        let taken = write_lock.take();
        assert_eq!(
            froze,
            taken.is_some(),
            "should only had frozen a layer when TimelineWriterState existed"
        );

        froze
    }

    pub(crate) fn track_new_image_layers(
        &mut self,
        image_layers: &[ResidentLayer],
        metrics: &TimelineMetrics,
    ) {
        let mut updates = self.layer_map.batch_update();
        for layer in image_layers {
            Self::insert_historic_layer(layer.as_ref().clone(), &mut updates, &mut self.layer_fmgr);

            // record these here instead of Layer::finish_creating because otherwise partial
            // failure with create_image_layers would balloon up the physical size gauge. downside
            // is that all layers need to be created before metrics are updated.
            metrics.record_new_file_metrics(layer.layer_desc().file_size);
        }
        updates.flush();
    }

    pub(crate) fn finish_flush_l0_layer(
        &mut self,
        delta_layer: Option<&ResidentLayer>,
        frozen_layer_for_check: &Arc<InMemoryLayer>,
        metrics: &TimelineMetrics,
    ) {
        let inmem = self
            .layer_map
            .frozen_layers
            .pop_front()
            .expect("there must be a inmem layer to flush");

        // Only one task may call this function at a time (for this
        // timeline). If two tasks tried to flush the same frozen
        // layer to disk at the same time, that would not work.
        assert_eq!(Arc::as_ptr(&inmem), Arc::as_ptr(frozen_layer_for_check));

        if let Some(l) = delta_layer {
            let mut updates = self.layer_map.batch_update();
            Self::insert_historic_layer(l.as_ref().clone(), &mut updates, &mut self.layer_fmgr);
            metrics.record_new_file_metrics(l.layer_desc().file_size);
            updates.flush();
        }
    }

    pub(crate) fn finish_compact_l0(
        &mut self,
        compact_from: &[Layer],
        compact_to: &[ResidentLayer],
        metrics: &TimelineMetrics,
    ) {
        let mut updates = self.layer_map.batch_update();
        for l in compact_to {
            Self::insert_historic_layer(l.as_ref().clone(), &mut updates, &mut self.layer_fmgr);
            metrics.record_new_file_metrics(l.layer_desc().file_size);
        }
        for l in compact_from {
            Self::delete_historic_layer(l, &mut updates, &mut self.layer_fmgr);
        }
        updates.flush();
    }

    pub(crate) fn finish_gc_compaction(
        &mut self,
        compact_from: &[Layer],
        compact_to: &[ResidentLayer],
        metrics: &TimelineMetrics,
    ) {
        // We can simply reuse compact l0 logic. Use a different function name to indicate a different type of layer map modification.
        self.finish_compact_l0(compact_from, compact_to, metrics)
    }

    pub(crate) fn rewrite_layers(
        &mut self,
        rewrite_layers: &[(Layer, ResidentLayer)],
        drop_layers: &[Layer],
        metrics: &TimelineMetrics,
    ) {
        let mut updates = self.layer_map.batch_update();
        for (old_layer, new_layer) in rewrite_layers {
            debug_assert_eq!(
                old_layer.layer_desc().key_range,
                new_layer.layer_desc().key_range
            );
            debug_assert_eq!(
                old_layer.layer_desc().lsn_range,
                new_layer.layer_desc().lsn_range
            );

            // Safety: we may never rewrite the same file in-place.  Callers are responsible
            // for ensuring that they only rewrite layers after something changes the path,
            // such as an increment in the generation number.
            assert_ne!(old_layer.local_path(), new_layer.local_path());

            Self::delete_historic_layer(old_layer, &mut updates, &mut self.layer_fmgr);

            Self::insert_historic_layer(
                new_layer.as_ref().clone(),
                &mut updates,
                &mut self.layer_fmgr,
            );

            metrics.record_new_file_metrics(new_layer.layer_desc().file_size);
        }
        for l in drop_layers {
            Self::delete_historic_layer(l, &mut updates, &mut self.layer_fmgr);
        }
        updates.flush();
    }

    pub(crate) fn finish_gc_timeline(&mut self, gc_layers: &[Layer]) {
        let mut updates = self.layer_map.batch_update();
        for doomed_layer in gc_layers {
            Self::delete_historic_layer(doomed_layer, &mut updates, &mut self.layer_fmgr);
        }
        updates.flush()
    }

    #[cfg(test)]
    pub(crate) fn force_insert_layer(&mut self, layer: ResidentLayer) {
        let mut updates = self.layer_map.batch_update();
        Self::insert_historic_layer(layer.as_ref().clone(), &mut updates, &mut self.layer_fmgr);
        updates.flush()
    }

    /// Helper function to insert a layer into the layer map and file manager.
    fn insert_historic_layer(
        layer: Layer,
        updates: &mut BatchedUpdates<'_>,
        mapping: &mut LayerFileManager<Layer>,
    ) {
        updates.insert_historic(layer.layer_desc().clone());
        mapping.insert(layer);
    }

    /// Removes the layer from local FS (if present) and from memory.
    /// Remote storage is not affected by this operation.
    fn delete_historic_layer(
        // we cannot remove layers otherwise, since gc and compaction will race
        layer: &Layer,
        updates: &mut BatchedUpdates<'_>,
        mapping: &mut LayerFileManager<Layer>,
    ) {
        let desc = layer.layer_desc();

        // TODO Removing from the bottom of the layer map is expensive.
        //      Maybe instead discard all layer map historic versions that
        //      won't be needed for page reconstruction for this timeline,
        //      and mark what we can't delete yet as deleted from the layer
        //      map index without actually rebuilding the index.
        updates.remove_historic(desc);
        mapping.remove(layer);
        layer.delete_on_drop();
    }
}

pub(crate) struct LayerFileManager<T>(HashMap<PersistentLayerKey, T>);

impl<T> Default for LayerFileManager<T> {
    fn default() -> Self {
        Self(HashMap::default())
    }
}

impl<T: AsLayerDesc + Clone> LayerFileManager<T> {
    fn get_from_desc(&self, desc: &PersistentLayerDesc) -> Option<&T> {
        // The assumption for the `expect()` is that all code maintains the following invariant:
        // A layer's descriptor is present in the LayerMap => the LayerFileManager contains a layer for the descriptor.
        self.0.get(&desc.key())
    }

    pub(crate) fn insert(&mut self, layer: T) {
        let present = self.0.insert(layer.layer_desc().key(), layer.clone());
        if present.is_some() && cfg!(debug_assertions) {
            panic!("overwriting a layer: {:?}", layer.layer_desc())
        }
    }

    pub(crate) fn remove(&mut self, layer: &T) {
        let present = self.0.remove(&layer.layer_desc().key());
        if present.is_none() && cfg!(debug_assertions) {
            panic!(
                "removing layer that is not present in layer mapping: {:?}",
                layer.layer_desc()
            )
        }
    }
}
