// Package catalog_service provides embed-and-override adapters that expose
// the Milvus internal RootCoordCatalog / DataCoordCatalog contracts while
// routing engine-facing methods through a remote milvus-catalog gRPC client.
//
// Design shape (see plan fluffy-fluttering-prism.md §1):
//
//	type RootCoordAdapter struct {
//	    metastore.RootCoordCatalog  // embedded local kv catalog — default
//	    svc catalog.MilvusCatalogService  // remote gRPC client — overrides
//	}
//
// Go interface embedding promotes all methods of the local kv catalog onto
// the adapter. We then override ONLY the ~30 engine-facing methods to call
// svc instead. Any method we don't override — including future additions —
// falls through to local, which is the safe default: the local kv catalog
// talks directly to etcd and never silently corrupts cross-backend state.
//
// This pattern replaces the pre-rev-2 "hybrid dispatch 115 stubs" adapter.
package catalog_service

import (
	"context"

	catalog "github.com/milvus-io/milvus-catalog"
	"github.com/milvus-io/milvus-catalog/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metastore"
	"github.com/milvus-io/milvus/pkg/v3/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ============================================================================
// RootCoordAdapter
// ============================================================================

// RootCoordAdapter embeds a local RootCoordCatalog as the default backbone
// and overrides the ~18 engine-facing methods to route through svc.
type RootCoordAdapter struct {
	metastore.RootCoordCatalog
	svc catalog.MilvusCatalogService
}

// Compile-time interface satisfaction check. This passes automatically
// because the embedded RootCoordCatalog provides every method.
var _ metastore.RootCoordCatalog = (*RootCoordAdapter)(nil)

// NewRootCoordAdapter returns an adapter that wraps the given local catalog
// and routes engine-facing methods through the remote catalog service.
func NewRootCoordAdapter(remote catalog.MilvusCatalogService, local metastore.RootCoordCatalog) *RootCoordAdapter {
	return &RootCoordAdapter{
		RootCoordCatalog: local,
		svc:              remote,
	}
}

// Svc exposes the remote client for callers (e.g. adapter tests or feature
// flag branches) that need to check capability via type assertion.
func (a *RootCoordAdapter) Svc() catalog.MilvusCatalogService { return a.svc }

// --- Database (4 engine-facing: Create, List, Alter, Drop) ---

func (a *RootCoordAdapter) CreateDatabase(ctx context.Context, db *model.Database, ts typeutil.Timestamp) error {
	return a.svc.CreateDatabase(ctx, db, ts)
}

func (a *RootCoordAdapter) ListDatabases(ctx context.Context, ts typeutil.Timestamp) ([]*model.Database, error) {
	return a.svc.ListDatabases(ctx, ts)
}

func (a *RootCoordAdapter) AlterDatabase(ctx context.Context, newDB *model.Database, ts typeutil.Timestamp) error {
	return a.svc.AlterDatabase(ctx, newDB, ts)
}

func (a *RootCoordAdapter) DropDatabase(ctx context.Context, dbID int64, ts typeutil.Timestamp) error {
	return a.svc.DropDatabase(ctx, dbID, ts)
}

// --- Collection CRUD (7 engine-facing) ---
// AlterCollectionDB / CollectionExists fall through to local.
// CollectionExists is a dead method; keeping it on local is harmless.

func (a *RootCoordAdapter) DropCollection(ctx context.Context, collectionInfo *model.Collection, ts typeutil.Timestamp) error {
	return a.svc.DropCollection(ctx, collectionInfo, ts)
}

func (a *RootCoordAdapter) CreateCollection(ctx context.Context, coll *model.Collection, ts typeutil.Timestamp) error {
	return a.svc.CreateCollection(ctx, coll, ts)
}

func (a *RootCoordAdapter) GetCollectionByID(ctx context.Context, dbID int64, ts typeutil.Timestamp, collectionID typeutil.UniqueID) (*model.Collection, error) {
	return a.svc.GetCollectionByID(ctx, dbID, ts, collectionID)
}

func (a *RootCoordAdapter) GetCollectionByName(ctx context.Context, dbID int64, dbName string, collectionName string, ts typeutil.Timestamp) (*model.Collection, error) {
	return a.svc.GetCollectionByName(ctx, dbID, dbName, collectionName, ts)
}

func (a *RootCoordAdapter) ListCollections(ctx context.Context, dbID int64, ts typeutil.Timestamp) ([]*model.Collection, error) {
	return a.svc.ListCollections(ctx, dbID, ts)
}

// AlterCollection bridges metastore.AlterType to catalog.AlterType.
func (a *RootCoordAdapter) AlterCollection(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, alterType metastore.AlterType, ts typeutil.Timestamp, fieldModify bool) error {
	return a.svc.AlterCollection(ctx, oldColl, newColl, catalog.AlterType(alterType), ts, fieldModify)
}

// --- Partition (3) ---

func (a *RootCoordAdapter) CreatePartition(ctx context.Context, dbID int64, partition *model.Partition, ts typeutil.Timestamp) error {
	return a.svc.CreatePartition(ctx, dbID, partition, ts)
}

func (a *RootCoordAdapter) DropPartition(ctx context.Context, dbID int64, collectionID typeutil.UniqueID, partitionID typeutil.UniqueID, ts typeutil.Timestamp) error {
	return a.svc.DropPartition(ctx, dbID, collectionID, partitionID, ts)
}

func (a *RootCoordAdapter) AlterPartition(ctx context.Context, dbID int64, oldPart *model.Partition, newPart *model.Partition, alterType metastore.AlterType, ts typeutil.Timestamp) error {
	return a.svc.AlterPartition(ctx, dbID, oldPart, newPart, catalog.AlterType(alterType), ts)
}

// --- Alias (4) ---

func (a *RootCoordAdapter) CreateAlias(ctx context.Context, alias *model.Alias, ts typeutil.Timestamp) error {
	return a.svc.CreateAlias(ctx, alias, ts)
}

func (a *RootCoordAdapter) DropAlias(ctx context.Context, dbID int64, alias string, ts typeutil.Timestamp) error {
	return a.svc.DropAlias(ctx, dbID, alias, ts)
}

func (a *RootCoordAdapter) AlterAlias(ctx context.Context, alias *model.Alias, ts typeutil.Timestamp) error {
	return a.svc.AlterAlias(ctx, alias, ts)
}

func (a *RootCoordAdapter) ListAliases(ctx context.Context, dbID int64, ts typeutil.Timestamp) ([]*model.Alias, error) {
	return a.svc.ListAliases(ctx, dbID, ts)
}

// --- FileResource (3) ---

func (a *RootCoordAdapter) SaveFileResource(ctx context.Context, resource *internalpb.FileResourceInfo, version uint64) error {
	return a.svc.SaveFileResource(ctx, resource, version)
}

func (a *RootCoordAdapter) RemoveFileResource(ctx context.Context, resourceID int64, version uint64) error {
	return a.svc.RemoveFileResource(ctx, resourceID, version)
}

func (a *RootCoordAdapter) ListFileResource(ctx context.Context) ([]*internalpb.FileResourceInfo, uint64, error) {
	return a.svc.ListFileResource(ctx)
}

// --- RBAC (23 methods) all fall through to embedded local ---
// Credential / Role / Grant / Backup / PrivilegeGroup are never overridden.
// The embedded RootCoordCatalog (local kv catalog) handles them directly
// against etcd.

// --- Close ---

func (a *RootCoordAdapter) Close() {
	a.svc.Close()
	a.RootCoordCatalog.Close()
}

// ============================================================================
// DataCoordAdapter
// ============================================================================

// DataCoordAdapter embeds a local DataCoordCatalog and overrides the ~12
// engine-facing methods plus the special GcConfirm compose and the 4 atomic
// composite API methods (which are routed through type-assertion from the
// caller side via AtomicCapableCatalog).
type DataCoordAdapter struct {
	metastore.DataCoordCatalog
	svc catalog.MilvusCatalogService
}

// Compile-time interface satisfaction check.
var _ metastore.DataCoordCatalog = (*DataCoordAdapter)(nil)

// Compile-time extension interface check. DataCoordAdapter additionally
// satisfies AtomicCapableCatalog so DataCoord callers can type-assert.
var _ AtomicCapableCatalog = (*DataCoordAdapter)(nil)

// NewDataCoordAdapter returns an adapter that wraps the given local catalog
// and routes engine-facing methods through the remote catalog service.
func NewDataCoordAdapter(remote catalog.MilvusCatalogService, local metastore.DataCoordCatalog) *DataCoordAdapter {
	return &DataCoordAdapter{
		DataCoordCatalog: local,
		svc:              remote,
	}
}

// Svc exposes the remote client for capability checks / tests.
func (a *DataCoordAdapter) Svc() catalog.MilvusCatalogService { return a.svc }

// --- Segment (5) ---

func (a *DataCoordAdapter) ListSegments(ctx context.Context, collectionID int64) ([]*datapb.SegmentInfo, error) {
	return a.svc.ListSegments(ctx, collectionID)
}

func (a *DataCoordAdapter) AddSegment(ctx context.Context, segment *datapb.SegmentInfo) error {
	return a.svc.AddSegment(ctx, segment)
}

// AlterSegments bridges metastore.BinlogsIncrement to catalog.BinlogsIncrement.
func (a *DataCoordAdapter) AlterSegments(ctx context.Context, newSegments []*datapb.SegmentInfo, binlogs ...metastore.BinlogsIncrement) error {
	converted := make([]catalog.BinlogsIncrement, len(binlogs))
	for i, b := range binlogs {
		converted[i] = catalog.BinlogsIncrement{
			Segment: b.Segment,
			UpdateMask: catalog.BinlogsUpdateMask{
				WithoutBinlogs:       b.UpdateMask.WithoutBinlogs,
				WithoutDeltalogs:     b.UpdateMask.WithoutDeltalogs,
				WithoutStatslogs:     b.UpdateMask.WithoutStatslogs,
				WithoutBm25Statslogs: b.UpdateMask.WithoutBm25Statslogs,
			},
		}
	}
	return a.svc.AlterSegments(ctx, newSegments, converted...)
}

func (a *DataCoordAdapter) SaveDroppedSegmentsInBatch(ctx context.Context, segments []*datapb.SegmentInfo) error {
	return a.svc.SaveDroppedSegmentsInBatch(ctx, segments)
}

func (a *DataCoordAdapter) DropSegment(ctx context.Context, segment *datapb.SegmentInfo) error {
	return a.svc.DropSegment(ctx, segment)
}

// --- Index definition (4) ---
// CreateSegmentIndex / ListSegmentIndexes / AlterSegmentIndexes /
// DropSegmentIndex all fall through to local — per rev 2 the SegmentIndex
// layer is DataCoord-internal.

func (a *DataCoordAdapter) CreateIndex(ctx context.Context, index *model.Index) error {
	return a.svc.CreateIndex(ctx, index)
}

func (a *DataCoordAdapter) ListIndexes(ctx context.Context) ([]*model.Index, error) {
	return a.svc.ListIndexes(ctx)
}

func (a *DataCoordAdapter) AlterIndexes(ctx context.Context, newIndexes []*model.Index) error {
	return a.svc.AlterIndexes(ctx, newIndexes)
}

func (a *DataCoordAdapter) DropIndex(ctx context.Context, collID, dropIdxID typeutil.UniqueID) error {
	return a.svc.DropIndex(ctx, collID, dropIdxID)
}

// --- FileResource (3) ---

func (a *DataCoordAdapter) SaveFileResource(ctx context.Context, resource *internalpb.FileResourceInfo, version uint64) error {
	return a.svc.SaveFileResource(ctx, resource, version)
}

func (a *DataCoordAdapter) RemoveFileResource(ctx context.Context, resourceID int64, version uint64) error {
	return a.svc.RemoveFileResource(ctx, resourceID, version)
}

func (a *DataCoordAdapter) ListFileResource(ctx context.Context) ([]*internalpb.FileResourceInfo, uint64, error) {
	return a.svc.ListFileResource(ctx)
}

// --- GcConfirm: compose via remote ListSegments ---
//
// GcConfirm is named after FileResource but its semantics are "is the
// segment meta for this collection/partition fully GC'd?". Local kv catalog
// scans its own etcd prefix — which would be empty in UseCatalogService=true
// because segment meta lives on the remote side. That would make GcConfirm
// always return true, a silent GC corruption bug. See memory reference
// milvus_datacoord_gcconfirm_semantic_trap for full rationale.
//
// The fix is to compose GcConfirm at the adapter layer using
// svc.ListSegments() as the source of truth for segment presence.
func (a *DataCoordAdapter) GcConfirm(ctx context.Context, collectionID, partitionID typeutil.UniqueID) bool {
	segs, err := a.svc.ListSegments(ctx, collectionID)
	if err != nil {
		// Match local semantics: errors are treated as "not finished".
		return false
	}
	if partitionID == common.AllPartitionsID {
		return len(segs) == 0
	}
	for _, s := range segs {
		if s.GetPartitionID() == partitionID {
			return false
		}
	}
	return true
}

// --- Channel / WAL / Task State / Snapshot / Import / ExternalCollection ---
// All ~52 remaining methods fall through to the embedded local catalog.

// --- Atomic composite API (4 methods, via AtomicCapableCatalog interface) ---
//
// These are not part of metastore.DataCoordCatalog. DataCoord callers that
// want atomic semantics should do a type-assertion to AtomicCapableCatalog
// first; if it fails (e.g. UseCatalogService=false → caller has raw local
// catalog, no adapter), they must fall back to the legacy multi-step path.

func (a *DataCoordAdapter) CommitCompaction(ctx context.Context, req *catalogpb.CommitCompactionRequest) (*catalogpb.CommitCompactionResponse, error) {
	return a.svc.CommitCompaction(ctx, req)
}

func (a *DataCoordAdapter) CreateCollectionWithIndex(ctx context.Context, req *catalogpb.CreateCollectionWithIndexRequest) (*catalogpb.CreateCollectionWithIndexResponse, error) {
	return a.svc.CreateCollectionWithIndex(ctx, req)
}

func (a *DataCoordAdapter) CommitFlushSegment(ctx context.Context, req *catalogpb.CommitFlushSegmentRequest) (*catalogpb.CommitFlushSegmentResponse, error) {
	return a.svc.CommitFlushSegment(ctx, req)
}

func (a *DataCoordAdapter) GetCommitMarker(ctx context.Context, req *catalogpb.GetCommitMarkerRequest) (*catalogpb.GetCommitMarkerResponse, error) {
	return a.svc.GetCommitMarker(ctx, req)
}

// DataCoordCatalog has no Close method in the interface, so the adapter
// closes the remote svc via Svc() if callers explicitly need it. No Close()
// override is defined to avoid ambiguity.
