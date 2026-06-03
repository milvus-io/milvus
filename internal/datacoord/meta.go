// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package datacoord contains core functions in datacoord
package datacoord

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/metastore"
	datacoordkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type CompactionMeta interface {
	GetSegment(ctx context.Context, segID UniqueID) *SegmentInfo
	GetSegmentInfos(segIDs []UniqueID) []*SegmentInfo
	SelectSegments(ctx context.Context, filters ...SegmentFilter) []*SegmentInfo
	GetHealthySegment(ctx context.Context, segID UniqueID) *SegmentInfo
	UpdateSegmentsInfo(ctx context.Context, mutations map[int64][]SegmentOperator, newSegments ...*datapb.SegmentInfo) error
	SetSegmentsCompacting(ctx context.Context, segmentID []int64, compacting bool)
	CheckAndSetSegmentsCompacting(ctx context.Context, segmentIDs []int64) (bool, bool)
	CompleteCompactionMutation(ctx context.Context, t *datapb.CompactionTask, result *datapb.CompactionPlanResult) ([]*SegmentInfo, *segMetricMutation, error)
	ValidateSegmentStateBeforeCompleteCompactionMutation(t *datapb.CompactionTask) error
	CleanPartitionStatsInfo(ctx context.Context, info *datapb.PartitionStatsInfo) error

	SaveCompactionTask(ctx context.Context, task *datapb.CompactionTask) error
	DropCompactionTask(ctx context.Context, task *datapb.CompactionTask) error
	GetCompactionTasks(ctx context.Context) map[int64][]*datapb.CompactionTask
	GetCompactionTasksByTriggerID(ctx context.Context, triggerID int64) []*datapb.CompactionTask

	GetIndexMeta() *indexMeta
	GetAnalyzeMeta() *analyzeMeta
	GetPartitionStatsMeta() *partitionStatsMeta
	GetCompactionTaskMeta() *compactionTaskMeta
	GetFileResources(ctx context.Context, resourceIDs ...int64) ([]*internalpb.FileResourceInfo, error)
}

var _ CompactionMeta = (*meta)(nil)

type meta struct {
	ctx            context.Context
	catalog        metastore.DataCoordCatalog
	metaRootPath   string
	segmentPersist *SegmentTxnWrapper

	collections *typeutil.ConcurrentMap[UniqueID, *collectionInfo] // collection id to collection info

	segments *CachedSegmentsInfo // segment id to segment info

	channelCPs   *channelCPs // vChannel -> channel checkpoint/see position
	chunkManager storage.ChunkManager

	indexMeta                     *indexMeta
	analyzeMeta                   *analyzeMeta
	partitionStatsMeta            *partitionStatsMeta
	compactionTaskMeta            *compactionTaskMeta
	statsTaskMeta                 *statsTaskMeta
	externalCollectionRefreshMeta *externalCollectionRefreshMeta

	// File Resource Meta
	resourceIDMap   map[int64]*internalpb.FileResourceInfo // id -> info
	resourceVersion uint64
	resourceLock    lock.RWMutex
	// Snapshot Meta
	snapshotMeta *snapshotMeta
}

func (m *meta) GetIndexMeta() *indexMeta {
	return m.indexMeta
}

func (m *meta) GetAnalyzeMeta() *analyzeMeta {
	return m.analyzeMeta
}

func (m *meta) GetPartitionStatsMeta() *partitionStatsMeta {
	return m.partitionStatsMeta
}

func (m *meta) GetCompactionTaskMeta() *compactionTaskMeta {
	return m.compactionTaskMeta
}

func (m *meta) GetSnapshotMeta() *snapshotMeta {
	return m.snapshotMeta
}

type channelCPs struct {
	lock.RWMutex
	checkpoints map[string]*msgpb.MsgPosition
	cond        *syncutil.ContextCond
}

func newChannelCps() *channelCPs {
	cp := &channelCPs{
		checkpoints: make(map[string]*msgpb.MsgPosition),
	}
	// use the same lock as channelCPs
	cp.cond = syncutil.NewContextCond(&cp.RWMutex)
	return cp
}

type segmentMetricStateChange map[string]map[string]map[string]map[string]map[string]int

// A local cache of segment metric update. Must call commit() to take effect.
type segMetricMutation struct {
	stateChange             segmentMetricStateChange // segment level -> state -> isSorted -> storageVersion -> format change count.
	deferSegmentLabelChange bool                     // UpdateSegmentsInfo computes label changes from original and final segment state.
	rowCountChange          int64                    // Change in # of rows.
	rowCountAccChange       int64                    // Total # of historical added rows, accumulated.
}

type collectionInfo struct {
	ID             int64
	Schema         *schemapb.CollectionSchema
	Partitions     []int64
	StartPositions []*commonpb.KeyDataPair
	Properties     map[string]string
	CreatedAt      Timestamp
	DatabaseName   string
	DatabaseID     int64
	VChannelNames  []string
}

const (
	segmentMetricFormatLegacy  = "legacy"
	segmentMetricFormatUnknown = "unknown"
	segmentMetricFormatMixed   = "mixed"
)

func segmentMetricFormatLabel(segment *SegmentInfo) string {
	if segment == nil {
		return segmentMetricFormatUnknown
	}

	formats := make(map[string]struct{})
	for _, fieldBinlog := range segment.GetBinlogs() {
		format := strings.TrimSpace(fieldBinlog.GetFormat())
		if format == "" {
			continue
		}
		formats[format] = struct{}{}
	}
	if len(formats) == 0 {
		if segment.GetStorageVersion() < storage.StorageV2 {
			return segmentMetricFormatLegacy
		}
		return segmentMetricFormatUnknown
	}
	if len(formats) > 1 {
		return segmentMetricFormatMixed
	}
	for format := range formats {
		return format
	}
	return segmentMetricFormatUnknown
}

func segmentMetricLabelValues(segment *SegmentInfo) []string {
	return []string{
		segment.GetState().String(),
		segment.GetLevel().String(),
		getSortStatus(segment.GetIsSorted()),
		fmt.Sprint(segment.GetStorageVersion()),
		segmentMetricFormatLabel(segment),
	}
}

// IsExternal returns true when the collection schema references an external source or spec.
func (c *collectionInfo) IsExternal() bool {
	if c == nil {
		return false
	}
	if c.Schema == nil {
		return false
	}
	return typeutil.IsExternalCollection(c.Schema)
}

type dbInfo struct {
	ID         int64
	Name       string
	Properties []*commonpb.KeyValuePair
}

// showCollectionIDs retrieves all collection IDs from RootCoord with retry on ErrServiceUnimplemented.
func showCollectionIDs(ctx context.Context, broker broker.Broker) ([]int64, error) {
	var (
		err  error
		resp *rootcoordpb.ShowCollectionIDsResponse
	)
	retryErr := retry.Handle(ctx, func() (bool, error) {
		resp, err = broker.ShowCollectionIDs(ctx)
		if errors.Is(err, merr.ErrServiceUnimplemented) {
			return true, err
		}
		return false, err
	})
	if retryErr != nil {
		return nil, retryErr
	}

	collectionIDs := make([]int64, 0, 4096)
	for _, collections := range resp.GetDbCollections() {
		collectionIDs = append(collectionIDs, collections.GetCollectionIDs()...)
	}
	return collectionIDs, nil
}

// NewMeta creates meta from provided `kv.TxnKV`
func (m *meta) joinMetaRootPath(key string) string {
	if m.metaRootPath == "" {
		return key
	}
	return strings.TrimSuffix(m.metaRootPath, "/") + "/" + key
}

func (m *meta) segmentKey(collectionID, partitionID, segmentID int64) string {
	return m.joinMetaRootPath(segmentKey(collectionID, partitionID, segmentID))
}

func (m *meta) segmentCollectionPrefix(collectionID int64) string {
	return m.joinMetaRootPath(fmt.Sprintf("%s%d/", segmentMetaPrefix, collectionID))
}

func newMeta(ctx context.Context, catalog metastore.DataCoordCatalog, chunkManager storage.ChunkManager, broker broker.Broker, segmentPersist *SegmentTxnWrapper, metaRootPath string) (*meta, error) {
	segmentPersist = segmentPersist.WithMetaRootPath(metaRootPath)

	// Fetch collection IDs first so both reloadFromKV and indexMeta can use them for per-collection loading.
	collectionIDs, err := showCollectionIDs(ctx, broker)
	if err != nil {
		return nil, err
	}

	var (
		im   *indexMeta
		am   *analyzeMeta
		psm  *partitionStatsMeta
		ctm  *compactionTaskMeta
		stm  *statsTaskMeta
		ecrm *externalCollectionRefreshMeta
		spm  *snapshotMeta
	)

	// Construct meta struct first so reloadFromKV can run in parallel with sub-meta loading.
	// reloadFromKV uses m.catalog/m.segments/m.channelCPs which are independent of sub-metas.
	mt := &meta{
		ctx:             ctx,
		catalog:         catalog,
		metaRootPath:    metaRootPath,
		segmentPersist:  segmentPersist,
		collections:     typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		segments:        NewCachedSegmentsInfo(),
		channelCPs:      newChannelCps(),
		chunkManager:    chunkManager,
		resourceIDMap:   make(map[int64]*internalpb.FileResourceInfo),
		resourceVersion: 0,
		resourceLock:    lock.RWMutex{},
	}

	g, _ := errgroup.WithContext(ctx)

	g.Go(func() error {
		var err error
		im, err = newIndexMeta(ctx, catalog, collectionIDs)
		return err
	})

	g.Go(func() error {
		var err error
		am, err = newAnalyzeMeta(ctx, catalog)
		return err
	})

	g.Go(func() error {
		var err error
		psm, err = newPartitionStatsMeta(ctx, catalog)
		return err
	})

	g.Go(func() error {
		var err error
		ctm, err = newCompactionTaskMeta(ctx, catalog)
		return err
	})

	g.Go(func() error {
		var err error
		stm, err = newStatsTaskMeta(ctx, catalog)
		return err
	})

	g.Go(func() error {
		var err error
		ecrm, err = newExternalCollectionRefreshMeta(ctx, catalog)
		return err
	})

	g.Go(func() error {
		var err error
		spm, err = newSnapshotMeta(ctx, catalog, chunkManager)
		return err
	})

	// reloadFromKV (ListSegments, ListChannelCheckpoint) runs in parallel with sub-meta loading.
	// It only uses mt.catalog/mt.segments/mt.channelCPs, which are independent of sub-metas.
	g.Go(func() error {
		return mt.reloadFromKV(ctx, collectionIDs)
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Assign sub-metas after all goroutines complete
	mt.indexMeta = im
	mt.analyzeMeta = am
	mt.partitionStatsMeta = psm
	mt.compactionTaskMeta = ctm
	mt.statsTaskMeta = stm
	mt.externalCollectionRefreshMeta = ecrm
	mt.snapshotMeta = spm

	return mt, nil
}

// reloadFromKV loads meta from KV storage
func (m *meta) reloadFromKV(ctx context.Context, collectionIDs []int64) error {
	record := timerecord.NewTimeRecorder("datacoord")

	pool := conc.NewPool[any](paramtable.Get().MetaStoreCfg.ReadConcurrency.GetAsInt())
	defer pool.Release()
	type scanResult struct {
		segments []*datapb.SegmentInfo
		versions []int64
	}
	futures := make([]*conc.Future[any], 0, len(collectionIDs))
	collectionResults := make([]scanResult, len(collectionIDs))
	rootPath := ""
	if m.chunkManager != nil {
		rootPath = m.chunkManager.RootPath()
	}
	for i, collectionID := range collectionIDs {
		i := i
		collectionID := collectionID
		futures = append(futures, pool.Submit(func() (any, error) {
			prefix := m.segmentCollectionPrefix(collectionID)
			values, versions, err := m.segmentPersist.Scan(m.ctx, prefix)
			if err != nil {
				return nil, err
			}
			// Backward-compat: older clusters persist binlog fields as separate KVs and
			// strip them from the segment proto. A segment with rows but no embedded
			// binlogs indicates such legacy data; stitch binlogs from their side-prefix KVs.
			needsBinlogFallback := false
			for _, seg := range values {
				if seg.GetNumOfRows() > 0 && len(seg.GetBinlogs()) == 0 &&
					len(seg.GetDeltalogs()) == 0 && len(seg.GetStatslogs()) == 0 {
					needsBinlogFallback = true
					break
				}
			}
			if needsBinlogFallback {
				insertMap, err := m.scanLegacyBinlogs(datacoordkv.SegmentBinlogPathPrefix, collectionID)
				if err != nil {
					return nil, err
				}
				deltaMap, err := m.scanLegacyBinlogs(datacoordkv.SegmentDeltalogPathPrefix, collectionID)
				if err != nil {
					return nil, err
				}
				statsMap, err := m.scanLegacyBinlogs(datacoordkv.SegmentStatslogPathPrefix, collectionID)
				if err != nil {
					return nil, err
				}
				bm25Map, err := m.scanLegacyBinlogs(datacoordkv.SegmentBM25logPathPrefix, collectionID)
				if err != nil {
					return nil, err
				}
				for _, seg := range values {
					if len(seg.GetBinlogs()) > 0 || len(seg.GetDeltalogs()) > 0 ||
						len(seg.GetStatslogs()) > 0 || len(seg.GetBm25Statslogs()) > 0 {
						continue
					}
					id := seg.GetID()
					seg.Binlogs = insertMap[id]
					seg.Deltalogs = deltaMap[id]
					seg.Statslogs = statsMap[id]
					seg.Bm25Statslogs = bm25Map[id]
				}
			}
			// Restore full paths for text index logs (filenames-only in etcd).
			for _, seg := range values {
				if len(seg.GetTextStatsLogs()) == 0 {
					continue
				}
				metautil.BuildTextLogPaths(
					rootPath,
					seg.GetCollectionID(),
					seg.GetPartitionID(),
					seg.GetID(),
					seg.GetTextStatsLogs(),
				)
			}
			collectionResults[i] = scanResult{segments: values, versions: versions}
			return nil, nil
		}))
	}
	if err := conc.AwaitAll(futures...); err != nil {
		return err
	}

	mlog.Info(ctx, "datacoord show segments done", mlog.Duration("dur", record.RecordSpan()))

	metrics.DataCoordNumCollections.WithLabelValues().Set(0)
	metrics.DataCoordNumSegments.Reset()
	numStoredRows := int64(0)
	numSegments := 0
	for _, result := range collectionResults {
		numSegments += len(result.segments)
		for j, segment := range result.segments {
			m.segments.SetSegment(segment.ID, NewSegmentInfo(segment), result.versions[j])
			metrics.DataCoordNumSegments.WithLabelValues(segment.GetState().String(), segment.GetLevel().String(), getSortStatus(segment.GetIsSorted()), fmt.Sprint(segment.GetStorageVersion())).Inc()
			if segment.State == commonpb.SegmentState_Flushed {
				numStoredRows += segment.NumOfRows

				insertFileNum := 0
				for _, fieldBinlog := range segment.GetBinlogs() {
					insertFileNum += len(fieldBinlog.GetBinlogs())
				}
				metrics.FlushedSegmentFileNum.WithLabelValues(metrics.InsertFileLabel).Observe(float64(insertFileNum))

				statFileNum := 0
				for _, fieldBinlog := range segment.GetStatslogs() {
					statFileNum += len(fieldBinlog.GetBinlogs())
				}
				metrics.FlushedSegmentFileNum.WithLabelValues(metrics.StatFileLabel).Observe(float64(statFileNum))

				deleteFileNum := 0
				for _, filedBinlog := range segment.GetDeltalogs() {
					deleteFileNum += len(filedBinlog.GetBinlogs())
				}
				metrics.FlushedSegmentFileNum.WithLabelValues(metrics.DeleteFileLabel).Observe(float64(deleteFileNum))
			}
		}
	}

	channelCPs, err := m.catalog.ListChannelCheckpoint(m.ctx)
	if err != nil {
		return err
	}
	for vChannel, pos := range channelCPs {
		// for 2.2.2 issue https://github.com/milvus-io/milvus/issues/22181
		pos.ChannelName = vChannel
		m.channelCPs.checkpoints[vChannel] = pos
		if !funcutil.IsDroppedChannelCheckpoint(pos) {
			// Should not be set as metric since it's a tombstone value.
			ts, _ := tsoutil.ParseTS(pos.Timestamp)
			metrics.DataCoordCheckpointUnixSeconds.WithLabelValues(paramtable.GetStringNodeID(), vChannel).
				Set(float64(ts.Unix()))
		}
	}

	mlog.Info(ctx, "DataCoord meta reloadFromKV done", mlog.Int("numSegments", numSegments), mlog.Duration("duration", record.ElapseSpan()))
	return nil
}

func (m *meta) reloadCollectionsFromRootcoord(ctx context.Context, broker broker.Broker) error {
	resp, err := broker.ListDatabases(ctx)
	if err != nil {
		return err
	}
	for _, dbName := range resp.GetDbNames() {
		collectionsResp, err := broker.ShowCollections(ctx, dbName)
		if err != nil {
			return err
		}
		for _, collectionID := range collectionsResp.GetCollectionIds() {
			descResp, err := broker.DescribeCollectionInternal(ctx, collectionID)
			if err != nil {
				return err
			}
			partitionIDs, err := broker.ShowPartitionsInternal(ctx, collectionID)
			if err != nil {
				return err
			}
			collection := &collectionInfo{
				ID:             collectionID,
				Schema:         descResp.GetSchema(),
				Partitions:     partitionIDs,
				StartPositions: descResp.GetStartPositions(),
				Properties:     funcutil.KeyValuePair2Map(descResp.GetProperties()),
				CreatedAt:      descResp.GetCreatedTimestamp(),
				DatabaseName:   descResp.GetDbName(),
				DatabaseID:     descResp.GetDbId(),
				VChannelNames:  descResp.GetVirtualChannelNames(),
			}
			m.AddCollection(collection)
		}
	}
	return nil
}

// AddCollection adds a collection into meta
// Note that collection info is just for caching and will not be set into etcd from datacoord
func (m *meta) AddCollection(collection *collectionInfo) {
	mlog.Info(context.TODO(), "meta update: add collection", mlog.Int64("collectionID", collection.ID))
	m.collections.Insert(collection.ID, collection)
	metrics.DataCoordNumCollections.WithLabelValues().Set(float64(m.collections.Len()))
	mlog.Info(context.TODO(), "meta update: add collection - complete", mlog.Int64("collectionID", collection.ID))
}

// DropCollection drop a collection from meta
func (m *meta) DropCollection(collectionID int64) {
	mlog.Info(context.TODO(), "meta update: drop collection", mlog.Int64("collectionID", collectionID))
	if _, ok := m.collections.GetAndRemove(collectionID); ok {
		metrics.CleanupDataCoordWithCollectionID(collectionID)
		metrics.DataCoordNumCollections.WithLabelValues().Set(float64(m.collections.Len()))
		mlog.Info(context.TODO(), "meta update: drop collection - complete", mlog.Int64("collectionID", collectionID))
	}
}

// GetCollection returns collection info with provided collection id from local cache
func (m *meta) GetCollection(collectionID UniqueID) *collectionInfo {
	collection, ok := m.collections.Get(collectionID)
	if !ok {
		return nil
	}
	return collection
}

// GetCollections returns collections from local cache
func (m *meta) GetCollections() []*collectionInfo {
	return m.collections.Values()
}

func (m *meta) GetClonedCollectionInfo(collectionID UniqueID) *collectionInfo {
	coll, ok := m.collections.Get(collectionID)
	if !ok {
		return nil
	}

	clonedProperties := make(map[string]string)
	maps.Copy(clonedProperties, coll.Properties)
	cloneColl := &collectionInfo{
		ID:             coll.ID,
		Schema:         proto.Clone(coll.Schema).(*schemapb.CollectionSchema),
		Partitions:     coll.Partitions,
		StartPositions: common.CloneKeyDataPairs(coll.StartPositions),
		Properties:     clonedProperties,
		DatabaseName:   coll.DatabaseName,
		DatabaseID:     coll.DatabaseID,
		VChannelNames:  coll.VChannelNames,
	}

	return cloneColl
}

// GetSegmentsChanPart returns segments organized in Channel-Partition dimension with selector applied
// TODO: Move this function to the compaction module after reorganizing the DataCoord modules.
func GetSegmentsChanPart(m *meta, collectionID int64, filters ...SegmentFilter) []*chanPartSegments {
	type dim struct {
		partitionID int64
		channelName string
	}

	mDimEntry := make(map[dim]*chanPartSegments)

	filters = append(filters, WithCollection(collectionID))
	candidates := m.SelectSegments(context.Background(), filters...)
	for _, si := range candidates {
		d := dim{si.PartitionID, si.InsertChannel}
		entry, ok := mDimEntry[d]
		if !ok {
			entry = &chanPartSegments{
				collectionID: si.CollectionID,
				partitionID:  si.PartitionID,
				channelName:  si.InsertChannel,
			}
			mDimEntry[d] = entry
		}
		entry.segments = append(entry.segments, si)
	}
	result := make([]*chanPartSegments, 0, len(mDimEntry))
	for _, entry := range mDimEntry {
		result = append(result, entry)
	}
	return result
}

// GetNumRowsOfCollection returns total rows count of segments belongs to provided collection
func (m *meta) GetNumRowsOfCollection(ctx context.Context, collectionID UniqueID) int64 {
	var ret int64
	segments := m.SelectSegments(ctx, WithCollection(collectionID), SegmentFilterFunc(isSegmentHealthy))
	for _, segment := range segments {
		ret += segment.GetNumOfRows()
	}
	return ret
}

func getBinlogFileCount(s *datapb.SegmentInfo) int {
	statsFieldFn := func(fieldBinlogs []*datapb.FieldBinlog) int {
		cnt := 0
		for _, fbs := range fieldBinlogs {
			cnt += len(fbs.Binlogs)
		}
		return cnt
	}

	cnt := 0
	cnt += statsFieldFn(s.GetBinlogs())
	cnt += statsFieldFn(s.GetStatslogs())
	cnt += statsFieldFn(s.GetDeltalogs())
	return cnt
}

func (m *meta) GetQuotaInfo() *metricsinfo.DataCoordQuotaMetrics {
	info := &metricsinfo.DataCoordQuotaMetrics{}
	collectionBinlogSize := make(map[UniqueID]int64)
	partitionBinlogSize := make(map[UniqueID]map[UniqueID]int64)
	collectionRowsNum := make(map[UniqueID]map[commonpb.SegmentState]int64)
	// collection id => l0 delta entry count
	collectionL0RowCounts := make(map[UniqueID]int64)

	segments := m.segments.GetSegments()
	var total int64
	storedBinlogSize := make(map[string]map[string]int64) // map[collectionID]map[segment_state]size
	binlogFileCount := make(map[string]int64)             // map[collectionID]count
	coll2DbName := make(map[string]string)

	for _, segment := range segments {
		segmentSize := segment.getSegmentSize()
		if isSegmentHealthy(segment) && !segment.GetIsImporting() {
			total += segmentSize
			collectionBinlogSize[segment.GetCollectionID()] += segmentSize

			partBinlogSize, ok := partitionBinlogSize[segment.GetCollectionID()]
			if !ok {
				partBinlogSize = make(map[int64]int64)
				partitionBinlogSize[segment.GetCollectionID()] = partBinlogSize
			}
			partBinlogSize[segment.GetPartitionID()] += segmentSize

			coll, ok := m.collections.Get(segment.GetCollectionID())
			if ok {
				collIDStr := strconv.FormatInt(segment.GetCollectionID(), 10)
				coll2DbName[collIDStr] = coll.DatabaseName
				if _, ok := storedBinlogSize[collIDStr]; !ok {
					storedBinlogSize[collIDStr] = make(map[string]int64)
				}

				storedBinlogSize[collIDStr][segment.GetState().String()] += segmentSize
				binlogFileCount[collIDStr] += int64(getBinlogFileCount(segment.SegmentInfo))
			}

			if _, ok := collectionRowsNum[segment.GetCollectionID()]; !ok {
				collectionRowsNum[segment.GetCollectionID()] = make(map[commonpb.SegmentState]int64)
			}
			collectionRowsNum[segment.GetCollectionID()][segment.GetState()] += segment.GetNumOfRows()

			if segment.GetLevel() == datapb.SegmentLevel_L0 {
				collectionL0RowCounts[segment.GetCollectionID()] += segment.getDeltaCount()
			}
		}
	}

	// Reset to remove dropped collection
	metrics.DataCoordStoredBinlogSize.Reset()
	for collectionID, state2Size := range storedBinlogSize {
		for state, size := range state2Size {
			metrics.DataCoordStoredBinlogSize.WithLabelValues(coll2DbName[collectionID], collectionID, state).Set(float64(size))
		}
	}
	// Reset to remove dropped collection
	metrics.DataCoordSegmentBinLogFileCount.Reset()
	for collectionID, size := range binlogFileCount {
		metrics.DataCoordSegmentBinLogFileCount.WithLabelValues(collectionID).Set(float64(size))
	}

	metrics.DataCoordNumStoredRows.Reset()
	for collectionID, statesRows := range collectionRowsNum {
		coll, ok := m.collections.Get(collectionID)
		if ok {
			for state, rows := range statesRows {
				metrics.DataCoordNumStoredRows.WithLabelValues(coll.DatabaseName, strconv.FormatInt(collectionID, 10), coll.Schema.GetName(), state.String()).Set(float64(rows))
			}
		}
	}

	metrics.DataCoordL0DeleteEntriesNum.Reset()
	for collectionID, entriesNum := range collectionL0RowCounts {
		coll, ok := m.collections.Get(collectionID)
		if ok {
			metrics.DataCoordL0DeleteEntriesNum.WithLabelValues(coll.DatabaseName, strconv.FormatInt(collectionID, 10)).Set(float64(entriesNum))
		}
	}

	info.TotalBinlogSize = total
	info.CollectionBinlogSize = collectionBinlogSize
	info.PartitionsBinlogSize = partitionBinlogSize
	info.CollectionL0RowCount = collectionL0RowCounts

	return info
}

func (m *meta) GetAllCollectionNumRows() map[int64]int64 {
	ret := make(map[int64]int64, m.collections.Len())
	segments := m.segments.GetSegments()
	for _, segment := range segments {
		if isSegmentHealthy(segment) {
			ret[segment.GetCollectionID()] += segment.GetNumOfRows()
		}
	}
	return ret
}

// AddSegment records segment info, persisting info into kv store.
// If the segment already exists in etcd, the operation is a no-op.
func (m *meta) AddSegment(ctx context.Context, segment *SegmentInfo) error {
	log := log.Ctx(ctx).With(zap.String("channel", segment.GetInsertChannel()))
	log.Info("meta update: adding segment - Start", zap.Int64("segmentID", segment.GetID()))

	key := m.segmentKey(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())
	txn := m.segmentPersist.Txn(ctx)
	if err := txn.Insert(key, segment.SegmentInfo); err != nil {
		log.Error("meta update: adding segment failed to build write",
			zap.Int64("segmentID", segment.GetID()), zap.Error(err))
		return err
	}
	results, err := txn.Commit()
	if err != nil {
		if errors.Is(err, ErrKeyAlreadyExists) {
			log.Info("segment already exists, ignore the operation", zap.Int64("segmentID", segment.ID))
			return nil
		}
		log.Error("meta update: adding segment failed",
			zap.Int64("segmentID", segment.GetID()),
			zap.Error(err))
		return err
	}
	m.segments.SetSegment(segment.GetID(), segment, results[0].Version)

	metrics.DataCoordNumSegments.WithLabelValues(segmentMetricLabelValues(segment)...).Inc()
	mlog.Info(context.TODO(), "meta update: adding segment - complete", mlog.Int64("segmentID", segment.GetID()))
	return nil
}

// DropSegment remove segment, etcd persistence also removed
func (m *meta) DropSegment(ctx context.Context, segment *SegmentInfo) error {
	log := log.Ctx(ctx)
	segmentID := segment.GetID()
	log.Debug("meta update: dropping segment", zap.Int64("segmentID", segmentID))
	key := m.segmentKey(segment.GetCollectionID(), segment.GetPartitionID(), segmentID)
	txn := m.segmentPersist.Txn(ctx)
	txn.Delete(key, segment.SegmentInfo)
	results, err := txn.Commit()
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			log.Info("meta update: dropping segment - already deleted", zap.Int64("segmentID", segmentID))
			m.segments.DropSegment(segmentID, math.MaxInt64)
			return nil
		}
		log.Warn("meta update: dropping segment failed",
			zap.Int64("segmentID", segmentID),
			zap.Error(err))
		return err
	}
	metrics.DataCoordNumSegments.WithLabelValues(segmentMetricLabelValues(segment)...).Dec()

	m.segments.DropSegment(segmentID, results[0].Version)
	log.Info("meta update: dropping segment - complete",
		zap.Int64("segmentID", segmentID))
	return nil
}

// scanLegacyBinlogs reads binlog KVs under a legacy side-prefix (e.g. SegmentBinlogPathPrefix)
// for a single collection via segmentPersist.ScanRaw, unmarshals each as FieldBinlog,
// and returns them grouped by segment ID. Called during reloadFromKV to stitch
// binlogs from the legacy side-prefix storage shape (which current writes still use).
//
// Legacy key shape (see kv_catalog.parseBinlogKey):
//
//	<rootPath>/<binlogPrefix>/<collID>/<partID>/<segID>/<fieldID>
//
// Only the segID (second-to-last segment of the key) is needed for grouping.
func (m *meta) scanLegacyBinlogs(pathPrefix string, collectionID int64) (map[int64][]*datapb.FieldBinlog, error) {
	prefix := m.joinMetaRootPath(fmt.Sprintf("%s/%d/", pathPrefix, collectionID))
	keys, values, _, err := m.segmentPersist.ScanRaw(m.ctx, prefix)
	if err != nil {
		return nil, err
	}
	result := make(map[int64][]*datapb.FieldBinlog, len(keys))
	for i, key := range keys {
		parts := strings.Split(key, "/")
		if len(parts) < 2 {
			return nil, fmt.Errorf("invalid legacy binlog key: %s", key)
		}
		segID, err := strconv.ParseInt(parts[len(parts)-2], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid legacy binlog key %q: %w", key, err)
		}
		fb := &datapb.FieldBinlog{}
		if err := proto.Unmarshal(values[i], fb); err != nil {
			return nil, fmt.Errorf("unmarshal FieldBinlog at %q: %w", key, err)
		}
		// Preserve the old-version compatibility fix from kv_catalog.listBinlogs.
		for j, b := range fb.GetBinlogs() {
			if b.GetMemorySize() == 0 {
				fb.Binlogs[j].MemorySize = b.GetLogSize()
			}
		}
		result[segID] = append(result[segID], fb)
	}
	return result, nil
}

// PruneSegment removes a dropped segment's tombstone entry from the cache.
// Only call this once the segment is fully reclaimed (e.g. after GC has removed
// its object-storage files and the drop is older than dropTolerance), so no
// in-flight write can still land with a pre-drop version.
func (m *meta) PruneSegment(segmentID UniqueID) {
	m.segments.PruneSegment(segmentID)
}

// GetHealthySegment returns segment info with provided id
// if not segment is found, nil will be returned
func (m *meta) GetHealthySegment(ctx context.Context, segID UniqueID) *SegmentInfo {
	segment := m.segments.GetSegment(segID)
	if segment != nil && isSegmentHealthy(segment) {
		return segment
	}
	return nil
}

// Get segments By filter function
func (m *meta) GetSegments(segIDs []UniqueID, filterFunc SegmentInfoSelector) []UniqueID {
	var result []UniqueID
	for _, id := range segIDs {
		segment := m.segments.GetSegment(id)
		if segment != nil && filterFunc(segment) {
			result = append(result, id)
		}
	}
	return result
}

func (m *meta) GetSegmentInfos(segIDs []UniqueID) []*SegmentInfo {
	var result []*SegmentInfo
	for _, id := range segIDs {
		segment := m.segments.GetSegment(id)
		if segment != nil {
			result = append(result, segment)
		}
	}
	return result
}

// GetSegment returns segment info with provided id
// include the unhealthy segment
// if not segment is found, nil will be returned
func (m *meta) GetSegment(ctx context.Context, segID UniqueID) *SegmentInfo {
	return m.segments.GetSegment(segID)
}

// GetAllSegmentsUnsafe returns all segments
func (m *meta) GetAllSegmentsUnsafe() []*SegmentInfo {
	return m.segments.GetSegments()
}

func (m *meta) GetSegmentsTotalNumRows(segmentIDs []UniqueID) int64 {
	var sum int64 = 0
	for _, segmentID := range segmentIDs {
		segment := m.segments.GetSegment(segmentID)
		if segment == nil {
			mlog.Warn(context.TODO(), "cannot find segment", mlog.Int64("segmentID", segmentID))
			continue
		}
		sum += segment.GetNumOfRows()
	}
	return sum
}

func (m *meta) GetSegmentsChannels(segmentIDs []UniqueID) (map[int64]string, error) {
	segChannels := make(map[int64]string)
	for _, segmentID := range segmentIDs {
		segment := m.segments.GetSegment(segmentID)
		if segment == nil {
			return nil, merr.WrapErrServiceInternalMsg("cannot find segment %d", segmentID)
		}
		segChannels[segmentID] = segment.GetInsertChannel()
	}
	return segChannels, nil
}

// segmentCASMaxRetries caps the retry loop for CAS-based single-segment updates.
// Contention on a single segment is rare in practice (one compaction / flush
// path owns it at a time), so a small cap is enough to cover the cache-update
// propagation window after a conflicting commit.
const segmentCASMaxRetries = 20

// updateSegmentCAS read-modify-writes a single segment with CAS on the cache
// version (== etcd ModRevision). mutate runs against a cloned in-memory
// SegmentInfo (with binlogs stitched from the cache); returning false skips
// the write. On CAS conflict the loop re-reads and retries. The mutator also
// returns the BinlogIncrement — the FieldBinlogs whose side-prefix KVs must be
// rewritten; an empty increment means a state-only write.
func (m *meta) updateSegmentCAS(ctx context.Context, segmentID UniqueID, mutate SegmentOperator) (*SegmentInfo, error) {
	ctx = m.opContext(ctx)
	var out *SegmentInfo
	err := retry.Do(ctx, func() error {
		cur, version := m.segments.GetSegmentWithVersion(segmentID)
		if cur == nil {
			return retry.Unrecoverable(merr.WrapErrSegmentNotFound(segmentID))
		}
		clone := cur.Clone()
		inc, ok := mutate(clone)
		if !ok {
			out = cur
			return nil
		}
		key := m.segmentKey(clone.GetCollectionID(), clone.GetPartitionID(), segmentID)
		txn := m.segmentPersist.Txn(ctx)
		if err := txn.Update(key, clone.SegmentInfo, version, inc); err != nil {
			return retry.Unrecoverable(err)
		}
		results, err := txn.Commit()
		if err != nil {
			return err
		}
		m.segments.SetSegment(segmentID, clone, results[0].Version)
		out = clone
		return nil
	}, retry.Attempts(segmentCASMaxRetries), retry.Sleep(0), retry.RetryErr(isCASFailed))
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (m *meta) opContext(ctx context.Context) context.Context {
	if ctx != nil {
		return ctx
	}
	if m.ctx != nil {
		return m.ctx
	}
	return context.TODO()
}

// isCASFailed is the retry.RetryErr predicate that limits CAS loops to
// retrying on version-mismatch commits only.
func isCASFailed(err error) bool { return errors.Is(err, ErrCASFailed) }

// SetState setting segment with provided ID state
func (m *meta) SetState(ctx context.Context, segmentID UniqueID, targetState commonpb.SegmentState) error {
	log := log.Ctx(context.TODO())
	log.Debug("meta update: setting segment state",
		zap.Int64("segmentID", segmentID),
		zap.Any("target state", targetState))

	var oldState commonpb.SegmentState
	updated, err := m.updateSegmentCAS(ctx, segmentID, func(s *SegmentInfo) (BinlogIncrement, bool) {
		oldState = s.GetState()
		s.State = targetState
		if targetState == commonpb.SegmentState_Dropped {
			s.DroppedAt = uint64(time.Now().UnixNano())
		}
		return BinlogIncrement{}, true
	})
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) && targetState == commonpb.SegmentState_Dropped {
			return nil
		}
		if errors.Is(err, merr.ErrSegmentNotFound) {
			return err
		}
		log.Warn("meta update: setting segment state - failed to alter segments",
			zap.Int64("segmentID", segmentID),
			zap.String("target state", targetState.String()),
			zap.Error(err))
		return err
	}
	if updated != nil && oldState != updated.GetState() {
		metricMutation := segMetricMutation{stateChange: make(map[string]map[string]map[string]map[string]int)}
		metricMutation.append(oldState, updated.GetState(), updated.GetLevel(), updated.GetIsSorted(), updated.GetStorageVersion(), updated.GetNumOfRows())
		metricMutation.commit()
	}
	mlog.Info(context.TODO(), "meta update: setting segment state - complete",
		mlog.Int64("segmentID", segmentID),
		mlog.String("target state", targetState.String()))
	return nil
}

func (m *meta) UpdateSegment(segmentID int64, operators ...SegmentOperator) error {
	log := log.Ctx(context.TODO())
	_, err := m.updateSegmentCAS(m.ctx, segmentID, func(seg *SegmentInfo) (BinlogIncrement, bool) {
		var inc BinlogIncrement
		updated := false
		for _, operator := range operators {
			opInc, changed := operator(seg)
			if changed {
				updated = true
				inc.Union(opInc)
			}
		}
		return inc, updated
	})
	if err != nil {
		if errors.Is(err, merr.ErrSegmentNotFound) {
			log.Warn("meta update: UpdateSegment - segment not found",
				zap.Int64("segmentID", segmentID))
			return err
		}
		log.Warn("meta update: update segment - failed to alter segments",
			zap.Int64("segmentID", segmentID),
			zap.Error(err))
		return err
	}
	log.Info("meta update: update segment - complete",
		zap.Int64("segmentID", segmentID))
	return nil
}

// UpdateSegmentsInfo atomically persists mutations to existing segments and
// inserts newSegments. Existing-segment operators run against a clone of the
// cached (fully-stitched) SegmentInfo; writes are CAS-gated on the cache
// version. On CAS conflict the whole operation retries from the cache read.
func (m *meta) UpdateSegmentsInfo(ctx context.Context, mutations map[int64][]SegmentOperator, newSegments ...*datapb.SegmentInfo) error {
	if len(mutations) == 0 && len(newSegments) == 0 {
		return nil
	}

	type mutatedEntry struct {
		segID int64
		clone *SegmentInfo
	}
	type newEntry struct {
		segID int64
		info  *SegmentInfo
	}

	var lastMutated []mutatedEntry
	var lastNew []newEntry
	var results []SegmentTxnResult
	var noop bool

	err := retry.Do(ctx, func() error {
		txn := m.segmentPersist.Txn(ctx)
		mutated := make([]mutatedEntry, 0, len(mutations))
		for segID, fns := range mutations {
			cur, version := m.segments.GetSegmentWithVersion(segID)
			if cur == nil {
				log.Ctx(ctx).Warn("meta update: segment not found, skipping",
					zap.Int64("segmentID", segID))
				continue
			}
			clone := cur.Clone()
			var inc BinlogIncrement
			shouldWrite := true
			for _, fn := range fns {
				fnInc, ok := fn(clone)
				if !ok {
					shouldWrite = false
					break
				}
				inc.Union(fnInc)
			}
			if !shouldWrite {
				continue
			}
			key := m.segmentKey(clone.GetCollectionID(), clone.GetPartitionID(), segID)
			if err := txn.Update(key, clone.SegmentInfo, version, inc); err != nil {
				return retry.Unrecoverable(err)
			}
			mutated = append(mutated, mutatedEntry{segID: segID, clone: clone})
		}

		newEntries := make([]newEntry, 0, len(newSegments))
		for _, seg := range newSegments {
			key := m.segmentKey(seg.GetCollectionID(), seg.GetPartitionID(), seg.GetID())
			if err := txn.Insert(key, seg); err != nil {
				return retry.Unrecoverable(err)
			}
			newEntries = append(newEntries, newEntry{segID: seg.GetID(), info: NewSegmentInfo(seg)})
		}

		if len(mutated) == 0 && len(newEntries) == 0 {
			noop = true
			return nil
		}

		committed, err := txn.Commit()
		if err != nil {
			return err
		}
		lastMutated = mutated
		lastNew = newEntries
		results = committed
		return nil
	}, retry.Attempts(segmentCASMaxRetries), retry.Sleep(0), retry.RetryErr(isCASFailed))
	if err != nil {
		log.Ctx(ctx).Error("meta update: failed to persist segments", zap.Error(err))
		return err
	}
	if noop {
		return nil
	}

	// Post-persist: update cache + metrics. Results are in add order: mutated first, then inserts.
	metricMutation := &segMetricMutation{
		stateChange: make(map[string]map[string]map[string]map[string]int),
	}
	idx := 0
	for _, e := range lastMutated {
		newSeg := e.clone
		oldSeg, existed := m.segments.SetSegment(e.segID, newSeg, results[idx].Version)
		if existed && oldSeg.GetState() != newSeg.GetState() {
			metricMutation.append(oldSeg.GetState(), newSeg.GetState(), newSeg.GetLevel(), newSeg.GetIsSorted(), newSeg.GetStorageVersion(), newSeg.GetNumOfRows())
		}
		idx++
	}
	for _, e := range lastNew {
		m.segments.SetSegment(e.segID, e.info, results[idx].Version)
		metricMutation.addNewSeg(e.info.GetState(), e.info.GetLevel(), e.info.GetIsSorted(), e.info.GetStorageVersion(), e.info.GetNumOfRows())
		idx++
	}
	metricMutation.commit()
	return nil
}

// UpdateDropChannelSegmentInfo updates segment checkpoints and binlogs before drop
// reusing segment info to pass segment id, binlogs, statslog, deltalog, start position and checkpoint
func (m *meta) UpdateDropChannelSegmentInfo(ctx context.Context, channel string, segments []*SegmentInfo) error {
	log := log.Ctx(ctx)
	log.Debug("meta update: update drop channel segment info",
		zap.String("channel", channel))

	// Build map of segment ID -> drop data for merge segments
	seg2DropMap := make(map[int64]*SegmentInfo)
	for _, seg2Drop := range segments {
		segment := m.segments.GetSegment(seg2Drop.ID)
		if segment == nil || !isSegmentHealthy(segment) {
			log.Warn("UpdateDropChannel skipping nil or unhealthy",
				zap.Bool("is nil", segment == nil),
				zap.Bool("isHealthy", isSegmentHealthy(segment)))
			continue
		}
		seg2DropMap[seg2Drop.GetID()] = seg2Drop
	}

	// Collect all healthy segments on this channel
	type segRef struct {
		id  int64
		key string
	}
	var segRefs []segRef
	for _, seg := range m.segments.GetSegmentsByChannel(channel) {
		segRefs = append(segRefs, segRef{
			id:  seg.GetID(),
			key: m.segmentKey(seg.GetCollectionID(), seg.GetPartitionID(), seg.GetID()),
		})
	}

	if len(segRefs) == 0 {
		// No segments to drop, just mark channel deleted
		if err := m.catalog.MarkChannelDeleted(ctx, channel); err != nil {
			return err
		}
		return nil
	}

	log.Info("meta update: batch save drop segments",
		zap.Int64s("drop segments", lo.Map(segRefs, func(r segRef, _ int) int64 { return r.id })))

	getFieldBinlogs := func(id UniqueID, binlogs []*datapb.FieldBinlog) *datapb.FieldBinlog {
		for _, binlog := range binlogs {
			if id == binlog.GetFieldID() {
				return binlog
			}
		}
		return nil
	}
	applyDrop := func(clone *SegmentInfo, mergeData *SegmentInfo) {
		clone.State = commonpb.SegmentState_Dropped
		clone.DroppedAt = uint64(time.Now().UnixNano())
		if mergeData == nil {
			return
		}
		currBinlogs := clone.GetBinlogs()
		for _, tBinlogs := range mergeData.GetBinlogs() {
			fieldBinlogs := getFieldBinlogs(tBinlogs.GetFieldID(), currBinlogs)
			if fieldBinlogs == nil {
				currBinlogs = append(currBinlogs, tBinlogs)
			} else {
				fieldBinlogs.Binlogs = append(fieldBinlogs.Binlogs, tBinlogs.Binlogs...)
			}
		}
		clone.Binlogs = currBinlogs

		currStatsLogs := clone.GetStatslogs()
		for _, tStatsLogs := range mergeData.GetStatslogs() {
			fieldStatsLog := getFieldBinlogs(tStatsLogs.GetFieldID(), currStatsLogs)
			if fieldStatsLog == nil {
				currStatsLogs = append(currStatsLogs, tStatsLogs)
			} else {
				fieldStatsLog.Binlogs = append(fieldStatsLog.Binlogs, tStatsLogs.Binlogs...)
			}
		}
		clone.Statslogs = currStatsLogs

		clone.Deltalogs = append(clone.GetDeltalogs(), mergeData.GetDeltalogs()...)
		if mergeData.GetStartPosition() != nil {
			clone.StartPosition = mergeData.GetStartPosition()
		}
		if mergeData.GetDmlPosition() != nil {
			clone.DmlPosition = mergeData.GetDmlPosition()
		}
		clone.NumOfRows = mergeData.GetNumOfRows()
	}

	type pending struct {
		id    int64
		clone *SegmentInfo
	}

	var results []SegmentTxnResult
	var pendings []pending

	err := retry.Do(ctx, func() error {
		txn := m.segmentPersist.Txn(ctx)
		pendings = pendings[:0]
		for _, ref := range segRefs {
			cur, version := m.segments.GetSegmentWithVersion(ref.id)
			if cur == nil {
				continue
			}
			clone := cur.Clone()
			mergeData := seg2DropMap[ref.id]
			applyDrop(clone, mergeData)
			// Only merge cases mutate binlog families; non-merge is state-only.
			var inc BinlogIncrement
			if mergeData != nil {
				inc = BinlogIncrement{
					Binlogs:   clone.GetBinlogs(),
					Deltalogs: clone.GetDeltalogs(),
					Statslogs: clone.GetStatslogs(),
				}
			}
			if err := txn.Update(ref.key, clone.SegmentInfo, version, inc); err != nil {
				return retry.Unrecoverable(err)
			}
			pendings = append(pendings, pending{id: ref.id, clone: clone})
		}
		if len(pendings) == 0 {
			return nil
		}
		committed, err := txn.Commit()
		if err != nil {
			return err
		}
		results = committed
		return nil
	}, retry.Attempts(segmentCASMaxRetries), retry.Sleep(0), retry.RetryErr(isCASFailed))
	if err != nil {
		log.Warn("meta update: update drop channel segment info failed",
			zap.String("channel", channel),
			zap.Error(err))
		return err
	}
	if err := m.catalog.MarkChannelDeleted(ctx, channel); err != nil {
		return err
	}

	metricMutation := &segMetricMutation{
		stateChange: make(map[string]map[string]map[string]map[string]int),
	}
	for i, p := range pendings {
		newInfo := p.clone
		oldSeg, existed := m.segments.SetSegment(p.id, newInfo, results[i].Version)
		if existed && oldSeg.GetState() != newInfo.GetState() {
			metricMutation.append(oldSeg.GetState(), newInfo.GetState(), newInfo.GetLevel(), newInfo.GetIsSorted(), newInfo.GetStorageVersion(), newInfo.GetNumOfRows())
		}
	}
	metricMutation.commit()

	log.Info("meta update: update drop channel segment info - complete",
		zap.String("channel", channel))
	return nil
}

// GetSegmentsByChannel returns all segment info which insert channel equals provided `dmlCh`
func (m *meta) GetSegmentsByChannel(channel string) []*SegmentInfo {
	return m.SelectSegments(m.ctx, SegmentFilterFunc(isSegmentHealthy), WithChannel(channel))
}

// GetSegmentsOfCollection get all segments of collection
func (m *meta) GetSegmentsOfCollection(ctx context.Context, collectionID UniqueID) []*SegmentInfo {
	return m.SelectSegments(ctx, SegmentFilterFunc(isSegmentHealthy), WithCollection(collectionID))
}

// GetSegmentsIDOfCollection returns all segment ids which collection equals to provided `collectionID`
func (m *meta) GetSegmentsIDOfCollection(ctx context.Context, collectionID UniqueID) []UniqueID {
	segments := m.SelectSegments(ctx, SegmentFilterFunc(isSegmentHealthy), WithCollection(collectionID))

	return lo.Map(segments, func(segment *SegmentInfo, _ int) int64 {
		return segment.ID
	})
}

// GetSegmentsIDOfCollectionWithDropped returns all dropped segment ids which collection equals to provided `collectionID`
func (m *meta) GetSegmentsIDOfCollectionWithDropped(ctx context.Context, collectionID UniqueID) []UniqueID {
	segments := m.SelectSegments(ctx, WithCollection(collectionID), SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return segment != nil &&
			segment.GetState() != commonpb.SegmentState_SegmentStateNone &&
			segment.GetState() != commonpb.SegmentState_NotExist
	}))

	return lo.Map(segments, func(segment *SegmentInfo, _ int) int64 {
		return segment.ID
	})
}

// GetSegmentsIDOfPartition returns all segments ids which collection & partition equals to provided `collectionID`, `partitionID`
func (m *meta) GetSegmentsIDOfPartition(ctx context.Context, collectionID, partitionID UniqueID) []UniqueID {
	segments := m.SelectSegments(ctx, WithCollection(collectionID), SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return isSegmentHealthy(segment) &&
			segment.PartitionID == partitionID
	}))

	return lo.Map(segments, func(segment *SegmentInfo, _ int) int64 {
		return segment.ID
	})
}

// GetSegmentsIDOfPartitionWithDropped returns all dropped segments ids which collection & partition equals to provided `collectionID`, `partitionID`
func (m *meta) GetSegmentsIDOfPartitionWithDropped(ctx context.Context, collectionID, partitionID UniqueID) []UniqueID {
	segments := m.SelectSegments(ctx, WithCollection(collectionID), SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return segment.GetState() != commonpb.SegmentState_SegmentStateNone &&
			segment.GetState() != commonpb.SegmentState_NotExist &&
			segment.PartitionID == partitionID
	}))

	return lo.Map(segments, func(segment *SegmentInfo, _ int) int64 {
		return segment.ID
	})
}

// GetNumRowsOfPartition returns row count of segments belongs to provided collection & partition
func (m *meta) GetNumRowsOfPartition(ctx context.Context, collectionID UniqueID, partitionID UniqueID) int64 {
	var ret int64
	segments := m.SelectSegments(ctx, WithCollection(collectionID), SegmentFilterFunc(func(si *SegmentInfo) bool {
		return isSegmentHealthy(si) && si.GetPartitionID() == partitionID
	}))
	for _, segment := range segments {
		ret += segment.NumOfRows
	}
	return ret
}

// GetUnFlushedSegments get all segments which state is not `Flushing` nor `Flushed`
func (m *meta) GetUnFlushedSegments() []*SegmentInfo {
	return m.SelectSegments(m.ctx, SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return segment.GetState() == commonpb.SegmentState_Growing || segment.GetState() == commonpb.SegmentState_Sealed
	}))
}

// GetFlushingSegments get all segments which state is `Flushing`
func (m *meta) GetFlushingSegments() []*SegmentInfo {
	return m.SelectSegments(m.ctx, SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return segment.GetState() == commonpb.SegmentState_Flushing
	}))
}

// SelectSegments select segments with selector
func (m *meta) SelectSegments(ctx context.Context, filters ...SegmentFilter) []*SegmentInfo {
	return m.segments.GetSegmentsBySelector(filters...)
}

func (m *meta) GetRealSegmentsForChannel(channel string) []*SegmentInfo {
	return m.segments.GetRealSegmentsForChannel(channel)
}

// AddAllocation add allocation in segment
func (m *meta) AddAllocation(segmentID UniqueID, allocation *Allocation) error {
	log.Ctx(m.ctx).Debug("meta update: add allocation",
		zap.Int64("segmentID", segmentID),
		zap.Any("allocation", allocation))

	curSegInfo := m.segments.GetSegment(segmentID)
	if curSegInfo == nil {
		// TODO: Error handling.
		mlog.Error(m.ctx, "meta update: add allocation failed - segment not found", mlog.Int64("segmentID", segmentID))
		return merr.WrapErrSegmentNotFound(segmentID, "meta update: add allocation failed")
	}
	// As we use global segment lastExpire to guarantee data correctness after restart
	// there is no need to persist allocation to meta store, only update allocation in-memory meta.
	m.segments.AddAllocation(segmentID, allocation)
	mlog.Info(m.ctx, "meta update: add allocation - complete", mlog.Int64("segmentID", segmentID))
	return nil
}

func (m *meta) SetRowCount(segmentID UniqueID, rowCount int64) {
	m.segments.SetRowCount(segmentID, rowCount)
}

// SetAllocations set Segment allocations, will overwrite ALL original allocations
// Note that allocations is not persisted in KV store
func (m *meta) SetAllocations(segmentID UniqueID, allocations []*Allocation) {
	m.segments.SetAllocations(segmentID, allocations)
}

// SetLastExpire set lastExpire time for segment
// Note that last is not necessary to store in KV meta
func (m *meta) SetLastExpire(segmentID UniqueID, lastExpire uint64) {
	m.segments.SetLastExpire(segmentID, lastExpire)
}

// SetLastFlushTime set LastFlushTime for segment with provided `segmentID`
// Note that lastFlushTime is not persisted in KV store
func (m *meta) SetLastFlushTime(segmentID UniqueID, t time.Time) {
	m.segments.SetFlushTime(segmentID, t)
}

// SetLastWrittenTime set LastWrittenTime for segment with provided `segmentID`
// Note that lastWrittenTime is not persisted in KV store
func (m *meta) SetLastWrittenTime(segmentID UniqueID) {
	m.segments.SetLastWrittenTime(segmentID)
}

// SetSegmentCompacting sets compaction state for segment
func (m *meta) SetSegmentCompacting(segmentID UniqueID, compacting bool) {
	m.segments.SetIsCompacting(segmentID, compacting)
}

// IsSegmentCompacting check if segment is compacting
func (m *meta) IsSegmentCompacting(segmentID UniqueID) bool {
	seg := m.segments.GetSegment(segmentID)
	if seg == nil {
		return false
	}
	return seg.isCompacting
}

// CheckAndSetSegmentsCompacting check all segments are not compacting
// if true, set them compacting and return true
// if false, skip setting and
func (m *meta) CheckAndSetSegmentsCompacting(ctx context.Context, segmentIDs []UniqueID) (exist, canDo bool) {
	var hasCompacting bool
	exist = true
	for _, segmentID := range segmentIDs {
		seg := m.segments.GetSegment(segmentID)
		if seg != nil {
			if seg.isCompacting {
				hasCompacting = true
			}
		} else {
			exist = false
			break
		}
	}
	canDo = exist && !hasCompacting
	if canDo {
		for _, segmentID := range segmentIDs {
			m.segments.SetIsCompacting(segmentID, true)
		}
	}
	return exist, canDo
}

func (m *meta) SetSegmentsCompacting(ctx context.Context, segmentIDs []UniqueID, compacting bool) {
	for _, segmentID := range segmentIDs {
		m.segments.SetIsCompacting(segmentID, compacting)
	}
}

// SetSegmentLevel sets level for segment
func (m *meta) SetSegmentLevel(segmentID UniqueID, level datapb.SegmentLevel) {
	m.segments.SetLevel(segmentID, level)
}

func getMinPosition(positions []*msgpb.MsgPosition) *msgpb.MsgPosition {
	var minPos *msgpb.MsgPosition
	for _, pos := range positions {
		if minPos == nil ||
			pos != nil && pos.GetTimestamp() < minPos.GetTimestamp() {
			minPos = pos
		}
	}
	return minPos
}

func getMaxPosition(positions []*msgpb.MsgPosition) *msgpb.MsgPosition {
	var maxPos *msgpb.MsgPosition
	for _, pos := range positions {
		if maxPos == nil ||
			pos != nil && pos.GetTimestamp() > maxPos.GetTimestamp() {
			maxPos = pos
		}
	}
	return maxPos
}

// recalculateSegmentPosition recalculates StartPosition and DmlPosition from
// actual binlog timestamps on the compaction result segment. This makes compaction
// self-healing: wrong positions from import or prior compaction are corrected.
// Also fixes the DmlPosition bug where getMinPosition was used instead of max.
// Falls back to the provided fallback positions if binlog timestamps are unavailable
// (e.g., legacy segments without TimestampFrom/TimestampTo populated).
func recalculateSegmentPosition(binlogs []*datapb.FieldBinlog, channel string, fallbackStart, fallbackDml *msgpb.MsgPosition) (startPos, dmlPos *msgpb.MsgPosition) {
	minTs, maxTs := extractTimestampFromBinlogs(binlogs)
	if minTs > 0 && minTs != math.MaxUint64 && maxTs > 0 {
		return &msgpb.MsgPosition{
				ChannelName: channel,
				Timestamp:   minTs,
			}, &msgpb.MsgPosition{
				ChannelName: channel,
				Timestamp:   maxTs,
			}
	}
	return fallbackStart, fallbackDml
}

// normalizePositionTimestamp updates a position's timestamp to commitTs if
// commitTs is non-zero and larger. Used during compaction completion to
// normalize import segment positions after row timestamps are rewritten.
//
// Note: this intentionally bumps Timestamp without advancing MsgID, so the
// returned MsgPosition violates the usual Timestamp == TSO(MsgID) invariant.
// Safe here because the result is only consumed as a *fallback* start/dml
// position for compaction-output segments by ts-only callers — GC,
// TruncateChannelByTime, GetEarliestTs — never seeked in the WAL by MsgID.
// Any future caller that needs MsgID/Timestamp consistency (WAL seek,
// resume-from-position) must NOT use this helper.
func normalizePositionTimestamp(pos *msgpb.MsgPosition, commitTs uint64) *msgpb.MsgPosition {
	if commitTs == 0 || pos == nil || pos.Timestamp >= commitTs {
		return pos
	}
	return &msgpb.MsgPosition{
		ChannelName: pos.ChannelName,
		MsgID:       pos.MsgID,
		Timestamp:   commitTs,
	}
}

func maxCommitTimestamp(compactFromSegInfos []*SegmentInfo) uint64 {
	var maxCommitTs uint64
	for _, info := range compactFromSegInfos {
		maxCommitTs = max(maxCommitTs, info.GetCommitTimestamp())
	}
	return maxCommitTs
}

func getCompactionFallbackPositions(compactFromSegInfos []*SegmentInfo) (fallbackStart, fallbackDml *msgpb.MsgPosition) {
	maxCommitTs := maxCommitTimestamp(compactFromSegInfos)

	// Fallback positions are used only when output binlog timestamps are
	// unavailable. Keep the raw minimum start timestamp so normal rows in mixed
	// compaction still receive deletes after their original start. Normalize the
	// fallback DML timestamp to commit_ts so temporal cleanup does not treat the
	// compacted output as complete before committed import rows become visible.
	fallbackStart = getMinPosition(lo.Map(compactFromSegInfos, func(info *SegmentInfo, _ int) *msgpb.MsgPosition {
		return info.GetStartPosition()
	}))
	fallbackDml = normalizePositionTimestamp(getMaxPosition(lo.Map(compactFromSegInfos, func(info *SegmentInfo, _ int) *msgpb.MsgPosition {
		return info.GetDmlPosition()
	})), maxCommitTs)
	return fallbackStart, fallbackDml
}

func (m *meta) completeClusterCompactionMutation(ctx context.Context, t *datapb.CompactionTask, result *datapb.CompactionPlanResult) ([]*SegmentInfo, *segMetricMutation, error) {
	ctx = m.opContext(ctx)
	log := log.Ctx(ctx).With(zap.Int64("planID", t.GetPlanID()),
		zap.String("type", t.GetType().String()),
		zap.Int64("collectionID", t.CollectionID),
		zap.Int64("partitionID", t.PartitionID),
		zap.String("channel", t.GetChannel()))
	metricMutation := &segMetricMutation{stateChange: make(segmentMetricStateChange)}
	compactFromSegIDs := make([]int64, 0)
	compactFromSegInfos := make([]*SegmentInfo, 0)
	compactToSegInfos := make([]*SegmentInfo, 0)

	for _, segmentID := range t.GetInputSegments() {
		segment := m.segments.GetSegment(segmentID)
		if segment == nil {
			return nil, nil, merr.WrapErrSegmentNotFound(segmentID)
		}

		// Re-validate segment health to prevent race condition with drop collection
		// between ValidateSegmentStateBeforeCompleteCompactionMutation and here
		if !isSegmentHealthy(segment) {
			mlog.Warn(context.TODO(), "input segment was dropped during compaction mutation",
				mlog.Int64("planID", t.GetPlanID()),
				mlog.Int64("segmentID", segmentID),
				mlog.String("state", segment.GetState().String()))
			return nil, nil, merr.WrapErrSegmentNotFound(segmentID, "input segment was dropped")
		}

		cloned := segment.Clone()

		compactFromSegInfos = append(compactFromSegInfos, cloned)
		compactFromSegIDs = append(compactFromSegIDs, cloned.GetID())
	}

	fallbackStart, fallbackDml := getCompactionFallbackPositions(compactFromSegInfos)

	for _, seg := range result.GetSegments() {
		startPos, dmlPos := recalculateSegmentPosition(seg.GetInsertLogs(), t.GetChannel(), fallbackStart, fallbackDml)
		segmentInfo := &datapb.SegmentInfo{
			ID:                  seg.GetSegmentID(),
			CollectionID:        compactFromSegInfos[0].CollectionID,
			PartitionID:         compactFromSegInfos[0].PartitionID,
			InsertChannel:       t.GetChannel(),
			NumOfRows:           seg.NumOfRows,
			State:               commonpb.SegmentState_Flushed,
			MaxRowNum:           compactFromSegInfos[0].MaxRowNum,
			Binlogs:             seg.GetInsertLogs(),
			Statslogs:           seg.GetField2StatslogPaths(),
			CreatedByCompaction: true,
			CompactionFrom:      compactFromSegIDs,
			LastExpireTime:      tsoutil.ComposeTSByTime(time.Unix(t.GetStartTime(), 0), 0),
			Level:               datapb.SegmentLevel_L2,
			StartPosition:       startPos,
			DmlPosition:         dmlPos,
			// visible after stats and index
			IsInvisible:     true,
			StorageVersion:  seg.GetStorageVersion(),
			ManifestPath:    seg.GetManifest(),
			ExpirQuantiles:  seg.GetExpirQuantiles(),
			SchemaVersion:   t.GetSchema().GetVersion(),
			CommitTimestamp: 0, // Normalized: row timestamps already rewritten
		}
		segment := NewSegmentInfo(segmentInfo)
		compactToSegInfos = append(compactToSegInfos, segment)
		metricMutation.addNewSeg(segment.GetState(), segment.GetLevel(), segment.GetIsSorted(), segment.GetStorageVersion(), segmentMetricFormatLabel(segment), segment.GetNumOfRows())
	}

	mlog.Debug(context.TODO(), "meta update: prepare for meta mutation - complete")

	// Persist new compactTo segments
	txn := m.segmentPersist.Txn(ctx)
	for _, info := range compactToSegInfos {
		if err := txn.Insert(m.segmentKey(info.GetCollectionID(), info.GetPartitionID(), info.GetID()), info.SegmentInfo); err != nil {
			return nil, nil, err
		}
	}
	results, err := txn.Commit()
	if err != nil {
		log.Warn("fail to alter compactTo segments", zap.Error(err))
		return nil, nil, err
	}
	for i, info := range compactToSegInfos {
		m.segments.SetSegment(info.GetID(), info, results[i].Version)
	}
	log.Info("meta update: alter in memory meta after compaction - complete")
	return compactToSegInfos, metricMutation, nil
}

func (m *meta) completeMixCompactionMutation(
	ctx context.Context,
	t *datapb.CompactionTask,
	result *datapb.CompactionPlanResult,
) ([]*SegmentInfo, *segMetricMutation, error) {
	ctx = m.opContext(ctx)
	log := log.Ctx(context.TODO()).With(zap.Int64("planID", t.GetPlanID()),
		zap.String("type", t.GetType().String()),
		zap.Int64("collectionID", t.CollectionID),
		zap.Int64("partitionID", t.PartitionID),
		zap.String("channel", t.GetChannel()),
		zap.Int64("planID", t.GetPlanID()),
	)

	metricMutation := &segMetricMutation{stateChange: make(map[string]map[string]map[string]map[string]int)}
	// Read compactFrom segments from cache (read-only for validation and new segment construction).
	var compactFromSegIDs []int64
	var compactFromCached []*SegmentInfo
	for _, segmentID := range t.GetInputSegments() {
		segment := m.segments.GetSegment(segmentID)
		if segment == nil {
			return nil, nil, merr.WrapErrSegmentNotFound(segmentID)
		}

		// Re-validate segment health to prevent race condition with drop collection
		// between ValidateSegmentStateBeforeCompleteCompactionMutation and here
		if !isSegmentHealthy(segment) {
			mlog.Warn(context.TODO(), "input segment was dropped during compaction mutation",
				mlog.Int64("planID", t.GetPlanID()),
				mlog.Int64("segmentID", segmentID),
				mlog.String("state", segment.GetState().String()))
			return nil, nil, merr.WrapErrSegmentNotFound(segmentID, "input segment was dropped")
		}

		compactFromCached = append(compactFromCached, segment)
		compactFromSegIDs = append(compactFromSegIDs, segmentID)

		log.Info("compact from segment",
			zap.Int64("segmentID", segmentID),
			zap.Int64("segment size", segment.getSegmentSize()),
			zap.Int64("num rows", segment.GetNumOfRows()),
		)
	}

	log = log.With(zap.Int64s("compactFrom", compactFromSegIDs))

	if t.GetSchema() == nil {
		return nil, nil, merr.WrapErrIllegalCompactionPlan("mix compaction task schema is nil")
	}
	outputSchemaVersion := t.GetSchema().GetVersion()

	fallbackStart, fallbackDml := getCompactionFallbackPositions(compactFromCached)

	compactToSegments := make([]*SegmentInfo, 0)
	for _, compactToSegment := range result.GetSegments() {
		startPos, dmlPos := recalculateSegmentPosition(compactToSegment.GetInsertLogs(), t.GetChannel(), fallbackStart, fallbackDml)
		compactToSegmentInfo := NewSegmentInfo(
			&datapb.SegmentInfo{
				ID:            compactToSegment.GetSegmentID(),
				CollectionID:  compactFromCached[0].CollectionID,
				PartitionID:   compactFromCached[0].PartitionID,
				InsertChannel: t.GetChannel(),
				NumOfRows:     compactToSegment.NumOfRows,
				State:         commonpb.SegmentState_Flushed,
				MaxRowNum:     compactFromCached[0].MaxRowNum,
				Binlogs:       compactToSegment.GetInsertLogs(),
				Statslogs:     compactToSegment.GetField2StatslogPaths(),
				Deltalogs:     compactToSegment.GetDeltalogs(),
				Bm25Statslogs: compactToSegment.GetBm25Logs(),
				TextStatsLogs: compactToSegment.GetTextStatsLogs(),

				CreatedByCompaction: true,
				CompactionFrom:      compactFromSegIDs,
				LastExpireTime:      tsoutil.ComposeTSByTime(time.Unix(t.GetStartTime(), 0), 0),
				Level:               datapb.SegmentLevel_L1,
				StorageVersion:      compactToSegment.GetStorageVersion(),
				StartPosition:       startPos,
				DmlPosition:         dmlPos,
				IsSorted:            compactToSegment.GetIsSorted(),
				ManifestPath:        compactToSegment.GetManifest(),
				IsSortedByNamespace: compactToSegment.GetIsSortedByNamespace(),
				ExpirQuantiles:      compactToSegment.GetExpirQuantiles(),
				SchemaVersion:       compactFromCached[0].GetSchemaVersion(),
			})

		if compactToSegmentInfo.GetNumOfRows() == 0 {
			compactToSegmentInfo.State = commonpb.SegmentState_Dropped
		}

		// metrics mutation for compactTo segments
		metricMutation.addNewSeg(compactToSegmentInfo.GetState(), compactToSegmentInfo.GetLevel(), compactToSegmentInfo.GetIsSorted(), compactToSegmentInfo.GetStorageVersion(), segmentMetricFormatLabel(compactToSegmentInfo), compactToSegmentInfo.GetNumOfRows())

		mlog.Info(context.TODO(), "Add a new compactTo segment",
			mlog.Int64("compactTo", compactToSegmentInfo.GetID()),
			mlog.Int64("compactTo segment numRows", compactToSegmentInfo.GetNumOfRows()),
			mlog.Int("binlog count", len(compactToSegmentInfo.GetBinlogs())),
			mlog.Int("statslog count", len(compactToSegmentInfo.GetStatslogs())),
			mlog.Int("deltalog count", len(compactToSegmentInfo.GetDeltalogs())),
			mlog.Int64("segment size", compactToSegmentInfo.getSegmentSize()),
			mlog.Int64s("expirQuantiles", compactToSegmentInfo.GetExpirQuantiles()),
		)
		compactToSegments = append(compactToSegments, compactToSegmentInfo)
	}

	log.Debug("meta update: prepare for meta mutation - complete")

	// Persist all segments atomically in one transaction, retrying on CAS conflict.
	type compactFromEntry struct {
		seg   *SegmentInfo
		clone *SegmentInfo
	}
	var fromPendings []compactFromEntry
	var results []SegmentTxnResult
	err := retry.Do(ctx, func() error {
		txn := m.segmentPersist.Txn(ctx)
		for _, info := range compactToSegments {
			if err := txn.Insert(m.segmentKey(info.GetCollectionID(), info.GetPartitionID(), info.GetID()), info.SegmentInfo); err != nil {
				return retry.Unrecoverable(err)
			}
		}
		fromPendings = fromPendings[:0]
		for _, seg := range compactFromCached {
			cur, version := m.segments.GetSegmentWithVersion(seg.GetID())
			if cur == nil {
				continue
			}
			clone := cur.Clone()
			clone.State = commonpb.SegmentState_Dropped
			clone.DroppedAt = uint64(time.Now().UnixNano())
			clone.Compacted = true
			key := m.segmentKey(seg.GetCollectionID(), seg.GetPartitionID(), seg.GetID())
			if err := txn.Update(key, clone.SegmentInfo, version, BinlogIncrement{}); err != nil {
				return retry.Unrecoverable(err)
			}
			fromPendings = append(fromPendings, compactFromEntry{seg: seg, clone: clone})
		}
		committed, err := txn.Commit()
		if err != nil {
			return err
		}
		results = committed
		return nil
	}, retry.Attempts(segmentCASMaxRetries), retry.Sleep(0), retry.RetryErr(isCASFailed))
	if err != nil {
		log.Warn("fail to alter segments for compaction", zap.Error(err))
		return nil, nil, err
	}
	toCount := len(compactToSegments)
	for i, info := range compactToSegments {
		m.segments.SetSegment(info.GetID(), info, results[i].Version)
	}
	for i, entry := range fromPendings {
		newInfo := entry.clone
		old, existed := m.segments.SetSegment(entry.seg.GetID(), newInfo, results[toCount+i].Version)
		if existed && old.GetState() != newInfo.GetState() {
			metricMutation.append(old.GetState(), newInfo.GetState(), newInfo.GetLevel(), newInfo.GetIsSorted(), newInfo.GetStorageVersion(), newInfo.GetNumOfRows())
		}
	}

	mlog.Info(context.TODO(), "meta update: alter in memory meta after compaction - complete")
	return compactToSegments, metricMutation, nil
}

func (m *meta) ValidateSegmentStateBeforeCompleteCompactionMutation(t *datapb.CompactionTask) error {
	// Snapshot compaction protection exists to keep the sealed-segment list stable during
	// backfill — if an L1/L2 segment gets merged away mid-backfill, the backfill breaks.
	// L0 segments are transient delete-log carriers, not part of that stable list, and
	// L0 compaction only appends deltalogs to L1/L2 targets without touching L1/L2 binlogs.
	// So L0 delete compaction is outside the protection's concern and must not be blocked.
	if t.GetType() != datapb.CompactionType_Level0DeleteCompaction {
		// Check if compaction is blocked for this collection (snapshot pending or RefIndex not loaded).
		if m.isCollectionCompactionBlocked(t.GetCollectionID()) {
			mlog.Info(context.TODO(), "compaction rejected: collection has pending snapshot or unloaded RefIndex",
				mlog.Int64("planID", t.GetPlanID()),
				mlog.String("type", t.GetType().String()),
				mlog.Int64("collectionID", t.GetCollectionID()),
				mlog.String("channel", t.GetChannel()),
				mlog.Int64s("inputSegments", t.GetInputSegments()),
			)
			return merr.WrapErrCompactionBlocked(
				fmt.Sprintf("collection %d has pending snapshot or unloaded snapshot RefIndex",
					t.GetCollectionID()))
		}

		// Check if any input segment is protected by a snapshot.
		for _, segmentID := range t.GetInputSegments() {
			if m.isSegmentCompactionProtected(segmentID) {
				mlog.Info(context.TODO(), "compaction rejected: input segment is protected by snapshot",
					mlog.Int64("planID", t.GetPlanID()),
					mlog.String("type", t.GetType().String()),
					mlog.Int64("collectionID", t.GetCollectionID()),
					mlog.String("channel", t.GetChannel()),
					mlog.Int64("segmentID", segmentID),
					mlog.Int64s("inputSegments", t.GetInputSegments()),
				)
				return merr.WrapErrCompactionBlocked(
					fmt.Sprintf("input segment %d is protected by a snapshot", segmentID))
			}
		}
	}

	for _, segmentID := range t.GetInputSegments() {
		segment := m.segments.GetSegment(segmentID)
		if !isSegmentHealthy(segment) {
			// SHOULD NOT HAPPEN: input segment was dropped.
			// This indicates that compaction tasks, which should be mutually exclusive,
			// may have executed concurrently.
			mlog.Warn(context.TODO(), "should not happen! input segment was dropped",
				mlog.Int64("planID", t.GetPlanID()),
				mlog.String("type", t.GetType().String()),
				mlog.String("channel", t.GetChannel()),
				mlog.Int64("partitionID", t.GetPartitionID()),
				mlog.Int64("segmentID", segmentID),
			)
			return merr.WrapErrSegmentNotFound(segmentID, "input segment was dropped")
		}
	}
	return nil
}

func (m *meta) CompleteCompactionMutation(ctx context.Context, t *datapb.CompactionTask, result *datapb.CompactionPlanResult) ([]*SegmentInfo, *segMetricMutation, error) {
	switch t.GetType() {
	case datapb.CompactionType_MixCompaction:
		return m.completeMixCompactionMutation(ctx, t, result)
	case datapb.CompactionType_ClusteringCompaction:
		return m.completeClusterCompactionMutation(ctx, t, result)
	case datapb.CompactionType_SortCompaction:
		return m.completeSortCompactionMutation(ctx, t, result)
	case datapb.CompactionType_BumpSchemaVersionCompaction:
		return m.completeBumpSchemaVersionCompactionMutation(t, result)
	}
	return nil, nil, merr.WrapErrIllegalCompactionPlan("illegal compaction type")
}

// buildSegment utility function for compose datapb.SegmentInfo struct with provided info
func buildSegment(collectionID UniqueID, partitionID UniqueID, segmentID UniqueID, channelName string) *SegmentInfo {
	info := &datapb.SegmentInfo{
		ID:            segmentID,
		CollectionID:  collectionID,
		PartitionID:   partitionID,
		InsertChannel: channelName,
		NumOfRows:     0,
		State:         commonpb.SegmentState_Growing,
	}
	return NewSegmentInfo(info)
}

func isSegmentHealthy(segment *SegmentInfo) bool {
	return segment != nil &&
		segment.GetState() != commonpb.SegmentState_SegmentStateNone &&
		segment.GetState() != commonpb.SegmentState_NotExist &&
		segment.GetState() != commonpb.SegmentState_Dropped
}

func (m *meta) HasSegments(segIDs []UniqueID) (bool, error) {
	for _, segID := range segIDs {
		if m.segments.GetSegment(segID) == nil {
			return false, fmt.Errorf("segment is not exist with ID = %d", segID)
		}
	}
	return true, nil
}

// GetCompactionTo returns the segment info of the segment to be compacted to.
func (m *meta) GetCompactionTo(segmentID int64) ([]*SegmentInfo, bool) {
	return m.segments.GetCompactionTo(segmentID)
}

// GetMinGrowingSegmentCheckpoint returns the minimum DmlPosition of all growing
// segments on the given channel that belong to TEXT collections.
func (m *meta) GetMinGrowingSegmentCheckpoint(channel string) *msgpb.MsgPosition {
	segments := m.SelectSegments(context.TODO(), WithChannel(channel))

	textCollectionCache := make(map[int64]bool)
	var minPos *msgpb.MsgPosition
	for _, s := range segments {
		if s.GetState() != commonpb.SegmentState_Growing {
			continue
		}
		if s.GetLevel() == datapb.SegmentLevel_L0 {
			continue
		}

		collID := s.GetCollectionID()
		isText, cached := textCollectionCache[collID]
		if !cached {
			isText = m.collectionHasTextFields(collID)
			textCollectionCache[collID] = isText
		}
		if !isText {
			continue
		}

		pos := s.GetDmlPosition()
		if pos == nil {
			continue
		}
		if minPos == nil || pos.GetTimestamp() < minPos.GetTimestamp() {
			minPos = pos
		}
	}
	return minPos
}

func (m *meta) collectionHasTextFields(collectionID int64) bool {
	coll := m.GetCollection(collectionID)
	if coll == nil || coll.Schema == nil {
		return false
	}
	for _, field := range coll.Schema.GetFields() {
		if field.GetDataType() == schemapb.DataType_Text {
			return true
		}
	}
	return false
}

// UpdateChannelCheckpoint updates and saves channel checkpoint.
func (m *meta) UpdateChannelCheckpoint(ctx context.Context, vChannel string, pos *msgpb.MsgPosition) error {
	if pos == nil || pos.GetMsgID() == nil {
		return merr.WrapErrServiceInternalMsg("channelCP is nil, vChannel=%s", vChannel)
	}

	minGrowingCP := m.GetMinGrowingSegmentCheckpoint(vChannel)
	if minGrowingCP != nil && pos.GetTimestamp() > minGrowingCP.GetTimestamp() {
		log.Ctx(ctx).Info("clamping channel checkpoint to min growing segment checkpoint",
			zap.String("vChannel", vChannel),
			zap.Uint64("requestedTs", pos.GetTimestamp()),
			zap.Uint64("clampedTs", minGrowingCP.GetTimestamp()))
		pos = minGrowingCP
	}

	m.channelCPs.Lock()
	defer m.channelCPs.Unlock()

	oldPosition, ok := m.channelCPs.checkpoints[vChannel]
	if !ok || oldPosition.Timestamp < pos.Timestamp || (oldPosition.Timestamp == pos.Timestamp && !bytes.Equal(oldPosition.MsgID, pos.MsgID)) {
		err := m.catalog.SaveChannelCheckpoint(ctx, vChannel, pos)
		if err != nil {
			return err
		}
		m.channelCPs.checkpoints[vChannel] = pos
		ts, _ := tsoutil.ParseTS(pos.Timestamp)
		mlog.Info(context.TODO(), "UpdateChannelCheckpoint done",
			mlog.String("vChannel", vChannel),
			mlog.Uint64("ts", pos.GetTimestamp()),
			mlog.ByteString("msgID", pos.GetMsgID()),
			mlog.Stringer("walName", pos.WALName),
			mlog.Time("time", ts))
		metrics.DataCoordCheckpointUnixSeconds.WithLabelValues(paramtable.GetStringNodeID(), vChannel).
			Set(float64(ts.Unix()))
	}
	return nil
}

// MarkChannelCheckpointDropped writes the dropped-channel sentinel
// (funcutil.DroppedChannelCheckpointTimestamp) so no later
// UpdateChannelCheckpoint can overwrite it, and removes the channel-checkpoint
// lag metric.
func (m *meta) MarkChannelCheckpointDropped(ctx context.Context, channel string) error {
	m.channelCPs.Lock()
	defer m.channelCPs.Unlock()

	cp := &msgpb.MsgPosition{
		ChannelName: channel,
		Timestamp:   funcutil.DroppedChannelCheckpointTimestamp,
	}

	err := m.catalog.SaveChannelCheckpoints(ctx, []*msgpb.MsgPosition{cp})
	if err != nil {
		return err
	}

	m.channelCPs.checkpoints[channel] = cp

	metrics.DataCoordCheckpointUnixSeconds.DeleteLabelValues(paramtable.GetStringNodeID(), channel)
	return nil
}

// UpdateChannelCheckpoints updates and saves channel checkpoints.
func (m *meta) UpdateChannelCheckpoints(ctx context.Context, positions []*msgpb.MsgPosition) error {
	log := log.Ctx(ctx)
	for i, pos := range positions {
		if pos == nil || pos.GetChannelName() == "" {
			continue
		}
		minGrowingCP := m.GetMinGrowingSegmentCheckpoint(pos.GetChannelName())
		if minGrowingCP != nil && pos.GetTimestamp() > minGrowingCP.GetTimestamp() {
			log.Info("clamping channel checkpoint to min growing segment checkpoint",
				zap.String("vChannel", pos.GetChannelName()),
				zap.Uint64("requestedTs", pos.GetTimestamp()),
				zap.Uint64("clampedTs", minGrowingCP.GetTimestamp()))
			positions[i] = minGrowingCP
		}
	}

	m.channelCPs.Lock()
	defer m.channelCPs.Unlock()
	toUpdates := lo.Filter(positions, func(pos *msgpb.MsgPosition, _ int) bool {
		if pos == nil || (pos.GetMsgID() == nil && pos.GetWALName() != commonpb.WALName_WoodPecker) || pos.GetChannelName() == "" {
			mlog.Warn(context.TODO(), "illegal channel cp", mlog.Any("pos", pos))
			return false
		}
		vChannel := pos.GetChannelName()
		oldPosition, ok := m.channelCPs.checkpoints[vChannel]
		return !ok || oldPosition.Timestamp < pos.Timestamp || (oldPosition.Timestamp == pos.Timestamp && !bytes.Equal(oldPosition.MsgID, pos.MsgID))
	})
	err := m.catalog.SaveChannelCheckpoints(ctx, toUpdates)
	if err != nil {
		return err
	}
	for _, pos := range toUpdates {
		channel := pos.GetChannelName()
		m.channelCPs.checkpoints[channel] = pos
		mlog.Info(context.TODO(), "UpdateChannelCheckpoint done", mlog.String("channel", channel),
			mlog.Stringer("walName", pos.WALName),
			mlog.Uint64("ts", pos.GetTimestamp()),
			mlog.Time("time", tsoutil.PhysicalTime(pos.GetTimestamp())))
		ts, _ := tsoutil.ParseTS(pos.Timestamp)
		metrics.DataCoordCheckpointUnixSeconds.WithLabelValues(paramtable.GetStringNodeID(), channel).Set(float64(ts.Unix()))
	}
	// broadcast the change of channel checkpoint for TruncateCollection op to drop segments
	m.channelCPs.cond.UnsafeBroadcast()
	return nil
}

func (m *meta) GetChannelCheckpoint(vChannel string) *msgpb.MsgPosition {
	m.channelCPs.RLock()
	defer m.channelCPs.RUnlock()
	cp, ok := m.channelCPs.checkpoints[vChannel]
	if !ok {
		return nil
	}
	return proto.Clone(cp).(*msgpb.MsgPosition)
}

func (m *meta) DropChannelCheckpoint(vChannel string) error {
	m.channelCPs.Lock()
	defer m.channelCPs.Unlock()
	err := m.catalog.DropChannelCheckpoint(m.ctx, vChannel)
	if err != nil {
		return err
	}
	delete(m.channelCPs.checkpoints, vChannel)
	metrics.DataCoordCheckpointUnixSeconds.DeleteLabelValues(paramtable.GetStringNodeID(), vChannel)
	mlog.Info(context.TODO(), "DropChannelCheckpoint done", mlog.String("vChannel", vChannel))
	return nil
}

func (m *meta) GetChannelCheckpoints() map[string]*msgpb.MsgPosition {
	m.channelCPs.RLock()
	defer m.channelCPs.RUnlock()

	checkpoints := make(map[string]*msgpb.MsgPosition, len(m.channelCPs.checkpoints))
	for ch, cp := range m.channelCPs.checkpoints {
		checkpoints[ch] = proto.Clone(cp).(*msgpb.MsgPosition)
	}
	return checkpoints
}

func (m *meta) GcConfirm(ctx context.Context, collectionID, partitionID UniqueID) bool {
	return m.catalog.GcConfirm(ctx, collectionID, partitionID)
}

// isSegmentCompactionProtected checks if a segment is protected from compaction by a snapshot.
func (m *meta) isSegmentCompactionProtected(segmentID int64) bool {
	if m.snapshotMeta == nil {
		return false
	}
	return m.snapshotMeta.IsSegmentCompactionProtected(segmentID)
}

// isCollectionCompactionBlocked checks if compaction is blocked for a collection
// because a protected snapshot's RefIndex hasn't been loaded yet (fail-closed).
func (m *meta) isCollectionCompactionBlocked(collectionID int64) bool {
	if m.snapshotMeta == nil {
		return false
	}
	return m.snapshotMeta.IsCollectionCompactionBlocked(collectionID)
}

// GetCompactableSegmentGroupByCollection returns sealed segments grouped by collection.
// This is consumed exclusively by the L0 compaction policy, which only acts on L0 segments.
// Snapshot compaction protection targets L1/L2 segments referenced by snapshots, so it must
// NOT filter segments here: doing so would prevent L0 delete-log compaction and cause
// delta log accumulation, query latency spikes, and write stalls on collections with
// active snapshots.
func (m *meta) GetCompactableSegmentGroupByCollection() map[int64][]*SegmentInfo {
	allSegs := m.SelectSegments(m.ctx, SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return isSegmentHealthy(segment) &&
			isFlushed(segment) && // sealed segment
			!segment.isCompacting && // not compacting now
			!segment.GetIsImporting() // not importing now
	}))

	ret := make(map[int64][]*SegmentInfo)
	for _, seg := range allSegs {
		ret[seg.CollectionID] = append(ret[seg.CollectionID], seg)
	}

	return ret
}

func (m *meta) GetEarliestStartPositionOfGrowingSegments(label *CompactionGroupLabel) *msgpb.MsgPosition {
	segments := m.SelectSegments(m.ctx, WithCollection(label.CollectionID), SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return segment.GetState() == commonpb.SegmentState_Growing &&
			(label.PartitionID == common.AllPartitionsID || segment.GetPartitionID() == label.PartitionID) &&
			segment.GetInsertChannel() == label.Channel
	}))

	earliest := &msgpb.MsgPosition{Timestamp: math.MaxUint64}
	for _, seg := range segments {
		if earliest.GetTimestamp() == math.MaxUint64 || earliest.GetTimestamp() > seg.GetStartPosition().GetTimestamp() {
			earliest = seg.GetStartPosition()
		}
	}
	return earliest
}

// initStateChangeEntry initializes the nested map structure for the given keys and returns the format change map.
func (s *segMetricMutation) initStateChangeEntry(level, state, sortedStatus, storageVersion string) map[string]int {
	if _, ok := s.stateChange[level]; !ok {
		s.stateChange[level] = make(map[string]map[string]map[string]map[string]int)
	}
	if _, ok := s.stateChange[level][state]; !ok {
		s.stateChange[level][state] = make(map[string]map[string]map[string]int)
	}
	if _, ok := s.stateChange[level][state][sortedStatus]; !ok {
		s.stateChange[level][state][sortedStatus] = make(map[string]map[string]int)
	}
	if _, ok := s.stateChange[level][state][sortedStatus][storageVersion]; !ok {
		s.stateChange[level][state][sortedStatus][storageVersion] = make(map[string]int)
	}
	return s.stateChange[level][state][sortedStatus][storageVersion]
}

// addNewSeg update metrics update for a new segment.
func (s *segMetricMutation) addNewSeg(state commonpb.SegmentState, level datapb.SegmentLevel, isSorted bool, storageVersion int64, format string, rowCount int64) {
	storageVersionStr := fmt.Sprint(storageVersion)
	sortedStatus := getSortStatus(isSorted)
	entry := s.initStateChangeEntry(level.String(), state.String(), sortedStatus, storageVersionStr)
	entry[format] += 1

	s.rowCountChange += rowCount
	s.rowCountAccChange += rowCount
}

// commit persists all updates in current segMetricMutation, should and must be called AFTER segment state change
// has persisted in Etcd.
func (s *segMetricMutation) commit() {
	for level, submap := range s.stateChange {
		for state, sortedMap := range submap {
			for sortedLabel, versionMap := range sortedMap {
				for storageVersion, formatMap := range versionMap {
					for format, change := range formatMap {
						metrics.DataCoordNumSegments.WithLabelValues(state, level, sortedLabel, storageVersion, format).Add(float64(change))
					}
				}
			}
		}
	}
}

// append updates current segMetricMutation when segment state changes.
func (s *segMetricMutation) append(oldState, newState commonpb.SegmentState, level datapb.SegmentLevel, isSorted bool, storageVersion int64, format string, rowCountUpdate int64) {
	if oldState != newState && !s.deferSegmentLabelChange {
		storageVersionStr := fmt.Sprint(storageVersion)
		sortedStatus := getSortStatus(isSorted)
		levelStr := level.String()
		oldEntry := s.initStateChangeEntry(levelStr, oldState.String(), sortedStatus, storageVersionStr)
		newEntry := s.initStateChangeEntry(levelStr, newState.String(), sortedStatus, storageVersionStr)
		oldEntry[format] -= 1
		newEntry[format] += 1
	}
	// Update # of rows on new flush operations and drop operations.
	if isFlushState(newState) && !isFlushState(oldState) {
		// If new flush.
		s.rowCountChange += rowCountUpdate
		s.rowCountAccChange += rowCountUpdate
	} else if newState == commonpb.SegmentState_Dropped && oldState != newState {
		// If new drop.
		s.rowCountChange -= rowCountUpdate
	}
}

func sameSegmentMetricLabels(oldSegment, newSegment *SegmentInfo) bool {
	return oldSegment.GetState() == newSegment.GetState() &&
		oldSegment.GetLevel() == newSegment.GetLevel() &&
		oldSegment.GetIsSorted() == newSegment.GetIsSorted() &&
		oldSegment.GetStorageVersion() == newSegment.GetStorageVersion() &&
		segmentMetricFormatLabel(oldSegment) == segmentMetricFormatLabel(newSegment)
}

func (s *segMetricMutation) appendSegmentLabelChange(oldSegment, newSegment *SegmentInfo) {
	oldEntry := s.initStateChangeEntry(
		oldSegment.GetLevel().String(),
		oldSegment.GetState().String(),
		getSortStatus(oldSegment.GetIsSorted()),
		fmt.Sprint(oldSegment.GetStorageVersion()),
	)
	oldEntry[segmentMetricFormatLabel(oldSegment)] -= 1

	newEntry := s.initStateChangeEntry(
		newSegment.GetLevel().String(),
		newSegment.GetState().String(),
		getSortStatus(newSegment.GetIsSorted()),
		fmt.Sprint(newSegment.GetStorageVersion()),
	)
	newEntry[segmentMetricFormatLabel(newSegment)] += 1
}

func (p *updateSegmentPack) prepareSegmentMetricUpdates() {
	for id, updated := range p.segments {
		original := p.meta.segments.GetSegment(id)
		if original == nil {
			continue
		}
		if sameSegmentMetricLabels(original, updated) {
			continue
		}
		p.metricMutation.appendSegmentLabelChange(original, updated)
	}
}

func isFlushState(state commonpb.SegmentState) bool {
	return state == commonpb.SegmentState_Flushing || state == commonpb.SegmentState_Flushed
}

func updateSegStateAndPrepareMetrics(segToUpdate *SegmentInfo, targetState commonpb.SegmentState, metricMutation *segMetricMutation) {
	log.Ctx(context.TODO()).Debug("updating segment state and updating metrics",
		zap.Int64("segmentID", segToUpdate.GetID()),
		zap.String("old state", segToUpdate.GetState().String()),
		zap.String("new state", targetState.String()),
		zap.Int64("# of rows", segToUpdate.GetNumOfRows()))
	metricMutation.append(segToUpdate.GetState(), targetState, segToUpdate.GetLevel(), segToUpdate.GetIsSorted(), segToUpdate.GetStorageVersion(), segToUpdate.GetNumOfRows())
	segToUpdate.State = targetState
	if targetState == commonpb.SegmentState_Dropped {
		segToUpdate.DroppedAt = uint64(time.Now().UnixNano())
	}
}

func (m *meta) ListCollections() []int64 {
	return m.collections.Keys()
}

func (m *meta) DropCompactionTask(ctx context.Context, task *datapb.CompactionTask) error {
	return m.compactionTaskMeta.DropCompactionTask(ctx, task)
}

func (m *meta) SaveCompactionTask(ctx context.Context, task *datapb.CompactionTask) error {
	return m.compactionTaskMeta.SaveCompactionTask(ctx, task)
}

func (m *meta) GetCompactionTasks(ctx context.Context) map[int64][]*datapb.CompactionTask {
	return m.compactionTaskMeta.GetCompactionTasks()
}

func (m *meta) GetCompactionTasksByTriggerID(ctx context.Context, triggerID int64) []*datapb.CompactionTask {
	return m.compactionTaskMeta.GetCompactionTasksByTriggerID(triggerID)
}

func (m *meta) CleanPartitionStatsInfo(ctx context.Context, info *datapb.PartitionStatsInfo) error {
	removePaths := make([]string, 0)
	partitionStatsPath := path.Join(m.chunkManager.RootPath(), common.PartitionStatsPath,
		metautil.JoinIDPath(info.CollectionID, info.PartitionID),
		info.GetVChannel(), strconv.FormatInt(info.GetVersion(), 10))
	removePaths = append(removePaths, partitionStatsPath)
	analyzeT := m.analyzeMeta.GetTask(info.GetAnalyzeTaskID())
	if analyzeT != nil {
		centroidsFilePath := path.Join(m.chunkManager.RootPath(), common.AnalyzeStatsPath,
			metautil.JoinIDPath(analyzeT.GetTaskID(), analyzeT.GetVersion(), analyzeT.GetCollectionID(),
				analyzeT.GetPartitionID(), analyzeT.GetFieldID()),
			"centroids",
		)
		removePaths = append(removePaths, centroidsFilePath)
		for _, segID := range info.GetSegmentIDs() {
			segmentOffsetMappingFilePath := path.Join(m.chunkManager.RootPath(), common.AnalyzeStatsPath,
				metautil.JoinIDPath(analyzeT.GetTaskID(), analyzeT.GetVersion(), analyzeT.GetCollectionID(),
					analyzeT.GetPartitionID(), analyzeT.GetFieldID(), segID),
				"offset_mapping",
			)
			removePaths = append(removePaths, segmentOffsetMappingFilePath)
		}
	}

	mlog.Debug(ctx, "remove clustering compaction stats files",
		mlog.Int64("collectionID", info.GetCollectionID()),
		mlog.Int64("partitionID", info.GetPartitionID()),
		mlog.String("vChannel", info.GetVChannel()),
		mlog.Int64("planID", info.GetVersion()),
		mlog.Strings("removePaths", removePaths))
	err := m.chunkManager.MultiRemove(context.Background(), removePaths)
	if err != nil {
		mlog.Warn(ctx, "remove clustering compaction stats files failed", mlog.Err(err))
		return err
	}

	// first clean analyze task
	if err = m.analyzeMeta.DropAnalyzeTask(ctx, info.GetAnalyzeTaskID()); err != nil {
		mlog.Warn(ctx, "remove analyze task failed", mlog.Int64("analyzeTaskID", info.GetAnalyzeTaskID()), mlog.Err(err))
		return err
	}

	// finally, clean up the partition stats info, and make sure the analysis task is cleaned up
	err = m.partitionStatsMeta.DropPartitionStatsInfo(ctx, info)
	mlog.Debug(ctx, "drop partition stats meta",
		mlog.Int64("collectionID", info.GetCollectionID()),
		mlog.Int64("partitionID", info.GetPartitionID()),
		mlog.String("vChannel", info.GetVChannel()),
		mlog.Int64("planID", info.GetVersion()))
	if err != nil {
		return err
	}
	return nil
}

func (m *meta) completeSortCompactionMutation(
	ctx context.Context,
	t *datapb.CompactionTask,
	result *datapb.CompactionPlanResult,
) ([]*SegmentInfo, *segMetricMutation, error) {
	ctx = m.opContext(ctx)
	log := log.Ctx(ctx).With(zap.Int64("planID", t.GetPlanID()),
		zap.String("type", t.GetType().String()),
		zap.Int64("collectionID", t.CollectionID),
		zap.Int64("partitionID", t.PartitionID),
		zap.String("channel", t.GetChannel()))
	metricMutation := &segMetricMutation{stateChange: make(segmentMetricStateChange)}
	compactFromSegID := t.GetInputSegments()[0]
	oldSegment := m.segments.GetSegment(compactFromSegID)
	if oldSegment == nil {
		return nil, nil, merr.WrapErrSegmentNotFound(compactFromSegID)
	}

	// Re-validate segment health to prevent race condition with drop collection
	// between ValidateSegmentStateBeforeCompleteCompactionMutation and here
	if !isSegmentHealthy(oldSegment) {
		mlog.Warn(context.TODO(), "input segment was dropped during compaction mutation",
			mlog.Int64("planID", t.GetPlanID()),
			mlog.Int64("segmentID", compactFromSegID),
			mlog.String("state", oldSegment.GetState().String()))
		return nil, nil, merr.WrapErrSegmentNotFound(compactFromSegID, "input segment was dropped")
	}

	resultInvisible := oldSegment.GetIsInvisible()
	if !oldSegment.GetCreatedByCompaction() {
		resultInvisible = false
	}

	resultSegment := result.GetSegments()[0]

	// Compaction normalizes import segments: row timestamps in the output
	// binlogs are already rewritten to commit_ts by the compactor, so
	// recalculateSegmentPosition picks up commit_ts from output binlogs.
	// The fallback is also normalized to commit_ts for safety.
	commitTs := oldSegment.GetCommitTimestamp()
	startPos, dmlPos := recalculateSegmentPosition(resultSegment.GetInsertLogs(), oldSegment.GetInsertChannel(),
		normalizePositionTimestamp(oldSegment.GetStartPosition(), commitTs),
		normalizePositionTimestamp(oldSegment.GetDmlPosition(), commitTs))

	if t.GetSchema() == nil {
		return nil, nil, merr.WrapErrIllegalCompactionPlan("sort compaction task schema is nil")
	}
	outputSchemaVersion := t.GetSchema().GetVersion()

	segmentInfo := &datapb.SegmentInfo{
		CollectionID:              oldSegment.GetCollectionID(),
		PartitionID:               oldSegment.GetPartitionID(),
		InsertChannel:             oldSegment.GetInsertChannel(),
		MaxRowNum:                 oldSegment.GetMaxRowNum(),
		LastExpireTime:            oldSegment.GetLastExpireTime(),
		StartPosition:             startPos,
		DmlPosition:               dmlPos,
		IsImporting:               oldSegment.GetIsImporting(),
		State:                     commonpb.SegmentState_Flushed,
		Level:                     oldSegment.GetLevel(),
		LastLevel:                 oldSegment.GetLastLevel(),
		PartitionStatsVersion:     oldSegment.GetPartitionStatsVersion(),
		LastPartitionStatsVersion: oldSegment.GetLastPartitionStatsVersion(),
		CreatedByCompaction:       oldSegment.GetCreatedByCompaction(),
		IsInvisible:               resultInvisible,
		StorageVersion:            resultSegment.GetStorageVersion(),
		ID:                        resultSegment.GetSegmentID(),
		NumOfRows:                 resultSegment.GetNumOfRows(),
		Binlogs:                   resultSegment.GetInsertLogs(),
		Statslogs:                 resultSegment.GetField2StatslogPaths(),
		TextStatsLogs:             resultSegment.GetTextStatsLogs(),
		Bm25Statslogs:             resultSegment.GetBm25Logs(),
		Deltalogs:                 resultSegment.GetDeltalogs(),
		CompactionFrom:            []int64{compactFromSegID},
		IsSorted:                  resultSegment.GetIsSorted(),
		ManifestPath:              resultSegment.GetManifest(),
		ExpirQuantiles:            resultSegment.GetExpirQuantiles(),
		IsSortedByNamespace:       resultSegment.GetIsSortedByNamespace(),
		SchemaVersion:             outputSchemaVersion,
		CommitTimestamp:           0, // Normalized: row timestamps already rewritten
	}

	segment := NewSegmentInfo(segmentInfo)
	if segment.GetNumOfRows() > 0 {
		metricMutation.addNewSeg(segment.GetState(), segment.GetLevel(), segment.GetIsSorted(), segment.GetStorageVersion(), segmentMetricFormatLabel(segment), segment.GetNumOfRows())
	} else {
		segment.State = commonpb.SegmentState_Dropped
		segment.DroppedAt = uint64(time.Now().UnixNano())
		mlog.Info(context.TODO(), "drop segment due to 0 rows", mlog.Int64("segmentID", segment.GetID()))
	}

	log = log.With(zap.Int64s("compactFrom", []int64{oldSegment.GetID()}), zap.Int64("compactTo", segment.GetID()))

	log.Info("meta update: prepare for complete stats mutation - complete",
		zap.Int64("num rows", segment.GetNumOfRows()),
		zap.Int64("segment size", segment.getSegmentSize()),
		zap.Int64s("expirQuantiles", segment.GetExpirQuantiles()))
	// Persist old (dropped) and new segments atomically, retrying on CAS conflict.
	oldKey := m.segmentKey(oldSegment.GetCollectionID(), oldSegment.GetPartitionID(), oldSegment.GetID())
	newKey := m.segmentKey(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())
	var results []SegmentTxnResult
	var oldClone *SegmentInfo
	err := retry.Do(ctx, func() error {
		cur, version := m.segments.GetSegmentWithVersion(oldSegment.GetID())
		if cur == nil {
			return retry.Unrecoverable(merr.WrapErrSegmentNotFound(oldSegment.GetID()))
		}
		oldClone = cur.Clone()
		oldClone.State = commonpb.SegmentState_Dropped
		oldClone.DroppedAt = uint64(time.Now().UnixNano())
		oldClone.Compacted = true

		txn := m.segmentPersist.Txn(ctx)
		if err := txn.Update(oldKey, oldClone.SegmentInfo, version, BinlogIncrement{}); err != nil {
			return retry.Unrecoverable(err)
		}
		if err := txn.Insert(newKey, segment.SegmentInfo); err != nil {
			log.Warn("fail to build new segment binlog kvs for sort compaction", zap.Error(err))
			return retry.Unrecoverable(err)
		}
		committed, err := txn.Commit()
		if err != nil {
			return err
		}
		results = committed
		return nil
	}, retry.Attempts(segmentCASMaxRetries), retry.Sleep(0), retry.RetryErr(isCASFailed))
	if err != nil {
		log.Warn("fail to persist segments for sort compaction", zap.Error(err))
		return nil, nil, err
	}

	// Update cache and compute metrics from the staged old-segment clone.
	old, existed := m.segments.SetSegment(oldSegment.GetID(), oldClone, results[0].Version)
	if existed && old.GetState() != oldClone.GetState() {
		metricMutation.append(old.GetState(), oldClone.GetState(), oldClone.GetLevel(), oldClone.GetIsSorted(), oldClone.GetStorageVersion(), oldClone.GetNumOfRows())
	}
	m.segments.SetSegment(segment.GetID(), segment, results[1].Version)
	log.Info("meta update: alter in memory meta after compaction - complete")
	return []*SegmentInfo{segment}, metricMutation, nil
}

func (m *meta) completeBumpSchemaVersionCompactionMutation(
	t *datapb.CompactionTask,
	result *datapb.CompactionPlanResult,
) ([]*SegmentInfo, *segMetricMutation, error) {
	metricMutation := &segMetricMutation{stateChange: make(segmentMetricStateChange)}

	// Schema bump compaction has one input and one result.
	if len(t.GetInputSegments()) != 1 {
		return nil, nil, merr.WrapErrIllegalCompactionPlan("schema bump compaction should have exactly one input segment")
	}
	if len(result.GetSegments()) != 1 {
		return nil, nil, merr.WrapErrIllegalCompactionPlan("schema bump compaction result should have exactly one segment")
	}

	segmentID := t.GetInputSegments()[0]
	resultSegment := result.GetSegments()[0]
	oldSegment := m.segments.GetSegment(segmentID)
	if oldSegment == nil {
		return nil, nil, merr.WrapErrSegmentNotFound(segmentID)
	}
	if !isSegmentHealthy(oldSegment) {
		mlog.Warn(context.TODO(), "input segment was dropped during compaction mutation",
			mlog.Int64("planID", t.GetPlanID()),
			mlog.Int64("segmentID", segmentID),
			mlog.String("state", oldSegment.GetState().String()))
		return nil, nil, merr.WrapErrSegmentNotFound(segmentID, "input segment was dropped")
	}
	if t.GetSchema() == nil {
		return nil, nil, merr.WrapErrIllegalCompactionPlan("schema bump compaction requires task schema")
	}
	newSchemaVersion := t.GetSchema().GetVersion()
	if newSchemaVersion < oldSegment.GetSchemaVersion() {
		return nil, nil, merr.WrapErrIllegalCompactionPlan("schema bump compaction schema version is older than input segment")
	}
	if oldSegment.GetIsInvisible() {
		return nil, nil, merr.WrapErrIllegalCompactionPlan("schema bump compaction input segment should not be invisible")
	}
	if resultSegment.GetNumOfRows() == 0 && resultSegment.GetSegmentID() != segmentID {
		if resultSegment.GetStorageVersion() < storage.StorageV3 {
			return nil, nil, merr.WrapErrIllegalCompactionPlan("schema bump compaction result should contain a StorageV3 segment")
		}
		return m.completeBumpSchemaVersionReplacementMutation(metricMutation, t, oldSegment, resultSegment, newSchemaVersion)
	}

	resultManifest := resultSegment.GetManifest()
	if resultSegment.GetStorageVersion() < storage.StorageV3 || resultManifest == "" {
		return nil, nil, merr.WrapErrIllegalCompactionPlan("schema bump compaction result should contain a StorageV3 manifest")
	}
	if resultSegment.GetSegmentID() != segmentID {
		return m.completeBumpSchemaVersionReplacementMutation(metricMutation, t, oldSegment, resultSegment, newSchemaVersion)
	}

	currentManifest := oldSegment.GetManifestPath()
	if currentManifest == "" {
		return nil, nil, merr.WrapErrIllegalCompactionPlan("schema bump compaction input segment should contain a StorageV3 manifest")
	}
	if currentManifest != resultManifest {
		manifestCompare, err := packed.CompareManifestPath(resultManifest, currentManifest)
		if err != nil {
			return nil, nil, merr.WrapErrIllegalCompactionPlanMsg("schema bump compaction result manifest is not comparable with current manifest: %v", err)
		}
		if manifestCompare <= 0 {
			return nil, nil, merr.WrapErrIllegalCompactionPlan("schema bump compaction result manifest is not newer than current manifest")
		}
	}

	var oldSchemaVersion int32
	var droppedErr error
	updated, err := m.updateSegmentCAS(m.ctx, segmentID, func(cloned *SegmentInfo) (BinlogIncrement, bool) {
		// Re-validate segment health to prevent race condition with drop collection
		// between ValidateSegmentStateBeforeCompleteCompactionMutation and here.
		if !isSegmentHealthy(cloned) {
			log.Warn("input segment was dropped during compaction mutation",
				zap.Int64("planID", t.GetPlanID()),
				zap.Int64("segmentID", segmentID),
				zap.String("state", cloned.GetState().String()))
			droppedErr = merr.WrapErrSegmentNotFound(segmentID, "input segment was dropped")
			return BinlogIncrement{}, false
		}
		oldSchemaVersion = cloned.GetSchemaVersion()

		// Replace binlogs with the merged result (original fields + new function output field).
		// For V3 segments, buildMergedLogsV3 assigns LogID!=0 so buildBinlogKvs validation passes
		// and getSegmentBinlogFields can detect the new field via ChildFields.
		cloned.Binlogs = resultSegment.GetInsertLogs()

		// Update BM25 stats logs: for V2 segments, stats are separate files tracked in Bm25Statslogs.
		// Merge so that stats for previously backfilled BM25 fields are preserved when a second
		// AlterCollectionSchema adds another BM25 function. Before merging, filter out entries
		// whose (fieldID, logID) already exist so that crash-replay — where the same result is
		// applied twice because datacoord crashed between the etcd write and the task state
		// transition — does not produce duplicate stats entries.
		// For V3 segments (manifest-based), BM25 stats are embedded in the manifest and
		// Bm25Logs in the result is nil — skip updating Bm25Statslogs to avoid clearing existing stats.
		if resultSegment.GetManifest() == "" {
			dedupedBm25Logs := filterDuplicateFieldBinlogs(cloned.GetBm25Statslogs(), resultSegment.GetBm25Logs())
			cloned.Bm25Statslogs = mergeFieldBinlogs(cloned.GetBm25Statslogs(), dedupedBm25Logs)
		}

		// Update SchemaVersion from task schema.
		// t.Schema is set from collection.Schema at task creation time and should never be nil.
		// A nil schema here indicates data corruption (e.g. etcd serialization loss); we warn
		// and skip the update so the segment's schemaVersion remains stale, causing the next
		// backfill trigger to re-evaluate and re-submit the task.
		if t.GetSchema() == nil {
			log.Warn("backfill compaction task has nil schema, skipping schemaVersion update — segment will be re-evaluated on next trigger",
				zap.Int64("segmentID", segmentID),
				zap.Int64("planID", t.GetPlanID()))
		} else if newVer := t.GetSchema().GetVersion(); newVer > cloned.GetSchemaVersion() {
			cloned.SchemaVersion = newVer
		}

		// Update StorageVersion only when result has manifest (true V3 segment).
		// V2 segments on V3 clusters stay V2 — backfill forces V2 path to avoid
		// creating partial manifests that would corrupt segment loading.
		if resultSegment.GetManifest() != "" {
			cloned.StorageVersion = resultSegment.GetStorageVersion()
			cloned.ManifestPath = resultSegment.GetManifest()
		}

		return BinlogIncrement{
			Binlogs:       cloned.Binlogs,
			Bm25Statslogs: cloned.Bm25Statslogs,
		}, true
	})
	if err != nil {
		log.Warn("fail to alter segment for backfill compaction", zap.Error(err))
		return nil, nil, err
	}
	if droppedErr != nil {
		return nil, nil, droppedErr
	}

	log.Info("meta update: alter in memory meta after backfill compaction - complete",
		zap.Int64("segmentID", segmentID),
		zap.Int32("oldSchemaVersion", oldSchemaVersion),
		zap.Int32("newSchemaVersion", updated.GetSchemaVersion()),
		zap.Int("newInsertLogsCount", len(resultSegment.GetInsertLogs())),
		zap.Int("newBm25LogsCount", len(resultSegment.GetBm25Logs())),
		zap.Int64("num rows", updated.GetNumOfRows()))

	return []*SegmentInfo{updated}, metricMutation, nil
}

func (m *meta) completeBumpSchemaVersionReplacementMutation(
	metricMutation *segMetricMutation,
	t *datapb.CompactionTask,
	oldSegment *SegmentInfo,
	resultSegment *datapb.CompactionSegment,
	schemaVersion int32,
) ([]*SegmentInfo, *segMetricMutation, error) {
	idRange := t.GetPreAllocatedSegmentIDs()
	if idRange == nil || idRange.GetBegin() >= idRange.GetEnd() || resultSegment.GetSegmentID() != idRange.GetBegin() {
		return nil, nil, merr.WrapErrIllegalCompactionPlanMsg("schema bump replacement result segment ID %d does not match the pre-allocated segment ID range", resultSegment.GetSegmentID())
	}

	dropped := oldSegment.Clone()
	dropped.Compacted = true
	updateSegStateAndPrepareMetrics(dropped, commonpb.SegmentState_Dropped, metricMutation)

	startPos, dmlPos := recalculateSegmentPosition(resultSegment.GetInsertLogs(), oldSegment.GetInsertChannel(), oldSegment.GetStartPosition(), oldSegment.GetDmlPosition())
	newSegment := NewSegmentInfo(&datapb.SegmentInfo{
		ID:                        resultSegment.GetSegmentID(),
		CollectionID:              oldSegment.GetCollectionID(),
		PartitionID:               oldSegment.GetPartitionID(),
		InsertChannel:             oldSegment.GetInsertChannel(),
		MaxRowNum:                 oldSegment.GetMaxRowNum(),
		LastExpireTime:            oldSegment.GetLastExpireTime(),
		StartPosition:             startPos,
		DmlPosition:               dmlPos,
		IsImporting:               oldSegment.GetIsImporting(),
		State:                     commonpb.SegmentState_Flushed,
		Level:                     oldSegment.GetLevel(),
		LastLevel:                 oldSegment.GetLastLevel(),
		PartitionStatsVersion:     oldSegment.GetPartitionStatsVersion(),
		LastPartitionStatsVersion: oldSegment.GetLastPartitionStatsVersion(),
		CreatedByCompaction:       true,
		IsInvisible:               false,
		StorageVersion:            resultSegment.GetStorageVersion(),
		NumOfRows:                 resultSegment.GetNumOfRows(),
		Binlogs:                   resultSegment.GetInsertLogs(),
		Statslogs:                 resultSegment.GetField2StatslogPaths(),
		TextStatsLogs:             resultSegment.GetTextStatsLogs(),
		Bm25Statslogs:             resultSegment.GetBm25Logs(),
		Deltalogs:                 resultSegment.GetDeltalogs(),
		CompactionFrom:            []int64{oldSegment.GetID()},
		IsSorted:                  oldSegment.GetIsSorted(),
		ManifestPath:              resultSegment.GetManifest(),
		ExpirQuantiles:            resultSegment.GetExpirQuantiles(),
		IsSortedByNamespace:       oldSegment.GetIsSortedByNamespace(),
		SchemaVersion:             schemaVersion,
	})
	if newSegment.GetNumOfRows() > 0 {
		metricMutation.addNewSeg(newSegment.GetState(), newSegment.GetLevel(), newSegment.GetIsSorted(), newSegment.GetStorageVersion(), segmentMetricFormatLabel(newSegment), newSegment.GetNumOfRows())
	} else {
		newSegment.State = commonpb.SegmentState_Dropped
		newSegment.DroppedAt = uint64(time.Now().UnixNano())
	}

	binlogsIncrement := metastore.BinlogsIncrement{Segment: newSegment.SegmentInfo}
	mlog.Info(m.ctx, "meta update: prepare replacement for schema bump full rewrite", mlog.Int64("num rows", newSegment.GetNumOfRows()))

	if err := m.catalog.AlterSegments(m.ctx, []*datapb.SegmentInfo{dropped.SegmentInfo, newSegment.SegmentInfo}, binlogsIncrement); err != nil {
		mlog.Warn(m.ctx, "fail to alter replacement segments for schema bump compaction", mlog.Err(err))
		return nil, nil, err
	}

	m.segments.SetSegment(dropped.GetID(), dropped, math.MaxInt64)
	m.segments.SetSegment(newSegment.GetID(), newSegment, math.MaxInt64)
	mlog.Info(m.ctx, "meta update: alter in memory meta after schema bump full rewrite replacement - complete")
	return []*SegmentInfo{newSegment}, metricMutation, nil
}

func (m *meta) getSegmentsMetrics(collectionID int64) []*metricsinfo.Segment {
	allSegments := m.segments.GetSegments()
	segments := make([]*metricsinfo.Segment, 0, len(allSegments))
	for _, s := range allSegments {
		if collectionID <= 0 || s.GetCollectionID() == collectionID {
			segments = append(segments, &metricsinfo.Segment{
				SegmentID:    s.ID,
				CollectionID: s.CollectionID,
				PartitionID:  s.PartitionID,
				Channel:      s.InsertChannel,
				NumOfRows:    s.NumOfRows,
				State:        s.State.String(),
				MemSize:      s.size.Load(),
				Level:        s.Level.String(),
				IsImporting:  s.IsImporting,
				Compacted:    s.Compacted,
				IsSorted:     s.IsSorted,
				NodeID:       paramtable.GetNodeID(),
			})
		}
	}

	return segments
}

func (m *meta) DropSegmentsOfPartition(ctx context.Context, partitionIDs []int64) error {
	// Collect segments to drop (read-only from cache for key construction).
	var segRefs []segKeyed
	for _, seg := range m.segments.GetSegments() {
		if contains(partitionIDs, seg.PartitionID) {
			segRefs = append(segRefs, segKeyed{
				id:  seg.GetID(),
				key: m.segmentKey(seg.GetCollectionID(), seg.GetPartitionID(), seg.GetID()),
			})
		}
	}

	results, pendings, err := m.batchSetDroppedWithCAS(m.ctx, segRefs)
	if err != nil {
		return err
	}

	metricMutation := &segMetricMutation{
		stateChange: make(map[string]map[string]map[string]map[string]int),
	}
	for i, p := range pendings {
		newSeg := p.clone
		oldSeg, existed := m.segments.SetSegment(p.id, newSeg, results[i].Version)
		if existed && oldSeg.GetState() != newSeg.GetState() {
			metricMutation.append(oldSeg.GetState(), newSeg.GetState(), newSeg.GetLevel(), newSeg.GetIsSorted(), newSeg.GetStorageVersion(), newSeg.GetNumOfRows())
		}
	}
	metricMutation.commit()
	return nil
}

// segKeyed carries a segment ID and its persist key for batch Drop paths.
type segKeyed struct {
	id  int64
	key string
}

// segPending carries a staged segment mutation and its cache-ready clone.
type segPending struct {
	segKeyed
	clone *SegmentInfo
}

// batchSetDroppedWithCAS marks every referenced segment as Dropped atomically,
// retrying on CAS conflicts. Returns the commit results and the pending
// entries actually staged (in order), so callers can update in-memory state.
func (m *meta) batchSetDroppedWithCAS(ctx context.Context, segRefs []segKeyed) ([]SegmentTxnResult, []segPending, error) {
	var results []SegmentTxnResult
	var pendings []segPending
	err := retry.Do(ctx, func() error {
		txn := m.segmentPersist.Txn(ctx)
		pendings = make([]segPending, 0, len(segRefs))
		for _, ref := range segRefs {
			cur, version := m.segments.GetSegmentWithVersion(ref.id)
			if cur == nil {
				continue
			}
			clone := cur.Clone()
			clone.State = commonpb.SegmentState_Dropped
			clone.DroppedAt = uint64(time.Now().UnixNano())
			if err := txn.Update(ref.key, clone.SegmentInfo, version, BinlogIncrement{}); err != nil {
				return retry.Unrecoverable(err)
			}
			pendings = append(pendings, segPending{segKeyed: ref, clone: clone})
		}
		if len(pendings) == 0 {
			return nil
		}
		committed, err := txn.Commit()
		if err != nil {
			return err
		}
		results = committed
		return nil
	}, retry.Attempts(segmentCASMaxRetries), retry.Sleep(0), retry.RetryErr(isCASFailed))
	if err != nil {
		return nil, nil, err
	}
	return results, pendings, nil
}

func contains(arr []int64, target int64) bool {
	for _, val := range arr {
		if val == target {
			return true
		}
	}
	return false
}

func (m *meta) UpdateFileResources(ctx context.Context, resources []*internalpb.FileResourceInfo, version uint64) error {
	m.resourceLock.Lock()
	defer m.resourceLock.Unlock()
	m.resourceIDMap = make(map[int64]*internalpb.FileResourceInfo)
	for _, resource := range resources {
		m.resourceIDMap[resource.Id] = resource
	}
	m.resourceVersion = version

	return nil
}

func (m *meta) ListFileResources(ctx context.Context) ([]*internalpb.FileResourceInfo, uint64) {
	m.resourceLock.RLock()
	defer m.resourceLock.RUnlock()
	return lo.Values(m.resourceIDMap), m.resourceVersion
}

func (m *meta) GetFileResources(ctx context.Context, resourceIDs ...int64) ([]*internalpb.FileResourceInfo, error) {
	m.resourceLock.RLock()
	defer m.resourceLock.RUnlock()

	resources := make([]*internalpb.FileResourceInfo, 0)
	for _, id := range resourceIDs {
		if resource, ok := m.resourceIDMap[id]; ok {
			resources = append(resources, resource)
		} else {
			return nil, merr.WrapErrServiceInternalMsg("file resource %d not found", id)
		}
	}
	return resources, nil
}

// TruncateChannelByTime drops segments of a channel that were updated before the flush timestamp
func (m *meta) TruncateChannelByTime(ctx context.Context, vChannel string, flushTs uint64) error {
	segments := m.segments.GetSegmentsBySelector(SegmentFilterFunc(isSegmentHealthy), WithChannel(vChannel))

	// Collect segments to drop (read-only from cache for key construction and filtering).
	var segRefs []segKeyed
	for _, segment := range segments {
		if segment.GetDmlPosition().GetTimestamp() <= flushTs && segment.GetState() != commonpb.SegmentState_Dropped {
			segRefs = append(segRefs, segKeyed{
				id:  segment.GetID(),
				key: m.segmentKey(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID()),
			})
		}
	}

	if len(segRefs) == 0 {
		return nil
	}

	results, pendings, err := m.batchSetDroppedWithCAS(ctx, segRefs)
	if err != nil {
		log.Ctx(ctx).Warn("Failed to batch set segments state to dropped", zap.Error(err))
		return err
	}

	metricMutation := &segMetricMutation{
		stateChange: make(map[string]map[string]map[string]map[string]int),
	}
	for i, p := range pendings {
		newSeg := p.clone
		oldSeg, existed := m.segments.SetSegment(p.id, newSeg, results[i].Version)
		if existed && oldSeg.GetState() != newSeg.GetState() {
			metricMutation.append(oldSeg.GetState(), newSeg.GetState(), newSeg.GetLevel(), newSeg.GetIsSorted(), newSeg.GetStorageVersion(), newSeg.GetNumOfRows())
		}
	}
	metricMutation.commit()

	return nil
}

// WatchChannelCheckpoint waits until the checkpoint of the specified channel
// reaches or exceeds the target timestamp. Used for TruncateCollection.
func (m *meta) WatchChannelCheckpoint(ctx context.Context, vChannel string, targetTs uint64) error {
	m.channelCPs.cond.L.Lock()

	for {
		cp, ok := m.channelCPs.checkpoints[vChannel]
		if ok && cp != nil && cp.GetTimestamp() >= targetTs {
			m.channelCPs.cond.L.Unlock()
			return nil
		}

		if err := m.channelCPs.cond.Wait(ctx); err != nil {
			return err
		}
	}
}
