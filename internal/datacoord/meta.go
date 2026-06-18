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
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/segmentutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
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
	UpdateSegmentsInfo(ctx context.Context, operators ...UpdateOperator) error
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
	ctx     context.Context
	catalog metastore.DataCoordCatalog

	collections *typeutil.ConcurrentMap[UniqueID, *collectionInfo] // collection id to collection info

	segMu    lock.RWMutex
	segments *SegmentsInfo // segment id to segment info

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
func newMeta(ctx context.Context, catalog metastore.DataCoordCatalog, chunkManager storage.ChunkManager, broker broker.Broker) (*meta, error) {
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
		collections:     typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		segments:        NewSegmentsInfo(),
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
	futures := make([]*conc.Future[any], 0, len(collectionIDs))
	collectionSegments := make([][]*datapb.SegmentInfo, len(collectionIDs))
	for i, collectionID := range collectionIDs {
		i := i
		collectionID := collectionID
		futures = append(futures, pool.Submit(func() (any, error) {
			segments, err := m.catalog.ListSegments(m.ctx, collectionID)
			if err != nil {
				return nil, err
			}
			collectionSegments[i] = segments
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
	for _, segments := range collectionSegments {
		numSegments += len(segments)
		for _, segment := range segments {
			// segments from catalog.ListSegments will not have logPath
			m.segments.SetSegment(segment.ID, NewSegmentInfo(segment))
			metrics.DataCoordNumSegments.WithLabelValues(segmentMetricLabelValues(NewSegmentInfo(segment))...).Inc()
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
	m.segMu.RLock()
	defer m.segMu.RUnlock()
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
	m.segMu.RLock()
	defer m.segMu.RUnlock()
	ret := make(map[int64]int64, m.collections.Len())
	segments := m.segments.GetSegments()
	for _, segment := range segments {
		if isSegmentHealthy(segment) {
			ret[segment.GetCollectionID()] += segment.GetNumOfRows()
		}
	}
	return ret
}

// AddSegment records segment info, persisting info into kv store
func (m *meta) AddSegment(ctx context.Context, segment *SegmentInfo) error {
	mlog.Info(context.TODO(), "meta update: adding segment - Start", mlog.Int64("segmentID", segment.GetID()))
	m.segMu.Lock()
	defer m.segMu.Unlock()
	if info := m.segments.GetSegment(segment.GetID()); info != nil {
		mlog.Info(context.TODO(), "segment is already exists, ignore the operation", mlog.Int64("segmentID", segment.ID))
		return nil
	}
	if err := m.catalog.AddSegment(ctx, segment.SegmentInfo); err != nil {
		mlog.Error(context.TODO(), "meta update: adding segment failed",
			mlog.Int64("segmentID", segment.GetID()),
			mlog.Err(err))
		return err
	}
	m.segments.SetSegment(segment.GetID(), segment)

	metrics.DataCoordNumSegments.WithLabelValues(segmentMetricLabelValues(segment)...).Inc()
	mlog.Info(context.TODO(), "meta update: adding segment - complete", mlog.Int64("segmentID", segment.GetID()))
	return nil
}

// DropSegment remove segment with provided id, etcd persistence also removed
func (m *meta) DropSegment(ctx context.Context, segmentID UniqueID) error {
	mlog.Debug(context.TODO(), "meta update: dropping segment", mlog.Int64("segmentID", segmentID))
	m.segMu.Lock()
	defer m.segMu.Unlock()
	segment := m.segments.GetSegment(segmentID)
	if segment == nil {
		mlog.Warn(context.TODO(), "meta update: dropping segment failed - segment not found",
			mlog.Int64("segmentID", segmentID))
		return nil
	}
	if err := m.catalog.DropSegment(ctx, segment.SegmentInfo); err != nil {
		mlog.Warn(context.TODO(), "meta update: dropping segment failed",
			mlog.Int64("segmentID", segmentID),
			mlog.Err(err))
		return err
	}
	metrics.DataCoordNumSegments.WithLabelValues(segmentMetricLabelValues(segment)...).Dec()

	m.segments.DropSegment(segmentID)
	mlog.Info(context.TODO(), "meta update: dropping segment - complete",
		mlog.Int64("segmentID", segmentID))
	return nil
}

// GetHealthySegment returns segment info with provided id
// if not segment is found, nil will be returned
func (m *meta) GetHealthySegment(ctx context.Context, segID UniqueID) *SegmentInfo {
	m.segMu.RLock()
	defer m.segMu.RUnlock()
	segment := m.segments.GetSegment(segID)
	if segment != nil && isSegmentHealthy(segment) {
		return segment
	}
	return nil
}

// Get segments By filter function
func (m *meta) GetSegments(segIDs []UniqueID, filterFunc SegmentInfoSelector) []UniqueID {
	m.segMu.RLock()
	defer m.segMu.RUnlock()
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
	m.segMu.RLock()
	defer m.segMu.RUnlock()
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
	m.segMu.RLock()
	defer m.segMu.RUnlock()
	return m.segments.GetSegment(segID)
}

// GetAllSegmentsUnsafe returns all segments
func (m *meta) GetAllSegmentsUnsafe() []*SegmentInfo {
	m.segMu.RLock()
	defer m.segMu.RUnlock()
	return m.segments.GetSegments()
}

func (m *meta) GetSegmentsTotalNumRows(segmentIDs []UniqueID) int64 {
	m.segMu.RLock()
	defer m.segMu.RUnlock()
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
	m.segMu.RLock()
	defer m.segMu.RUnlock()
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

// SetState setting segment with provided ID state
func (m *meta) SetState(ctx context.Context, segmentID UniqueID, targetState commonpb.SegmentState) error {
	mlog.Debug(context.TODO(), "meta update: setting segment state",
		mlog.Int64("segmentID", segmentID),
		mlog.Any("target state", targetState))
	m.segMu.Lock()
	defer m.segMu.Unlock()
	curSegInfo := m.segments.GetSegment(segmentID)
	if curSegInfo == nil {
		mlog.Warn(context.TODO(), "meta update: setting segment state - segment not found",
			mlog.Int64("segmentID", segmentID),
			mlog.Any("target state", targetState))
		// idempotent drop
		if targetState == commonpb.SegmentState_Dropped {
			return nil
		}
		return merr.WrapErrSegmentNotFound(segmentID)
	}
	// Persist segment updates first.
	clonedSegment := curSegInfo.Clone()
	metricMutation := &segMetricMutation{
		stateChange: make(segmentMetricStateChange),
	}
	if clonedSegment != nil && isSegmentHealthy(clonedSegment) {
		// Update segment state and prepare segment metric update.
		updateSegStateAndPrepareMetrics(clonedSegment, targetState, metricMutation)
		if err := m.catalog.AlterSegments(ctx, []*datapb.SegmentInfo{clonedSegment.SegmentInfo}); err != nil {
			mlog.Warn(context.TODO(), "meta update: setting segment state - failed to alter segments",
				mlog.Int64("segmentID", segmentID),
				mlog.String("target state", targetState.String()),
				mlog.Err(err))
			return err
		}
		// Apply segment metric update after successful meta update.
		metricMutation.commit()
		// Update in-memory meta.
		m.segments.SetSegment(segmentID, clonedSegment)
	}
	mlog.Info(context.TODO(), "meta update: setting segment state - complete",
		mlog.Int64("segmentID", segmentID),
		mlog.String("target state", targetState.String()))
	return nil
}

func (m *meta) UpdateSegment(segmentID int64, operators ...SegmentOperator) error {
	m.segMu.Lock()
	defer m.segMu.Unlock()
	info := m.segments.GetSegment(segmentID)
	if info == nil {
		mlog.Warn(context.TODO(), "meta update: UpdateSegment - segment not found",
			mlog.Int64("segmentID", segmentID))

		return merr.WrapErrSegmentNotFound(segmentID)
	}
	// Persist segment updates first.
	cloned := info.Clone()

	var updated bool
	for _, operator := range operators {
		if operator(cloned) {
			updated = true
		}
	}

	if !updated {
		mlog.Warn(context.TODO(), "meta update:UpdateSegmnt skipped, no update",
			mlog.Int64("segmentID", segmentID),
		)
		return nil
	}

	if err := m.catalog.AlterSegments(m.ctx, []*datapb.SegmentInfo{cloned.SegmentInfo}); err != nil {
		mlog.Warn(context.TODO(), "meta update: update segment - failed to alter segments",
			mlog.Int64("segmentID", segmentID),
			mlog.Err(err))
		return err
	}
	// Update in-memory meta.
	m.segments.SetSegment(segmentID, cloned)

	mlog.Info(context.TODO(), "meta update: update segment - complete",
		mlog.Int64("segmentID", segmentID))
	return nil
}

type updateSegmentPack struct {
	meta     *meta
	segments map[int64]*SegmentInfo
	// for update etcd binlog paths
	increments map[int64]metastore.BinlogsIncrement
	// for update segment metric after alter segments
	metricMutation              *segMetricMutation
	fromSaveBinlogPathSegmentID int64 // if true, the operator is from save binlog paths
	l0ManifestUpdates           []*l0ManifestUpdate
	err                         error
}

func (p *updateSegmentPack) fail(err error) bool {
	if err != nil {
		p.err = err
	}
	return false
}

func (p *updateSegmentPack) Validate() error {
	if p.fromSaveBinlogPathSegmentID != 0 {
		segment, ok := p.segments[p.fromSaveBinlogPathSegmentID]
		if !ok {
			panic(fmt.Sprintf("segment %d not found when validating save binlog paths", p.fromSaveBinlogPathSegmentID))
		}
		if segment.Level == datapb.SegmentLevel_L0 {
			return nil
		}
		segmentInMeta := p.meta.segments.GetSegment(segment.ID)
		if segmentInMeta.State == commonpb.SegmentState_Flushed && segment.State != commonpb.SegmentState_Dropped {
			// if the segment is flushed, we should not update the segment meta, ignore the operation directly.
			return merr.Wrapf(errIgnoredSegmentMetaOperation,
				"segment is flushed, segmentID: %d",
				segment.ID)
		}
		if segment.GetDmlPosition().GetTimestamp() < segmentInMeta.GetDmlPosition().GetTimestamp() {
			return merr.Wrapf(errIgnoredSegmentMetaOperation,
				"dml time tick is less than the segment meta, segmentID: %d, new incoming time tick: %d, existing time tick: %d",
				segment.ID,
				segment.GetDmlPosition().GetTimestamp(),
				segmentInMeta.GetDmlPosition().GetTimestamp())
		}
	}
	return nil
}

func (p *updateSegmentPack) Get(segmentID int64) *SegmentInfo {
	if segment, ok := p.segments[segmentID]; ok {
		return segment
	}

	segment := p.meta.segments.GetSegment(segmentID)
	if segment == nil {
		mlog.Warn(context.TODO(), "meta update: get segment failed - segment not found",
			mlog.Int64("segmentID", segmentID),
			mlog.Bool("segment nil", segment == nil),
			mlog.Bool("segment unhealthy", !isSegmentHealthy(segment)))
		return nil
	}

	p.segments[segmentID] = segment.Clone()
	return p.segments[segmentID]
}

type UpdateOperator func(*updateSegmentPack) bool

func CreateL0Operator(collectionID, partitionID, segmentID int64, channel string) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.meta.segments.GetSegment(segmentID)
		if segment == nil {
			mlog.Info(context.TODO(), "meta update: add new l0 segment",
				mlog.Int64("collectionID", collectionID),
				mlog.Int64("partitionID", partitionID),
				mlog.Int64("segmentID", segmentID))

			modPack.segments[segmentID] = NewSegmentInfo(&datapb.SegmentInfo{
				ID:            segmentID,
				CollectionID:  collectionID,
				PartitionID:   partitionID,
				InsertChannel: channel,
				State:         commonpb.SegmentState_Flushed,
				Level:         datapb.SegmentLevel_L0,
			})
			modPack.metricMutation.addNewSeg(commonpb.SegmentState_Flushed, datapb.SegmentLevel_L0, false, 0, segmentMetricFormatLegacy, 0)
		}
		return true
	}
}

func UpdateStorageVersionOperator(segmentID int64, version int64) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			mlog.Info(context.TODO(), "meta update: update storage version - segment not found",
				mlog.Int64("segmentID", segmentID))
			return false
		}

		segment.StorageVersion = version
		return true
	}
}

// Set status of segment
// and record dropped time when change segment status to dropped
func UpdateStatusOperator(segmentID int64, status commonpb.SegmentState) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			mlog.Warn(context.TODO(), "meta update: update status failed - segment not found",
				mlog.Int64("segmentID", segmentID),
				mlog.String("status", status.String()))
			return false
		}

		if segment.GetState() == status {
			mlog.Info(context.TODO(), "meta update: segment stats already is target state",
				mlog.Int64("segmentID", segmentID), mlog.String("status", status.String()))
			return false
		}

		updateSegStateAndPrepareMetrics(segment, status, modPack.metricMutation)
		if status == commonpb.SegmentState_Dropped {
			segment.DroppedAt = uint64(time.Now().UnixNano())
		}
		return true
	}
}

// Set storage version
func SetStorageVersion(segmentID int64, version int64) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			mlog.Warn(context.TODO(), "meta update: update storage version failed - segment not found",
				mlog.Int64("segmentID", segmentID))
			return false
		}

		if segment.GetStorageVersion() == version {
			mlog.Info(context.TODO(), "meta update: segment stats already is target version",
				mlog.Int64("segmentID", segmentID), mlog.Int64("version", version))
			return false
		}

		segment.StorageVersion = version
		return true
	}
}

func UpdateCompactedOperator(segmentID int64) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			mlog.Warn(context.TODO(), "meta update: update binlog failed - segment not found",
				mlog.Int64("segmentID", segmentID))
			return false
		}
		segment.Compacted = true
		return true
	}
}

func SetSegmentIsInvisible(segmentID int64, isInvisible bool) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			mlog.Warn(context.TODO(), "meta update: update segment visible fail - segment not found",
				mlog.Int64("segmentID", segmentID))
			return false
		}
		segment.IsInvisible = isInvisible
		return true
	}
}

func UpdateSegmentLevelOperator(segmentID int64, level datapb.SegmentLevel) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			mlog.Warn(context.TODO(), "meta update: update level fail - segment not found",
				mlog.Int64("segmentID", segmentID))
			return false
		}
		if segment.LastLevel == segment.Level && segment.Level == level {
			mlog.Debug(context.TODO(), "segment already is this level", mlog.Int64("segID", segmentID), mlog.String("level", level.String()))
			return true
		}
		segment.LastLevel = segment.Level
		segment.Level = level
		return true
	}
}

func UpdateSegmentPartitionStatsVersionOperator(segmentID int64, version int64) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			mlog.Warn(context.TODO(), "meta update: update partition stats version fail - segment not found",
				mlog.Int64("segmentID", segmentID))
			return false
		}
		segment.LastPartitionStatsVersion = segment.PartitionStatsVersion
		segment.PartitionStatsVersion = version
		mlog.Debug(context.TODO(), "update segment version", mlog.Int64("segmentID", segmentID), mlog.Int64("PartitionStatsVersion", version), mlog.Int64("LastPartitionStatsVersion", segment.LastPartitionStatsVersion))
		return true
	}
}

func RevertSegmentLevelOperator(segmentID int64) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			mlog.Warn(context.TODO(), "meta update: revert level fail - segment not found",
				mlog.Int64("segmentID", segmentID))
			return false
		}
		// just for compatibility,
		if segment.GetLevel() != segment.GetLastLevel() && segment.GetLastLevel() != datapb.SegmentLevel_Legacy {
			segment.Level = segment.LastLevel
			mlog.Debug(context.TODO(), "revert segment level", mlog.Int64("segmentID", segmentID), mlog.String("LastLevel", segment.LastLevel.String()))
			return true
		}
		return false
	}
}

func RevertSegmentPartitionStatsVersionOperator(segmentID int64) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			mlog.Warn(context.TODO(), "meta update: revert level fail - segment not found",
				mlog.Int64("segmentID", segmentID))
			return false
		}
		segment.PartitionStatsVersion = segment.LastPartitionStatsVersion
		mlog.Debug(context.TODO(), "revert segment partition stats version", mlog.Int64("segmentID", segmentID), mlog.Int64("LastPartitionStatsVersion", segment.LastPartitionStatsVersion))
		return true
	}
}

// Add binlogs in segmentInfo
func AddBinlogsOperator(segmentID int64, binlogs, statslogs, deltalogs, bm25logs []*datapb.FieldBinlog) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			mlog.Warn(context.TODO(), "meta update: add binlog failed - segment not found",
				mlog.Int64("segmentID", segmentID))
			return false
		}

		segment.Binlogs = mergeFieldBinlogs(segment.GetBinlogs(), binlogs)
		segment.Statslogs = mergeFieldBinlogs(segment.GetStatslogs(), statslogs)
		segment.Deltalogs = mergeFieldBinlogs(segment.GetDeltalogs(), deltalogs)
		if len(deltalogs) > 0 {
			segment.deltaRowcount.Store(-1)
		}

		segment.Bm25Statslogs = mergeFieldBinlogs(segment.GetBm25Statslogs(), bm25logs)
		modPack.increments[segmentID] = metastore.BinlogsIncrement{
			Segment: segment.SegmentInfo,
			UpdateMask: metastore.BinlogsUpdateMask{
				WithoutBinlogs:       len(binlogs) == 0,
				WithoutDeltalogs:     len(deltalogs) == 0,
				WithoutStatslogs:     len(statslogs) == 0,
				WithoutBm25Statslogs: len(bm25logs) == 0,
			},
		}
		return true
	}
}

func addDeltalogsToSegment(modPack *updateSegmentPack, segmentID int64, segment *SegmentInfo, deltalogs []*datapb.FieldBinlog) bool {
	if len(deltalogs) == 0 {
		return false
	}

	segment.Deltalogs = mergeFieldBinlogs(segment.GetDeltalogs(), deltalogs)
	segment.deltaRowcount.Store(-1)
	modPack.increments[segmentID] = metastore.BinlogsIncrement{
		Segment: segment.SegmentInfo,
		UpdateMask: metastore.BinlogsUpdateMask{
			WithoutBinlogs:       true,
			WithoutDeltalogs:     false,
			WithoutStatslogs:     true,
			WithoutBm25Statslogs: true,
		},
	}
	return true
}

func clearBinlogPaths(fieldBinlogs []*datapb.FieldBinlog) {
	for _, fieldBinlog := range fieldBinlogs {
		for _, binlog := range fieldBinlog.GetBinlogs() {
			binlog.LogPath = ""
		}
	}
}

func updateManifestPathIfNewer(segment *SegmentInfo, manifestPath string) error {
	if manifestPath == "" || segment.GetManifestPath() == manifestPath {
		return nil
	}

	currentBase, currentVersion, err := packed.UnmarshalManifestPath(segment.GetManifestPath())
	if err != nil {
		return err
	}
	incomingBase, incomingVersion, err := packed.UnmarshalManifestPath(manifestPath)
	if err != nil {
		return err
	}
	if currentBase != incomingBase {
		return merr.WrapErrServiceInternalMsg("manifest base path mismatch for segment %d: current %s, incoming %s", segment.GetID(), currentBase, incomingBase)
	}
	if incomingVersion > currentVersion {
		segment.ManifestPath = manifestPath
	}
	return nil
}

type l0ManifestUpdate struct {
	segmentID            int64
	deltalogs            []*datapb.FieldBinlog
	storageConfig        *indexpb.StorageConfig
	committedV3Manifests map[int64]string
	segment              *SegmentInfo
	manifestPath         string
	entries              []packed.DeltaLogEntry
}

func (u *l0ManifestUpdate) prepare(modPack *updateSegmentPack) bool {
	u.segment = modPack.Get(u.segmentID)
	if u.segment == nil {
		mlog.Warn(context.TODO(), "meta update: add L0 deltalog failed - segment not found",
			mlog.Int64("segmentID", u.segmentID))
		return false
	}
	if len(u.deltalogs) == 0 {
		return false
	}

	if u.segment.GetManifestPath() == "" {
		if err := binlog.CompressFieldBinlogs(u.deltalogs); err != nil {
			return modPack.fail(err)
		}
		return true
	}

	if u.committedV3Manifests != nil {
		u.manifestPath = u.committedV3Manifests[u.segmentID]
	}
	if u.manifestPath != "" {
		return true
	}

	entries, err := buildL0V3DeltaLogEntries(u.segmentID, u.deltalogs)
	if err != nil {
		return modPack.fail(err)
	}
	if len(entries) == 0 {
		return false
	}
	u.entries = entries
	return true
}

func (u *l0ManifestUpdate) commitManifest() error {
	if u.segment.GetManifestPath() == "" || u.manifestPath != "" || len(u.entries) == 0 {
		return nil
	}
	manifestPath, err := packed.AddDeltaLogsToManifestOverwrite(u.segment.GetManifestPath(), u.storageConfig, u.entries)
	if err != nil {
		return err
	}
	u.manifestPath = manifestPath
	return nil
}

func commitL0ManifestUpdates(updates []*l0ManifestUpdate) error {
	updates = lo.Filter(updates, func(update *l0ManifestUpdate, _ int) bool {
		return update.segment.GetManifestPath() != ""
	})
	if len(updates) == 0 {
		return nil
	}

	groups := make(map[int64][]*l0ManifestUpdate)
	for _, update := range updates {
		groups[update.segmentID] = append(groups[update.segmentID], update)
	}

	poolSize := paramtable.Get().DataCoordCfg.L0ManifestUpdatePoolSize.GetAsInt()
	if poolSize < 1 {
		poolSize = 1
	}
	if poolSize > len(groups) {
		poolSize = len(groups)
	}

	pool := conc.NewPool[struct{}](poolSize)
	defer pool.Release()

	futures := make([]*conc.Future[struct{}], 0, len(groups))
	for _, group := range groups {
		group := group
		futures = append(futures, pool.Submit(func() (struct{}, error) {
			return struct{}{}, commitL0ManifestUpdateGroup(group)
		}))
	}
	err := conc.BlockOnAll(futures...)
	for _, update := range updates {
		if update.committedV3Manifests != nil && update.manifestPath != "" {
			update.committedV3Manifests[update.segmentID] = update.manifestPath
		}
	}
	return err
}

func commitL0ManifestUpdateGroup(updates []*l0ManifestUpdate) error {
	for _, update := range updates {
		if update.manifestPath != "" {
			if err := updateManifestPathIfNewer(update.segment, update.manifestPath); err != nil {
				return err
			}
			continue
		}
		if len(update.entries) == 0 {
			continue
		}
		if err := update.commitManifest(); err != nil {
			return err
		}
		if err := updateManifestPathIfNewer(update.segment, update.manifestPath); err != nil {
			return err
		}
	}
	return nil
}

func (u *l0ManifestUpdate) apply(modPack *updateSegmentPack) bool {
	if u.segment.GetManifestPath() != "" {
		if err := updateManifestPathIfNewer(u.segment, u.manifestPath); err != nil {
			return modPack.fail(err)
		}
		clearBinlogPaths(u.deltalogs)
	}
	return addDeltalogsToSegment(modPack, u.segmentID, u.segment, u.deltalogs)
}

func AddL0DeltalogsAndUpdateManifestOperator(
	segmentID int64,
	deltalogs []*datapb.FieldBinlog,
	storageConfig *indexpb.StorageConfig,
	committedV3Manifests map[int64]string,
) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		update := &l0ManifestUpdate{
			segmentID:            segmentID,
			deltalogs:            deltalogs,
			storageConfig:        storageConfig,
			committedV3Manifests: committedV3Manifests,
		}
		if !update.prepare(modPack) {
			return false
		}
		modPack.l0ManifestUpdates = append(modPack.l0ManifestUpdates, update)
		return true
	}
}

func UpdateBinlogsOperator(segmentID int64, binlogs, statslogs, deltalogs, bm25logs []*datapb.FieldBinlog) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			mlog.Warn(context.TODO(), "meta update: update binlog failed - segment not found",
				mlog.Int64("segmentID", segmentID))
			return false
		}

		segment.Binlogs = binlogs
		segment.Statslogs = statslogs
		segment.Deltalogs = deltalogs
		segment.Bm25Statslogs = bm25logs
		modPack.increments[segmentID] = metastore.BinlogsIncrement{
			Segment: segment.SegmentInfo,
		}
		return true
	}
}

func UpdateBinlogsFromSaveBinlogPathsOperator(segmentID int64, binlogs, statslogs, deltalogs, bm25logs []*datapb.FieldBinlog) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		modPack.fromSaveBinlogPathSegmentID = segmentID
		segment := modPack.Get(segmentID)
		if segment == nil {
			mlog.Warn(context.TODO(), "meta update: update binlog failed - segment not found",
				mlog.Int64("segmentID", segmentID))
			return false
		}

		segment.Binlogs = mergeFieldBinlogs(nil, binlogs)
		segment.Statslogs = mergeFieldBinlogs(nil, statslogs)
		segment.Deltalogs = mergeFieldBinlogs(nil, deltalogs)
		if len(deltalogs) > 0 {
			segment.deltaRowcount.Store(-1)
		}
		segment.Bm25Statslogs = mergeFieldBinlogs(nil, bm25logs)
		modPack.increments[segmentID] = metastore.BinlogsIncrement{
			Segment: segment.SegmentInfo,
		}
		return true
	}
}

// UpdateSegmentColumnGroupsOperator upserts storage-v2 column groups on a
// segment's FieldBinlogs and removes the listed child fields from any other
// pre-existing group whose child_fields contained them, so that every field
// lives in exactly one column group. Idempotent: if a group with the same
// top-level fieldID already exists, it is replaced in place.
//
// The caller must validate up front that:
//   - the segment exists,
//   - its storage_version is 2.
func UpdateSegmentColumnGroupsOperator(segmentID int64, groups map[int64]*datapb.FieldBinlog) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			mlog.Warn(context.TODO(), "meta update: update column groups failed - segment not found",
				mlog.Int64("segmentID", segmentID))
			return false
		}

		incomingChildFields := typeutil.NewSet[int64]()
		for _, g := range groups {
			incomingChildFields.Insert(g.GetChildFields()...)
		}

		// Strip incoming child fields from any other existing group, then drop
		// in-place groups that the request is replacing. Also drop groups that
		// become empty (all ChildFields claimed by incoming groups) and record
		// their FieldIDs so the catalog removes the orphan etcd KV -- otherwise
		// listBinlogs' prefix scan will resurrect the zombie on restart.
		var droppedFieldIDs []int64
		kept := segment.Binlogs[:0]
		for _, existing := range segment.Binlogs {
			if _, replaced := groups[existing.GetFieldID()]; replaced {
				continue
			}
			if len(existing.GetChildFields()) > 0 {
				existing.ChildFields = lo.Filter(existing.GetChildFields(), func(fid int64, _ int) bool {
					return !incomingChildFields.Contain(fid)
				})
				if len(existing.ChildFields) == 0 {
					droppedFieldIDs = append(droppedFieldIDs, existing.GetFieldID())
					continue
				}
			}
			kept = append(kept, existing)
		}
		segment.Binlogs = kept

		for _, g := range groups {
			segment.Binlogs = append(segment.Binlogs, g)
		}

		// Bump DataVersion so querynodes with the segment already loaded will Reopen;
		// ManifestPath is intentionally not moved here (see segment_checker.isSegmentUpdate).
		segment.DataVersion++

		// Backfill column-group commit only mutates segment.Binlogs; skipping
		// Deltalogs / Statslogs / Bm25Statslogs avoids rewriting their KVs on
		// every call and the write amplification that comes with it.
		modPack.increments[segmentID] = metastore.BinlogsIncrement{
			Segment: segment.SegmentInfo,
			UpdateMask: metastore.BinlogsUpdateMask{
				WithoutDeltalogs:     true,
				WithoutStatslogs:     true,
				WithoutBm25Statslogs: true,
			},
			DroppedBinlogFieldIDs: droppedFieldIDs,
		}
		return true
	}
}

// update startPosition
func UpdateStartPosition(startPositions []*datapb.SegmentStartPosition) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		for _, pos := range startPositions {
			if len(pos.GetStartPosition().GetMsgID()) == 0 {
				continue
			}
			s := modPack.Get(pos.GetSegmentID())
			if s == nil {
				continue
			}

			s.StartPosition = pos.GetStartPosition()
		}
		return true
	}
}

func UpdateDmlPosition(segmentID int64, dmlPosition *msgpb.MsgPosition) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		if len(dmlPosition.GetMsgID()) == 0 {
			mlog.Warn(context.TODO(), "meta update: update dml position failed - nil position msg id",
				mlog.Int64("segmentID", segmentID))
			return false
		}

		segment := modPack.Get(segmentID)
		if segment == nil {
			mlog.Warn(context.TODO(), "meta update: update dml position failed - segment not found",
				mlog.Int64("segmentID", segmentID))
			return false
		}

		segment.DmlPosition = dmlPosition
		return true
	}
}

// UpdateCheckPointOperator updates segment checkpoint and num rows
func UpdateCheckPointOperator(segmentID int64, checkpoints []*datapb.CheckPoint, skipDmlPositionCheck ...bool) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			mlog.Warn(context.TODO(), "meta update: update checkpoint failed - segment not found",
				mlog.Int64("segmentID", segmentID))
			return false
		}

		var cpNumRows int64

		// Set segment dml position
		for _, cp := range checkpoints {
			if cp.SegmentID != segmentID {
				// Don't think this is gonna to happen, ignore for now.
				mlog.Warn(context.TODO(), "checkpoint in segment is not same as flush segment to update, igreo", mlog.Int64("current", segmentID), mlog.Int64("checkpoint segment", cp.SegmentID))
				continue
			}

			if cp.GetPosition() == nil {
				mlog.Warn(context.TODO(), "checkpoint has nil position, skip", mlog.Int64("segmentID", segmentID))
				continue
			}

			if segment.DmlPosition != nil && segment.DmlPosition.Timestamp >= cp.Position.Timestamp && (len(skipDmlPositionCheck) == 0 || !skipDmlPositionCheck[0]) {
				mlog.Warn(context.TODO(), "checkpoint in segment is larger than reported", mlog.Any("current", segment.GetDmlPosition()), mlog.Any("reported", cp.GetPosition()))
				// segment position in etcd is larger than checkpoint, then dont change it
				continue
			}

			cpNumRows = cp.NumOfRows
			segment.DmlPosition = cp.GetPosition()
		}

		// update segments num rows
		count := segmentutil.CalcRowCountFromBinLog(segment.SegmentInfo)
		if count > 0 {
			if cpNumRows != count {
				mlog.Info(context.TODO(), "check point reported row count inconsistent with binlog row count",
					mlog.Int64("segmentID", segmentID),
					mlog.Int64("binlog reported (wrong)", cpNumRows),
					mlog.Int64("segment binlog row count (correct)", count))
			}
			segment.NumOfRows = count
		} else if cpNumRows > 0 {
			// V3 storage: binlogs are empty, use checkpoint's NumOfRows
			segment.NumOfRows = cpNumRows
		}

		return true
	}
}

func UpdateManifest(segmentID int64, manifestPath string) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			mlog.Warn(context.TODO(), "meta update: update manifest failed - segment not found",
				mlog.Int64("segmentID", segmentID))
			return false
		}
		// skip empty manifest update and same manifest
		if manifestPath == "" || segment.ManifestPath == manifestPath {
			return false
		}
		segment.ManifestPath = manifestPath
		return true
	}
}

func UpdateManifestVersion(segmentID int64, manifestVersion int64) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			mlog.Warn(context.TODO(), "meta update: update manifest version failed - segment not found",
				mlog.Int64("segmentID", segmentID))
			return false
		}
		if segment.ManifestPath == "" {
			mlog.Warn(context.TODO(), "meta update: update manifest version failed - no manifest path",
				mlog.Int64("segmentID", segmentID))
			return false
		}
		basePath, currentVer, err := packed.UnmarshalManifestPath(segment.ManifestPath)
		if err != nil {
			mlog.Warn(context.TODO(), "meta update: update manifest version failed - unmarshal error",
				mlog.Int64("segmentID", segmentID), mlog.Err(err))
			return false
		}
		// Guard against version rollback. classifyBackfillSegments pre-checks
		// monotonicity at broadcast time, but a concurrent compaction may
		// advance ManifestPath between pre-check and this apply (compaction
		// commits use a different serialization path than this broadcaster).
		// Only accept strictly forward motion; equality is a no-op.
		if currentVer >= manifestVersion {
			if currentVer > manifestVersion {
				mlog.Warn(context.TODO(), "meta update: update manifest version rejected - would regress",
					mlog.Int64("segmentID", segmentID),
					mlog.Int64("currentVer", currentVer),
					mlog.Int64("incomingVer", manifestVersion))
			}
			return false
		}
		segment.ManifestPath = packed.MarshalManifestPath(basePath, manifestVersion)
		return true
	}
}

func UpdateImportedRows(segmentID int64, rows int64) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			mlog.Warn(context.TODO(), "meta update: update NumOfRows failed - segment not found",
				mlog.Int64("segmentID", segmentID))
			return false
		}
		segment.NumOfRows = rows
		segment.MaxRowNum = rows
		return true
	}
}

// ResetImportingSegmentRows clears NumOfRows and MaxRowNum on each given
// segment that is still in the Importing state. Used to discard partial
// progress reported by a failed import attempt before the task is rescheduled,
// so segments the retried attempt skips do not keep stale row counts that
// would break sort compaction.
func ResetImportingSegmentRows(segmentIDs ...int64) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		anyReset := false
		for _, segmentID := range segmentIDs {
			segment := modPack.Get(segmentID)
			if segment == nil {
				mlog.Warn(context.TODO(), "meta update: reset importing segment rows failed - segment not found",
					mlog.Int64("segmentID", segmentID))
				continue
			}
			if segment.GetState() != commonpb.SegmentState_Importing {
				mlog.Warn(context.TODO(), "meta update: reset importing segment rows skipped - segment not in Importing state",
					mlog.Int64("segmentID", segmentID),
					mlog.String("state", segment.GetState().String()))
				continue
			}
			segment.NumOfRows = 0
			segment.MaxRowNum = 0
			anyReset = true
		}
		return anyReset
	}
}

func UpdateIsImporting(segmentID int64, isImporting bool) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			mlog.Warn(context.TODO(), "meta update: update isImporting failed - segment not found",
				mlog.Int64("segmentID", segmentID))
			return false
		}
		segment.IsImporting = isImporting
		return true
	}
}

// UpdateCommitTimestamp sets the commit_timestamp on an import/CDC segment.
// Non-zero marks it as committed at that transaction time, overriding
// start_position.Timestamp for all temporal decisions.
//
// Invariant: a non-zero commit_timestamp MUST be >= max(binlog.TimestampTo)
// across all binlogs on the segment. Row timestamps cannot exceed the commit
// time logically (the data did not "exist" until commit). Violating inputs
// (e.g., CDC where source-cluster TSO > target-cluster TSO) are rejected at
// this entry point rather than letting C++ segcore silently lower row
// timestamps during the load-time overwrite. ts == 0 is the reset path
// (used by compaction completion) and always passes.
func UpdateCommitTimestamp(segmentID int64, ts uint64) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			mlog.Warn(context.TODO(), "meta update: update commit timestamp failed - segment not found",
				mlog.Int64("segmentID", segmentID))
			return false
		}
		if ts != 0 {
			var maxTsTo uint64
			for _, fieldBinlogs := range segment.GetBinlogs() {
				for _, l := range fieldBinlogs.GetBinlogs() {
					if l.GetTimestampTo() > maxTsTo {
						maxTsTo = l.GetTimestampTo()
					}
				}
			}
			if ts < maxTsTo {
				mlog.Error(context.TODO(), "meta update: update commit timestamp rejected - commit_ts < max(binlog.TimestampTo)",
					mlog.Int64("segmentID", segmentID),
					mlog.Uint64("commitTs", ts),
					mlog.Uint64("maxBinlogTimestampTo", maxTsTo))
				return false
			}
		}
		segment.CommitTimestamp = ts
		return true
	}
}

// UpdateImportSegmentPosition updates the segment's StartPosition and DmlPosition
// for import segments using actual timestamps from the imported data.
// Unlike UpdateStartPosition/UpdateDmlPosition, this operator allows nil MsgID
// since import segments don't have message queue positions.
func UpdateImportSegmentPosition(segmentID int64, minTs, maxTs uint64) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			mlog.Warn(context.TODO(), "meta update: update import segment position failed - segment not found",
				mlog.Int64("segmentID", segmentID))
			return false
		}
		channelName := segment.GetInsertChannel()
		// Use actual min timestamp for StartPosition
		segment.StartPosition = &msgpb.MsgPosition{
			ChannelName: channelName,
			MsgID:       nil,
			Timestamp:   minTs,
		}
		// Use actual max timestamp for DmlPosition
		segment.DmlPosition = &msgpb.MsgPosition{
			ChannelName: channelName,
			MsgID:       nil,
			Timestamp:   maxTs,
		}
		return true
	}
}

// UpdateAsDroppedIfEmptyWhenFlushing updates segment state to Dropped if segment is empty and in Flushing state
// It's used to make a empty flushing segment to be dropped directly.
func UpdateAsDroppedIfEmptyWhenFlushing(segmentID int64) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			mlog.Warn(context.TODO(), "meta update: update as dropped if empty when flusing failed - segment not found",
				mlog.Int64("segmentID", segmentID))
			return false
		}
		if segment.Level != datapb.SegmentLevel_L0 && segment.GetNumOfRows() == 0 && (segment.GetState() == commonpb.SegmentState_Flushing || segment.GetState() == commonpb.SegmentState_Flushed) {
			mlog.Info(context.TODO(), "meta update: update as dropped if empty when flusing", mlog.Int64("segmentID", segmentID))
			updateSegStateAndPrepareMetrics(segment, commonpb.SegmentState_Dropped, modPack.metricMutation)
		}
		return true
	}
}

// updateSegmentsInfo update segment infos
// will exec all operators, and update all changed segments
func (m *meta) UpdateSegmentsInfo(ctx context.Context, operators ...UpdateOperator) error {
	m.segMu.Lock()
	defer m.segMu.Unlock()
	updatePack := &updateSegmentPack{
		meta:       m,
		segments:   make(map[int64]*SegmentInfo),
		increments: make(map[int64]metastore.BinlogsIncrement),
		metricMutation: &segMetricMutation{
			stateChange:             make(segmentMetricStateChange),
			deferSegmentLabelChange: true,
		},
	}

	for _, operator := range operators {
		operator(updatePack)
		if updatePack.err != nil {
			return updatePack.err
		}
	}
	if err := commitL0ManifestUpdates(updatePack.l0ManifestUpdates); err != nil {
		return err
	}
	for _, update := range updatePack.l0ManifestUpdates {
		if !update.apply(updatePack) {
			return updatePack.err
		}
	}

	// skip if all segment not exist
	if len(updatePack.segments) == 0 {
		return nil
	}

	// Validate the update pack.
	if err := updatePack.Validate(); err != nil {
		// A stale save-binlog-paths update (segment already flushed, or an
		// outdated time tick) is a benign no-op: skip the meta write and
		// report success so the caller does not retry. The signal stays
		// inside this package on purpose; see errIgnoredSegmentMetaOperation.
		if errors.Is(err, errIgnoredSegmentMetaOperation) {
			mlog.Info(ctx, "meta update: ignored stale segment meta operation", mlog.Err(err))
			return nil
		}
		return err
	}
	updatePack.prepareSegmentMetricUpdates()

	segments := lo.MapToSlice(updatePack.segments, func(_ int64, segment *SegmentInfo) *datapb.SegmentInfo { return segment.SegmentInfo })
	increments := lo.Values(updatePack.increments)

	if err := m.catalog.AlterSegments(ctx, segments, increments...); err != nil {
		mlog.Error(ctx, "meta update: update flush segments info - failed to store flush segment info into Etcd",
			mlog.Err(err))
		return err
	}
	// Apply metric mutation after a successful meta update.
	updatePack.metricMutation.commit()
	// update memory status
	for id, s := range updatePack.segments {
		m.segments.SetSegment(id, s)
	}
	mlog.Info(ctx, "meta update: update flush segments info - update flush segments info successfully")
	return nil
}

// UpdateDropChannelSegmentInfo updates segment checkpoints and binlogs before drop
// reusing segment info to pass segment id, binlogs, statslog, deltalog, start position and checkpoint
func (m *meta) UpdateDropChannelSegmentInfo(ctx context.Context, channel string, segments []*SegmentInfo) error {
	mlog.Debug(context.TODO(), "meta update: update drop channel segment info",
		mlog.String("channel", channel))
	m.segMu.Lock()
	defer m.segMu.Unlock()

	// Prepare segment metric mutation.
	metricMutation := &segMetricMutation{
		stateChange: make(segmentMetricStateChange),
	}
	modSegments := make(map[UniqueID]*SegmentInfo)
	// save new segments flushed from buffer data
	for _, seg2Drop := range segments {
		var segment *SegmentInfo
		segment, metricMutation = m.mergeDropSegment(seg2Drop)
		if segment != nil {
			modSegments[seg2Drop.GetID()] = segment
		}
	}
	// set existed segments of channel to Dropped
	for _, seg := range m.segments.segments {
		if seg.InsertChannel != channel {
			continue
		}
		_, ok := modSegments[seg.ID]
		// seg inf mod segments are all in dropped state
		if !ok {
			clonedSeg := seg.Clone()
			updateSegStateAndPrepareMetrics(clonedSeg, commonpb.SegmentState_Dropped, metricMutation)
			modSegments[seg.ID] = clonedSeg
		}
	}
	err := m.batchSaveDropSegments(ctx, channel, modSegments)
	if err != nil {
		mlog.Warn(context.TODO(), "meta update: update drop channel segment info failed",
			mlog.String("channel", channel),
			mlog.Err(err))
	} else {
		mlog.Info(context.TODO(), "meta update: update drop channel segment info - complete",
			mlog.String("channel", channel))
		// Apply segment metric mutation on successful meta update.
		metricMutation.commit()
	}
	return err
}

// mergeDropSegment merges drop segment information with meta segments
func (m *meta) mergeDropSegment(seg2Drop *SegmentInfo) (*SegmentInfo, *segMetricMutation) {
	metricMutation := &segMetricMutation{
		stateChange: make(segmentMetricStateChange),
	}

	segment := m.segments.GetSegment(seg2Drop.ID)
	// healthy check makes sure the Idempotence
	if segment == nil || !isSegmentHealthy(segment) {
		mlog.Warn(context.TODO(), "UpdateDropChannel skipping nil or unhealthy", mlog.Bool("is nil", segment == nil),
			mlog.Bool("isHealthy", isSegmentHealthy(segment)))
		return nil, metricMutation
	}

	clonedSegment := segment.Clone()
	updateSegStateAndPrepareMetrics(clonedSegment, commonpb.SegmentState_Dropped, metricMutation)

	currBinlogs := clonedSegment.GetBinlogs()

	getFieldBinlogs := func(id UniqueID, binlogs []*datapb.FieldBinlog) *datapb.FieldBinlog {
		for _, binlog := range binlogs {
			if id == binlog.GetFieldID() {
				return binlog
			}
		}
		return nil
	}
	// binlogs
	for _, tBinlogs := range seg2Drop.GetBinlogs() {
		fieldBinlogs := getFieldBinlogs(tBinlogs.GetFieldID(), currBinlogs)
		if fieldBinlogs == nil {
			currBinlogs = append(currBinlogs, tBinlogs)
		} else {
			fieldBinlogs.Binlogs = append(fieldBinlogs.Binlogs, tBinlogs.Binlogs...)
		}
	}
	clonedSegment.Binlogs = currBinlogs
	// statlogs
	currStatsLogs := clonedSegment.GetStatslogs()
	for _, tStatsLogs := range seg2Drop.GetStatslogs() {
		fieldStatsLog := getFieldBinlogs(tStatsLogs.GetFieldID(), currStatsLogs)
		if fieldStatsLog == nil {
			currStatsLogs = append(currStatsLogs, tStatsLogs)
		} else {
			fieldStatsLog.Binlogs = append(fieldStatsLog.Binlogs, tStatsLogs.Binlogs...)
		}
	}
	clonedSegment.Statslogs = currStatsLogs
	// deltalogs
	clonedSegment.Deltalogs = append(clonedSegment.Deltalogs, seg2Drop.GetDeltalogs()...)

	// start position
	if seg2Drop.GetStartPosition() != nil {
		clonedSegment.StartPosition = seg2Drop.GetStartPosition()
	}
	// checkpoint
	if seg2Drop.GetDmlPosition() != nil {
		clonedSegment.DmlPosition = seg2Drop.GetDmlPosition()
	}
	clonedSegment.NumOfRows = seg2Drop.GetNumOfRows()
	return clonedSegment, metricMutation
}

// batchSaveDropSegments saves drop segments info with channel removal flag
// since the channel unwatching operation is not atomic here
// ** the removal flag is always with last batch
// ** the last batch must contains at least one segment
//  1. when failure occurs between batches, failover mechanism will continue with the earliest  checkpoint of this channel
//     since the flag is not marked so DataNode can re-consume the drop collection msg
//  2. when failure occurs between save meta and unwatch channel, the removal flag shall be check before let datanode watch this channel
func (m *meta) batchSaveDropSegments(ctx context.Context, channel string, modSegments map[int64]*SegmentInfo) error {
	var modSegIDs []int64
	for k := range modSegments {
		modSegIDs = append(modSegIDs, k)
	}
	mlog.Info(ctx, "meta update: batch save drop segments",
		mlog.Int64s("drop segments", modSegIDs))
	segments := make([]*datapb.SegmentInfo, 0)
	for _, seg := range modSegments {
		segments = append(segments, seg.SegmentInfo)
	}
	err := m.catalog.SaveDroppedSegmentsInBatch(ctx, segments)
	if err != nil {
		return err
	}

	if err = m.catalog.MarkChannelDeleted(ctx, channel); err != nil {
		return err
	}

	// update memory info
	for id, segment := range modSegments {
		m.segments.SetSegment(id, segment)
	}

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
	m.segMu.RLock()
	defer m.segMu.RUnlock()
	return m.segments.GetSegmentsBySelector(filters...)
}

func (m *meta) GetRealSegmentsForChannel(channel string) []*SegmentInfo {
	m.segMu.RLock()
	defer m.segMu.RUnlock()
	return m.segments.GetRealSegmentsForChannel(channel)
}

// AddAllocation add allocation in segment
func (m *meta) AddAllocation(segmentID UniqueID, allocation *Allocation) error {
	mlog.Debug(m.ctx, "meta update: add allocation",
		mlog.Int64("segmentID", segmentID),
		mlog.Any("allocation", allocation))
	m.segMu.Lock()
	defer m.segMu.Unlock()
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
	m.segMu.Lock()
	defer m.segMu.Unlock()
	m.segments.SetRowCount(segmentID, rowCount)
}

// SetAllocations set Segment allocations, will overwrite ALL original allocations
// Note that allocations is not persisted in KV store
func (m *meta) SetAllocations(segmentID UniqueID, allocations []*Allocation) {
	m.segMu.Lock()
	defer m.segMu.Unlock()
	m.segments.SetAllocations(segmentID, allocations)
}

// SetLastExpire set lastExpire time for segment
// Note that last is not necessary to store in KV meta
func (m *meta) SetLastExpire(segmentID UniqueID, lastExpire uint64) {
	m.segMu.Lock()
	defer m.segMu.Unlock()
	clonedSegment := m.segments.GetSegment(segmentID).Clone()
	clonedSegment.LastExpireTime = lastExpire
	m.segments.SetSegment(segmentID, clonedSegment)
}

// SetLastFlushTime set LastFlushTime for segment with provided `segmentID`
// Note that lastFlushTime is not persisted in KV store
func (m *meta) SetLastFlushTime(segmentID UniqueID, t time.Time) {
	m.segMu.Lock()
	defer m.segMu.Unlock()
	m.segments.SetFlushTime(segmentID, t)
}

// SetLastWrittenTime set LastWrittenTime for segment with provided `segmentID`
// Note that lastWrittenTime is not persisted in KV store
func (m *meta) SetLastWrittenTime(segmentID UniqueID) {
	m.segMu.Lock()
	defer m.segMu.Unlock()
	m.segments.SetLastWrittenTime(segmentID)
}

// SetSegmentCompacting sets compaction state for segment
func (m *meta) SetSegmentCompacting(segmentID UniqueID, compacting bool) {
	m.segMu.Lock()
	defer m.segMu.Unlock()

	m.segments.SetIsCompacting(segmentID, compacting)
}

// IsSegmentCompacting check if segment is compacting
func (m *meta) IsSegmentCompacting(segmentID UniqueID) bool {
	m.segMu.RLock()
	defer m.segMu.RUnlock()
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
	m.segMu.Lock()
	defer m.segMu.Unlock()
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
	m.segMu.Lock()
	defer m.segMu.Unlock()
	for _, segmentID := range segmentIDs {
		m.segments.SetIsCompacting(segmentID, compacting)
	}
}

// SetSegmentLevel sets level for segment
func (m *meta) SetSegmentLevel(segmentID UniqueID, level datapb.SegmentLevel) {
	m.segMu.Lock()
	defer m.segMu.Unlock()

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

func (m *meta) completeClusterCompactionMutation(t *datapb.CompactionTask, result *datapb.CompactionPlanResult) ([]*SegmentInfo, *segMetricMutation, error) {

	metricMutation := &segMetricMutation{stateChange: make(segmentMetricStateChange)}
	compactFromSegIDs := make([]int64, 0)
	compactToSegIDs := make([]int64, 0)
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
		compactToSegIDs = append(compactToSegIDs, segment.GetID())
		metricMutation.addNewSeg(segment.GetState(), segment.GetLevel(), segment.GetIsSorted(), segment.GetStorageVersion(), segmentMetricFormatLabel(segment), segment.GetNumOfRows())
	}

	mlog.Debug(context.TODO(), "meta update: prepare for meta mutation - complete")

	compactToInfos := lo.Map(compactToSegInfos, func(info *SegmentInfo, _ int) *datapb.SegmentInfo {
		return info.SegmentInfo
	})

	binlogs := make([]metastore.BinlogsIncrement, 0)
	for _, seg := range compactToInfos {
		binlogs = append(binlogs, metastore.BinlogsIncrement{Segment: seg})
	}
	// only add new segments
	if err := m.catalog.AlterSegments(m.ctx, compactToInfos, binlogs...); err != nil {
		mlog.Warn(context.TODO(), "fail to alter compactTo segments", mlog.Err(err))
		return nil, nil, err
	}
	lo.ForEach(compactToSegInfos, func(info *SegmentInfo, _ int) {
		m.segments.SetSegment(info.GetID(), info)
	})
	mlog.Info(context.TODO(), "meta update: alter in memory meta after compaction - complete")
	return compactToSegInfos, metricMutation, nil
}

func (m *meta) completeMixCompactionMutation(
	t *datapb.CompactionTask,
	result *datapb.CompactionPlanResult,
) ([]*SegmentInfo, *segMetricMutation, error) {

	metricMutation := &segMetricMutation{stateChange: make(segmentMetricStateChange)}
	var compactFromSegIDs []int64
	var compactFromSegInfos []*SegmentInfo
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
		cloned.DroppedAt = uint64(time.Now().UnixNano())
		cloned.Compacted = true

		compactFromSegInfos = append(compactFromSegInfos, cloned)
		compactFromSegIDs = append(compactFromSegIDs, cloned.GetID())

		// metrics mutation for compaction from segments
		updateSegStateAndPrepareMetrics(cloned, commonpb.SegmentState_Dropped, metricMutation)

		mlog.Info(context.TODO(), "compact from segment",
			mlog.Int64("segmentID", cloned.GetID()),
			mlog.Int64("segment size", cloned.getSegmentSize()),
			mlog.Int64("num rows", cloned.GetNumOfRows()),
		)
	}

	if t.GetSchema() == nil {
		return nil, nil, merr.WrapErrIllegalCompactionPlan("mix compaction task schema is nil")
	}
	outputSchemaVersion := t.GetSchema().GetVersion()

	fallbackStart, fallbackDml := getCompactionFallbackPositions(compactFromSegInfos)

	compactToSegments := make([]*SegmentInfo, 0)
	for _, compactToSegment := range result.GetSegments() {
		startPos, dmlPos := recalculateSegmentPosition(compactToSegment.GetInsertLogs(), t.GetChannel(), fallbackStart, fallbackDml)
		compactToSegmentInfo := NewSegmentInfo(
			&datapb.SegmentInfo{
				ID:            compactToSegment.GetSegmentID(),
				CollectionID:  compactFromSegInfos[0].CollectionID,
				PartitionID:   compactFromSegInfos[0].PartitionID,
				InsertChannel: t.GetChannel(),
				NumOfRows:     compactToSegment.NumOfRows,
				State:         commonpb.SegmentState_Flushed,
				MaxRowNum:     compactFromSegInfos[0].MaxRowNum,
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
				SchemaVersion:       outputSchemaVersion,
				CommitTimestamp:     0, // Normalized: row timestamps already rewritten
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

	mlog.Debug(context.TODO(), "meta update: prepare for meta mutation - complete")
	compactFromInfos := lo.Map(compactFromSegInfos, func(info *SegmentInfo, _ int) *datapb.SegmentInfo {
		return info.SegmentInfo
	})

	compactToInfos := lo.Map(compactToSegments, func(info *SegmentInfo, _ int) *datapb.SegmentInfo {
		return info.SegmentInfo
	})

	binlogs := make([]metastore.BinlogsIncrement, 0)
	for _, seg := range compactToInfos {
		binlogs = append(binlogs, metastore.BinlogsIncrement{Segment: seg})
	}

	// alter compactTo before compactFrom segments to avoid data lost if service crash during AlterSegments
	if err := m.catalog.AlterSegments(m.ctx, compactToInfos, binlogs...); err != nil {
		mlog.Warn(context.TODO(), "fail to alter compactTo segments", mlog.Err(err))
		return nil, nil, err
	}
	if err := m.catalog.AlterSegments(m.ctx, compactFromInfos); err != nil {
		mlog.Warn(context.TODO(), "fail to alter compactFrom segments", mlog.Err(err))
		return nil, nil, err
	}
	lo.ForEach(compactFromSegInfos, func(info *SegmentInfo, _ int) {
		m.segments.SetSegment(info.GetID(), info)
	})
	lo.ForEach(compactToSegments, func(info *SegmentInfo, _ int) {
		m.segments.SetSegment(info.GetID(), info)
	})

	mlog.Info(context.TODO(), "meta update: alter in memory meta after compaction - complete")
	return compactToSegments, metricMutation, nil
}

func (m *meta) ValidateSegmentStateBeforeCompleteCompactionMutation(t *datapb.CompactionTask) error {
	m.segMu.RLock()
	defer m.segMu.RUnlock()

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
	m.segMu.Lock()
	defer m.segMu.Unlock()
	switch t.GetType() {
	case datapb.CompactionType_MixCompaction:
		return m.completeMixCompactionMutation(t, result)
	case datapb.CompactionType_ClusteringCompaction:
		return m.completeClusterCompactionMutation(t, result)
	case datapb.CompactionType_SortCompaction:
		return m.completeSortCompactionMutation(t, result)
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
	m.segMu.RLock()
	defer m.segMu.RUnlock()

	for _, segID := range segIDs {
		if _, ok := m.segments.segments[segID]; !ok {
			return false, merr.WrapErrServiceInternalMsg("segment is not exist with ID = %d", segID)
		}
	}
	return true, nil
}

// GetCompactionTo returns the segment info of the segment to be compacted to.
func (m *meta) GetCompactionTo(segmentID int64) ([]*SegmentInfo, bool) {
	m.segMu.RLock()
	defer m.segMu.RUnlock()

	return m.segments.GetCompactionTo(segmentID)
}

// GetMinGrowingSegmentCheckpoint returns the minimum DmlPosition of all growing
// segments on the given channel that belong to TEXT collections.
// This is used to prevent the channel checkpoint from advancing beyond unflushed
// growing segment data (critical for TEXT collections where QueryNode manages
// insert flush independently).
//
// Only TEXT collections need clamping because their growing segment data is
// flushed by QueryNode (not StreamNode/DataNode). For normal collections,
// StreamNode owns both the data flush and checkpoint, so clamping would
// incorrectly slow down checkpoint advancement.
//
// Returns nil if no TEXT collection growing segments exist on the channel.
func (m *meta) GetMinGrowingSegmentCheckpoint(channel string) *msgpb.MsgPosition {
	segments := m.SelectSegments(context.TODO(), WithChannel(channel))

	// Cache collection TEXT field check results to avoid repeated schema scans
	// within the same call (many segments may belong to the same collection).
	textCollectionCache := make(map[int64]bool)

	var minPos *msgpb.MsgPosition
	for _, s := range segments {
		// Only consider growing (unflushed) segments at L1 level
		if s.GetState() != commonpb.SegmentState_Growing {
			continue
		}
		if s.GetLevel() == datapb.SegmentLevel_L0 {
			continue
		}

		// Only clamp for TEXT collections — normal collections don't need it.
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
			pos = s.GetStartPosition()
		}
		if pos == nil {
			continue
		}

		if minPos == nil || pos.GetTimestamp() < minPos.GetTimestamp() {
			minPos = pos
		}
	}
	return minPos
}

// collectionHasTextFields returns true if the collection has any TEXT type fields.
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

	// Clamp checkpoint to not advance beyond min growing segment checkpoint.
	// This prevents TEXT collection data loss where StreamNode (L0 delete) checkpoint
	// may advance beyond QueryNode's (L1 insert) unflushed data.
	minGrowingCP := m.GetMinGrowingSegmentCheckpoint(vChannel)
	if minGrowingCP != nil && pos.GetTimestamp() > minGrowingCP.GetTimestamp() {
		mlog.Info(context.TODO(), "clamping channel checkpoint to min growing segment checkpoint",
			mlog.String("vChannel", vChannel),
			mlog.Uint64("requestedTs", pos.GetTimestamp()),
			mlog.Uint64("clampedTs", minGrowingCP.GetTimestamp()))
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

	// Clamp each position to not advance beyond min growing segment checkpoint.
	for i, pos := range positions {
		if pos == nil || pos.GetChannelName() == "" {
			continue
		}
		minGrowingCP := m.GetMinGrowingSegmentCheckpoint(pos.GetChannelName())
		if minGrowingCP != nil && pos.GetTimestamp() > minGrowingCP.GetTimestamp() {
			mlog.Info(context.TODO(), "clamping channel checkpoint to min growing segment checkpoint",
				mlog.String("vChannel", pos.GetChannelName()),
				mlog.Uint64("requestedTs", pos.GetTimestamp()),
				mlog.Uint64("clampedTs", minGrowingCP.GetTimestamp()))
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

// updateSegStateAndPrepareMetrics updates a segment's in-memory state and prepare for the corresponding metric update.
func updateSegStateAndPrepareMetrics(segToUpdate *SegmentInfo, targetState commonpb.SegmentState, metricMutation *segMetricMutation) {
	mlog.Debug(context.TODO(), "updating segment state and updating metrics",
		mlog.Int64("segmentID", segToUpdate.GetID()),
		mlog.String("old state", segToUpdate.GetState().String()),
		mlog.String("new state", targetState.String()),
		mlog.Int64("# of rows", segToUpdate.GetNumOfRows()))
	metricMutation.append(segToUpdate.GetState(), targetState, segToUpdate.GetLevel(), segToUpdate.GetIsSorted(), segToUpdate.GetStorageVersion(), segmentMetricFormatLabel(segToUpdate), segToUpdate.GetNumOfRows())
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
	t *datapb.CompactionTask,
	result *datapb.CompactionPlanResult,
) ([]*SegmentInfo, *segMetricMutation, error) {

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

	cloned := oldSegment.Clone()
	cloned.DroppedAt = uint64(time.Now().UnixNano())
	cloned.Compacted = true

	updateSegStateAndPrepareMetrics(cloned, commonpb.SegmentState_Dropped, metricMutation)

	mlog.Info(context.TODO(), "meta update: prepare for complete stats mutation - complete",
		mlog.Int64("num rows", segment.GetNumOfRows()),
		mlog.Int64("segment size", segment.getSegmentSize()),
		mlog.Int64s("expirQuantiles", segment.GetExpirQuantiles()))
	if err := m.catalog.AlterSegments(m.ctx, []*datapb.SegmentInfo{cloned.SegmentInfo, segment.SegmentInfo}, metastore.BinlogsIncrement{Segment: segment.SegmentInfo}); err != nil {
		mlog.Warn(context.TODO(), "fail to alter segments and new segment", mlog.Err(err))
		return nil, nil, err
	}

	m.segments.SetSegment(oldSegment.GetID(), cloned)
	m.segments.SetSegment(segment.GetID(), segment)
	mlog.Info(context.TODO(), "meta update: alter in memory meta after compaction - complete")
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
	oldSegment := m.segments.GetSegment(segmentID)
	if oldSegment == nil {
		return nil, nil, merr.WrapErrSegmentNotFound(segmentID)
	}

	// Re-validate segment health to prevent race condition with drop collection
	// between ValidateSegmentStateBeforeCompleteCompactionMutation and here
	if !isSegmentHealthy(oldSegment) {
		mlog.Warn(context.TODO(), "input segment was dropped during compaction mutation",
			mlog.Int64("planID", t.GetPlanID()),
			mlog.Int64("segmentID", segmentID),
			mlog.String("state", oldSegment.GetState().String()))
		return nil, nil, merr.WrapErrSegmentNotFound(segmentID, "input segment was dropped")
	}

	resultSegment := result.GetSegments()[0]
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

	// Clone the segment for update
	cloned := oldSegment.Clone()

	cloned.Binlogs = resultSegment.GetInsertLogs()
	if newSchemaVersion > cloned.GetSchemaVersion() {
		cloned.SchemaVersion = newSchemaVersion
		mlog.Info(m.ctx, "meta update: update schema version for schema bump compaction",
			mlog.Int64("segmentID", segmentID),
			mlog.Int32("oldSchemaVersion", oldSegment.GetSchemaVersion()),
			mlog.Int32("newSchemaVersion", newSchemaVersion))
	}

	cloned.StorageVersion = resultSegment.GetStorageVersion()
	cloned.ManifestPath = resultManifest
	if !proto.Equal(oldSegment.SegmentInfo, cloned.SegmentInfo) {
		cloned.DataVersion = oldSegment.GetDataVersion() + 1
	}

	// Prepare binlogs increment for catalog update
	binlogsIncrement := metastore.BinlogsIncrement{
		Segment: cloned.SegmentInfo,
	}

	mlog.Info(m.ctx, "meta update: prepare for complete schema bump compaction mutation - complete",
		mlog.Int64("num rows", cloned.GetNumOfRows()))

	// Save to catalog
	if err := m.catalog.AlterSegments(m.ctx, []*datapb.SegmentInfo{cloned.SegmentInfo}, binlogsIncrement); err != nil {
		mlog.Warn(m.ctx, "fail to alter segment for schema bump compaction", mlog.Err(err))
		return nil, nil, err
	}

	// Update in-memory meta
	m.segments.SetSegment(segmentID, cloned)
	mlog.Info(m.ctx, "meta update: alter in memory meta after schema bump compaction - complete")

	return []*SegmentInfo{cloned}, metricMutation, nil
}

func (m *meta) completeBumpSchemaVersionReplacementMutation(
	metricMutation *segMetricMutation,
	t *datapb.CompactionTask,
	oldSegment *SegmentInfo,
	resultSegment *datapb.CompactionSegment,
	schemaVersion int32,
) ([]*SegmentInfo, *segMetricMutation, error) {
	idRange := t.GetPreAllocatedSegmentIDs()
	if idRange == nil || idRange.GetBegin()+1 != idRange.GetEnd() || resultSegment.GetSegmentID() != idRange.GetBegin() {
		return nil, nil, merr.WrapErrIllegalCompactionPlanMsg("schema bump replacement result segment ID %d does not match the pre-allocated segment ID", resultSegment.GetSegmentID())
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

	m.segments.SetSegment(dropped.GetID(), dropped)
	m.segments.SetSegment(newSegment.GetID(), newSegment)
	mlog.Info(m.ctx, "meta update: alter in memory meta after schema bump full rewrite replacement - complete")
	return []*SegmentInfo{newSegment}, metricMutation, nil
}

func (m *meta) getSegmentsMetrics(collectionID int64) []*metricsinfo.Segment {
	m.segMu.RLock()
	defer m.segMu.RUnlock()

	segments := make([]*metricsinfo.Segment, 0, len(m.segments.segments))
	for _, s := range m.segments.segments {
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
	m.segMu.Lock()
	defer m.segMu.Unlock()

	// Filter out the segments of the partition to be dropped.
	metricMutation := &segMetricMutation{
		stateChange: make(segmentMetricStateChange),
	}
	modSegments := make([]*SegmentInfo, 0)
	segments := make([]*datapb.SegmentInfo, 0)
	// set existed segments of channel to Dropped
	for _, seg := range m.segments.segments {
		if contains(partitionIDs, seg.PartitionID) {
			clonedSeg := seg.Clone()
			updateSegStateAndPrepareMetrics(clonedSeg, commonpb.SegmentState_Dropped, metricMutation)
			modSegments = append(modSegments, clonedSeg)
			segments = append(segments, clonedSeg.SegmentInfo)
		}
	}

	// Save dropped segments in batch into meta.
	err := m.catalog.SaveDroppedSegmentsInBatch(m.ctx, segments)
	if err != nil {
		return err
	}
	// update memory info
	for _, segment := range modSegments {
		m.segments.SetSegment(segment.GetID(), segment)
	}
	metricMutation.commit()
	return nil
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
	m.segMu.Lock()
	defer m.segMu.Unlock()

	segments := m.segments.GetSegmentsBySelector(SegmentFilterFunc(isSegmentHealthy), WithChannel(vChannel))
	segmentsToDrop := make([]*SegmentInfo, 0)
	metricMutation := &segMetricMutation{
		stateChange: make(segmentMetricStateChange),
	}

	for _, segment := range segments {
		if segmentEffectiveDmlTs(segment.SegmentInfo) <= flushTs && segment.GetState() != commonpb.SegmentState_Dropped {
			cloned := segment.Clone()
			updateSegStateAndPrepareMetrics(cloned, commonpb.SegmentState_Dropped, metricMutation)
			segmentsToDrop = append(segmentsToDrop, cloned)
		}
	}

	if len(segmentsToDrop) == 0 {
		return nil
	}

	// Persist to etcd
	segmentsProto := lo.Map(segmentsToDrop, func(seg *SegmentInfo, _ int) *datapb.SegmentInfo {
		return seg.SegmentInfo
	})
	if err := m.catalog.AlterSegments(ctx, segmentsProto); err != nil {
		mlog.Warn(ctx, "Failed to batch set segments state to dropped", mlog.Err(err))
		return err
	}

	// Update metrics
	metricMutation.commit()

	// Update memory
	for _, seg := range segmentsToDrop {
		m.segments.SetSegment(seg.GetID(), seg)
	}

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
