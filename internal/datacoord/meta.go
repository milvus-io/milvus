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
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
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
	UpdateSegmentsInfo(ctx context.Context, mutations map[int64][]MutateFunc, newSegments ...*datapb.SegmentInfo) error
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
	segmentPersist OptimisticTxnPersist[string, *datapb.SegmentInfo]

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

func (m *meta) isCollectionCompactionBlocked(collectionID int64) bool {
	if m.snapshotMeta == nil {
		return false
	}
	return m.snapshotMeta.IsCollectionCompactionBlocked(collectionID)
}

func (m *meta) isSegmentCompactionProtected(segmentID int64) bool {
	if m.snapshotMeta == nil {
		return false
	}
	return m.snapshotMeta.IsSegmentCompactionProtected(segmentID)
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

// A local cache of segment metric update. Must call commit() to take effect.
type segMetricMutation struct {
	stateChange       map[string]map[string]map[string]map[string]int // segment state, seg level -> state -> isSorted -> storageVersion change count (to increase or decrease).
	rowCountChange    int64                                           // Change in # of rows.
	rowCountAccChange int64                                           // Total # of historical added rows, accumulated.
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

func newMeta(ctx context.Context, catalog metastore.DataCoordCatalog, chunkManager storage.ChunkManager, broker broker.Broker, segmentPersist OptimisticTxnPersist[string, *datapb.SegmentInfo], metaRootPaths ...string) (*meta, error) {
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
	metaRootPath := ""
	if len(metaRootPaths) > 0 {
		metaRootPath = metaRootPaths[0]
	}

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
	for i, collectionID := range collectionIDs {
		i := i
		collectionID := collectionID
		futures = append(futures, pool.Submit(func() (any, error) {
			prefix := m.segmentCollectionPrefix(collectionID)
			_, values, versions, err := m.segmentPersist.Scan(m.ctx, prefix)
			if err != nil {
				return nil, err
			}
			collectionResults[i] = scanResult{segments: values, versions: versions}
			return nil, nil
		}))
	}
	if err := conc.AwaitAll(futures...); err != nil {
		return err
	}

	log.Ctx(ctx).Info("datacoord show segments done", zap.Duration("dur", record.RecordSpan()))

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
		if pos.Timestamp != math.MaxUint64 {
			// Should not be set as metric since it's a tombstone value.
			ts, _ := tsoutil.ParseTS(pos.Timestamp)
			metrics.DataCoordCheckpointUnixSeconds.WithLabelValues(paramtable.GetStringNodeID(), vChannel).
				Set(float64(ts.Unix()))
		}
	}

	log.Ctx(ctx).Info("DataCoord meta reloadFromKV done", zap.Int("numSegments", numSegments), zap.Duration("duration", record.ElapseSpan()))
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
	log.Info("meta update: add collection", zap.Int64("collectionID", collection.ID))
	m.collections.Insert(collection.ID, collection)
	metrics.DataCoordNumCollections.WithLabelValues().Set(float64(m.collections.Len()))
	log.Info("meta update: add collection - complete", zap.Int64("collectionID", collection.ID))
}

// DropCollection drop a collection from meta
func (m *meta) DropCollection(collectionID int64) {
	log.Info("meta update: drop collection", zap.Int64("collectionID", collectionID))
	if _, ok := m.collections.GetAndRemove(collectionID); ok {
		metrics.CleanupDataCoordWithCollectionID(collectionID)
		metrics.DataCoordNumCollections.WithLabelValues().Set(float64(m.collections.Len()))
		log.Info("meta update: drop collection - complete", zap.Int64("collectionID", collectionID))
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
	segments := m.SelectSegments(ctx, WithCollection(collectionID), SegmentFilterFunc(func(si *SegmentInfo) bool {
		return isSegmentHealthy(si)
	}))
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
				// } else {
				// log.Ctx(context.TODO()).Warn("not found database name", zap.Int64("collectionID", segment.GetCollectionID()))
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
	txn.Insert(key, segment.SegmentInfo)
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

	metrics.DataCoordNumSegments.WithLabelValues(segment.GetState().String(), segment.GetLevel().String(), getSortStatus(segment.GetIsSorted()), fmt.Sprint(segment.GetStorageVersion())).Inc()
	log.Info("meta update: adding segment - complete", zap.Int64("segmentID", segment.GetID()))
	return nil
}

// DropSegment remove segment, etcd persistence also removed
func (m *meta) DropSegment(ctx context.Context, segment *SegmentInfo) error {
	log := log.Ctx(ctx)
	segmentID := segment.GetID()
	log.Debug("meta update: dropping segment", zap.Int64("segmentID", segmentID))
	key := m.segmentKey(segment.GetCollectionID(), segment.GetPartitionID(), segmentID)
	txn := m.segmentPersist.Txn(ctx)
	txn.Delete(key)
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
	metrics.DataCoordNumSegments.WithLabelValues(segment.GetState().String(), segment.GetLevel().String(), getSortStatus(segment.GetIsSorted()), fmt.Sprint(segment.GetStorageVersion())).Dec()

	m.segments.DropSegment(segmentID, results[0].Version)
	log.Info("meta update: dropping segment - complete",
		zap.Int64("segmentID", segmentID))
	return nil
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
			log.Ctx(context.TODO()).Warn("cannot find segment", zap.Int64("segmentID", segmentID))
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
			return nil, errors.New(fmt.Sprintf("cannot find segment %d", segmentID))
		}
		segChannels[segmentID] = segment.GetInsertChannel()
	}
	return segChannels, nil
}

// SetState setting segment with provided ID state
func (m *meta) SetState(ctx context.Context, segmentID UniqueID, targetState commonpb.SegmentState) error {
	log := log.Ctx(context.TODO())
	log.Debug("meta update: setting segment state",
		zap.Int64("segmentID", segmentID),
		zap.Any("target state", targetState))
	curSegInfo := m.segments.GetSegment(segmentID)
	if curSegInfo == nil {
		return fmt.Errorf("segment is not exist with ID = %d", segmentID)
	}

	key := m.segmentKey(curSegInfo.GetCollectionID(), curSegInfo.GetPartitionID(), curSegInfo.GetID())
	txn := m.segmentPersist.Txn(ctx)
	txn.Update(key, func(existing *datapb.SegmentInfo) (*datapb.SegmentInfo, bool) {
		existing.State = targetState
		if targetState == commonpb.SegmentState_Dropped {
			existing.DroppedAt = uint64(time.Now().UnixNano())
		}
		return existing, true
	})
	results, err := txn.Commit()
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) && targetState == commonpb.SegmentState_Dropped {
			return nil
		}
		log.Warn("meta update: setting segment state - failed to alter segments",
			zap.Int64("segmentID", segmentID),
			zap.String("target state", targetState.String()),
			zap.Error(err))
		return err
	}
	updatedSeg := NewSegmentInfo(results[0].Value)
	old, existed := m.segments.SetSegment(segmentID, updatedSeg, results[0].Version)
	if existed && old.GetState() != updatedSeg.GetState() {
		metricMutation := segMetricMutation{stateChange: make(map[string]map[string]map[string]map[string]int)}
		metricMutation.append(old.GetState(), updatedSeg.GetState(), updatedSeg.GetLevel(), updatedSeg.GetIsSorted(), updatedSeg.GetStorageVersion(), updatedSeg.GetNumOfRows())
		metricMutation.commit()
	}
	log.Info("meta update: setting segment state - complete",
		zap.Int64("segmentID", segmentID),
		zap.String("target state", targetState.String()))
	return nil
}

func (m *meta) UpdateSegment(segmentID int64, operators ...SegmentOperator) error {
	log := log.Ctx(context.TODO())
	// Need cache to construct key (collection/partition IDs are immutable).
	info := m.segments.GetSegment(segmentID)
	if info == nil {
		log.Warn("meta update: UpdateSegment - segment not found",
			zap.Int64("segmentID", segmentID))
		return merr.WrapErrSegmentNotFound(segmentID)
	}

	key := m.segmentKey(info.GetCollectionID(), info.GetPartitionID(), info.GetID())
	txn := m.segmentPersist.Txn(m.ctx)
	txn.Update(key, func(existing *datapb.SegmentInfo) (*datapb.SegmentInfo, bool) {
		seg := NewSegmentInfo(existing)
		updated := false
		for _, operator := range operators {
			if operator(seg) {
				updated = true
			}
		}
		return seg.SegmentInfo, updated
	})
	results, err := txn.Commit()
	if err != nil {
		log.Warn("meta update: update segment - failed to alter segments",
			zap.Int64("segmentID", segmentID),
			zap.Error(err))
		return err
	}
	// Update in-memory meta.
	m.segments.SetSegment(segmentID, NewSegmentInfo(results[0].Value), results[0].Version)

	log.Info("meta update: update segment - complete",
		zap.Int64("segmentID", segmentID))
	return nil
}

// MutateFunc modifies a *datapb.SegmentInfo in place.
// Returns true to proceed with the write, false to skip this segment's update.
// Runs inside UpdateFunc against the persisted value for CAS correctness.
type MutateFunc func(seg *datapb.SegmentInfo) bool

// UpdateSegmentsInfo atomically persists mutations to existing segments
// and inserts newSegments. Each segment can have multiple MutateFuncs
// composed in order. If any MutateFunc returns false, that segment's
// update is skipped. All MutateFuncs run inside UpdateFunc against
// the persist value for CAS correctness.
func (m *meta) UpdateSegmentsInfo(ctx context.Context, mutations map[int64][]MutateFunc, newSegments ...*datapb.SegmentInfo) error {
	if len(mutations) == 0 && len(newSegments) == 0 {
		return nil
	}

	start := time.Now()

	txn := m.segmentPersist.Txn(ctx)

	type entry struct {
		segID    int64
		isInsert bool
		newSeg   *SegmentInfo // only for inserts
	}
	var entries []entry

	// Existing segments: run MutateFuncs inside UpdateFunc for CAS
	// Track max UpdateFunc duration (called inside txn.Commit, possibly during retries)
	var maxUpdateFuncNs atomic.Int64
	for segID, fns := range mutations {
		cached := m.segments.GetSegment(segID)
		if cached == nil {
			log.Ctx(ctx).Warn("meta update: segment not found, skipping",
				zap.Int64("segmentID", segID))
			continue
		}
		key := m.segmentKey(cached.GetCollectionID(), cached.GetPartitionID(), segID)
		funcs := fns // capture
		txn.Update(key, func(existing *datapb.SegmentInfo) (*datapb.SegmentInfo, bool) {
			fnStart := time.Now()
			for _, fn := range funcs {
				if !fn(existing) {
					return existing, false
				}
			}
			dur := time.Since(fnStart).Nanoseconds()
			for {
				cur := maxUpdateFuncNs.Load()
				if dur <= cur || maxUpdateFuncNs.CompareAndSwap(cur, dur) {
					break
				}
			}
			return existing, true
		})
		entries = append(entries, entry{segID: segID})
	}

	buildMutationsDur := time.Since(start)

	// New segments: insert directly
	for _, seg := range newSegments {
		key := m.segmentKey(seg.GetCollectionID(), seg.GetPartitionID(), seg.GetID())
		info := NewSegmentInfo(seg)
		txn.Insert(key, seg)
		entries = append(entries, entry{segID: seg.GetID(), isInsert: true, newSeg: info})
	}

	if len(entries) == 0 {
		return nil
	}

	buildInsertsDur := time.Since(start) - buildMutationsDur

	// Persist to etcd/tikv
	commitStart := time.Now()
	results, err := txn.Commit()
	commitDur := time.Since(commitStart)
	if err != nil {
		log.Ctx(ctx).Error("meta update: failed to persist segments", zap.Error(err))
		return err
	}

	// Post-persist: update cache + compute metrics
	cacheStart := time.Now()
	metricMutation := &segMetricMutation{
		stateChange: make(map[string]map[string]map[string]map[string]int),
	}
	for i, e := range entries {
		if e.isInsert {
			m.segments.SetSegment(e.segID, e.newSeg, results[i].Version)
			metricMutation.addNewSeg(e.newSeg.GetState(), e.newSeg.GetLevel(), e.newSeg.GetIsSorted(), e.newSeg.GetStorageVersion(), e.newSeg.GetNumOfRows())
		} else {
			newSeg := NewSegmentInfo(results[i].Value)
			oldSeg, existed := m.segments.SetSegment(e.segID, newSeg, results[i].Version)
			if existed && oldSeg.GetState() != newSeg.GetState() {
				metricMutation.append(oldSeg.GetState(), newSeg.GetState(), newSeg.GetLevel(), newSeg.GetIsSorted(), newSeg.GetStorageVersion(), newSeg.GetNumOfRows())
			}
		}
	}
	metricMutation.commit()
	cacheDur := time.Since(cacheStart)

	totalDur := time.Since(start)
	if totalDur > 40*time.Millisecond {
		log.Ctx(ctx).Info("UpdateSegmentsInfo slow",
			zap.Duration("total", totalDur),
			zap.Duration("buildMutations", buildMutationsDur),
			zap.Duration("buildInserts", buildInsertsDur),
			zap.Duration("txnCommit", commitDur),
			zap.Duration("maxUpdateFunc", time.Duration(maxUpdateFuncNs.Load())),
			zap.Duration("updateCache", cacheDur),
			zap.Int("numMutations", len(mutations)),
			zap.Int("numNewSegments", len(newSegments)),
			zap.Int("numEntries", len(entries)))
	}

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

	// Build txn with proper UpdateFunc per segment
	txn := m.segmentPersist.Txn(ctx)
	for _, ref := range segRefs {
		dropData, hasMerge := seg2DropMap[ref.id]
		if hasMerge {
			// Merge segments: apply drop data into persist value inside UpdateFunc
			mergeData := dropData
			txn.Update(ref.key, func(existing *datapb.SegmentInfo) (*datapb.SegmentInfo, bool) {
				existing.State = commonpb.SegmentState_Dropped
				existing.DroppedAt = uint64(time.Now().UnixNano())

				// Merge binlogs
				getFieldBinlogs := func(id UniqueID, binlogs []*datapb.FieldBinlog) *datapb.FieldBinlog {
					for _, binlog := range binlogs {
						if id == binlog.GetFieldID() {
							return binlog
						}
					}
					return nil
				}
				currBinlogs := existing.GetBinlogs()
				for _, tBinlogs := range mergeData.GetBinlogs() {
					fieldBinlogs := getFieldBinlogs(tBinlogs.GetFieldID(), currBinlogs)
					if fieldBinlogs == nil {
						currBinlogs = append(currBinlogs, tBinlogs)
					} else {
						fieldBinlogs.Binlogs = append(fieldBinlogs.Binlogs, tBinlogs.Binlogs...)
					}
				}
				existing.Binlogs = currBinlogs

				// Merge statslogs
				currStatsLogs := existing.GetStatslogs()
				for _, tStatsLogs := range mergeData.GetStatslogs() {
					fieldStatsLog := getFieldBinlogs(tStatsLogs.GetFieldID(), currStatsLogs)
					if fieldStatsLog == nil {
						currStatsLogs = append(currStatsLogs, tStatsLogs)
					} else {
						fieldStatsLog.Binlogs = append(fieldStatsLog.Binlogs, tStatsLogs.Binlogs...)
					}
				}
				existing.Statslogs = currStatsLogs

				// Merge deltalogs
				existing.Deltalogs = append(existing.Deltalogs, mergeData.GetDeltalogs()...)

				// Start position
				if mergeData.GetStartPosition() != nil {
					existing.StartPosition = mergeData.GetStartPosition()
				}
				// Checkpoint
				if mergeData.GetDmlPosition() != nil {
					existing.DmlPosition = mergeData.GetDmlPosition()
				}
				existing.NumOfRows = mergeData.GetNumOfRows()

				return existing, true
			})
		} else {
			// Non-merge segments: just set state to Dropped
			txn.Update(ref.key, func(existing *datapb.SegmentInfo) (*datapb.SegmentInfo, bool) {
				existing.State = commonpb.SegmentState_Dropped
				existing.DroppedAt = uint64(time.Now().UnixNano())
				return existing, true
			})
		}
	}

	results, err := txn.Commit()
	if err != nil {
		log.Warn("meta update: update drop channel segment info failed",
			zap.String("channel", channel),
			zap.Error(err))
		return err
	}

	if err = m.catalog.MarkChannelDeleted(ctx, channel); err != nil {
		return err
	}

	// Compute metrics and update cache post-persist
	metricMutation := &segMetricMutation{
		stateChange: make(map[string]map[string]map[string]map[string]int),
	}
	for i, ref := range segRefs {
		newInfo := NewSegmentInfo(results[i].Value)
		oldSeg, existed := m.segments.SetSegment(ref.id, newInfo, results[i].Version)
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
		log.Ctx(m.ctx).Error("meta update: add allocation failed - segment not found", zap.Int64("segmentID", segmentID))
		return errors.New("meta update: add allocation failed - segment not found")
	}
	// As we use global segment lastExpire to guarantee data correctness after restart
	// there is no need to persist allocation to meta store, only update allocation in-memory meta.
	m.segments.AddAllocation(segmentID, allocation)
	log.Ctx(m.ctx).Info("meta update: add allocation - complete", zap.Int64("segmentID", segmentID))
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

func (m *meta) completeClusterCompactionMutation(t *datapb.CompactionTask, result *datapb.CompactionPlanResult) ([]*SegmentInfo, *segMetricMutation, error) {
	log := log.Ctx(context.TODO()).With(zap.Int64("planID", t.GetPlanID()),
		zap.String("type", t.GetType().String()),
		zap.Int64("collectionID", t.CollectionID),
		zap.Int64("partitionID", t.PartitionID),
		zap.String("channel", t.GetChannel()))

	metricMutation := &segMetricMutation{stateChange: make(map[string]map[string]map[string]map[string]int)}
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
			log.Warn("input segment was dropped during compaction mutation",
				zap.Int64("planID", t.GetPlanID()),
				zap.Int64("segmentID", segmentID),
				zap.String("state", segment.GetState().String()))
			return nil, nil, merr.WrapErrSegmentNotFound(segmentID, "input segment was dropped")
		}

		cloned := segment.Clone()

		compactFromSegInfos = append(compactFromSegInfos, cloned)
		compactFromSegIDs = append(compactFromSegIDs, cloned.GetID())
	}

	fallbackStart := getMinPosition(lo.Map(compactFromSegInfos, func(info *SegmentInfo, _ int) *msgpb.MsgPosition {
		return info.GetStartPosition()
	}))
	fallbackDml := getMaxPosition(lo.Map(compactFromSegInfos, func(info *SegmentInfo, _ int) *msgpb.MsgPosition {
		return info.GetDmlPosition()
	}))

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
			IsInvisible:    true,
			StorageVersion: seg.GetStorageVersion(),
			ManifestPath:   seg.GetManifest(),
			ExpirQuantiles: seg.GetExpirQuantiles(),
			SchemaVersion:  t.GetSchema().GetVersion(),
		}
		segment := NewSegmentInfo(segmentInfo)
		compactToSegInfos = append(compactToSegInfos, segment)
		compactToSegIDs = append(compactToSegIDs, segment.GetID())
		metricMutation.addNewSeg(segment.GetState(), segment.GetLevel(), segment.GetIsSorted(), segment.GetStorageVersion(), segment.GetNumOfRows())
	}

	log = log.With(zap.Int64s("compact from", compactFromSegIDs), zap.Int64s("compact to", compactToSegIDs))
	log.Debug("meta update: prepare for meta mutation - complete")

	// Persist new compactTo segments
	txn := m.segmentPersist.Txn(m.ctx)
	for _, info := range compactToSegInfos {
		txn.Insert(m.segmentKey(info.GetCollectionID(), info.GetPartitionID(), info.GetID()), info.SegmentInfo)
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
	t *datapb.CompactionTask,
	result *datapb.CompactionPlanResult,
) ([]*SegmentInfo, *segMetricMutation, error) {
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
			log.Warn("input segment was dropped during compaction mutation",
				zap.Int64("planID", t.GetPlanID()),
				zap.Int64("segmentID", segmentID),
				zap.String("state", segment.GetState().String()))
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

	fallbackStart := getMinPosition(lo.Map(compactFromCached, func(info *SegmentInfo, _ int) *msgpb.MsgPosition {
		return info.GetStartPosition()
	}))
	fallbackDml := getMaxPosition(lo.Map(compactFromCached, func(info *SegmentInfo, _ int) *msgpb.MsgPosition {
		return info.GetDmlPosition()
	}))

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
				SchemaVersion:       t.GetSchema().GetVersion(),
			})

		if compactToSegmentInfo.GetNumOfRows() == 0 {
			compactToSegmentInfo.State = commonpb.SegmentState_Dropped
		}

		// metrics mutation for compactTo segments
		metricMutation.addNewSeg(compactToSegmentInfo.GetState(), compactToSegmentInfo.GetLevel(), compactToSegmentInfo.GetIsSorted(), compactToSegmentInfo.GetStorageVersion(), compactToSegmentInfo.GetNumOfRows())

		log.Info("Add a new compactTo segment",
			zap.Int64("compactTo", compactToSegmentInfo.GetID()),
			zap.Int64("compactTo segment numRows", compactToSegmentInfo.GetNumOfRows()),
			zap.Int("binlog count", len(compactToSegmentInfo.GetBinlogs())),
			zap.Int("statslog count", len(compactToSegmentInfo.GetStatslogs())),
			zap.Int("deltalog count", len(compactToSegmentInfo.GetDeltalogs())),
			zap.Int64("segment size", compactToSegmentInfo.getSegmentSize()),
			zap.Int64s("expirQuantiles", compactToSegmentInfo.GetExpirQuantiles()),
		)
		compactToSegments = append(compactToSegments, compactToSegmentInfo)
	}

	log.Debug("meta update: prepare for meta mutation - complete")

	// Persist all segments atomically in one transaction
	txn := m.segmentPersist.Txn(m.ctx)
	for _, info := range compactToSegments {
		txn.Insert(m.segmentKey(info.GetCollectionID(), info.GetPartitionID(), info.GetID()), info.SegmentInfo)
	}
	for _, seg := range compactFromCached {
		txn.Update(m.segmentKey(seg.GetCollectionID(), seg.GetPartitionID(), seg.GetID()), func(existing *datapb.SegmentInfo) (*datapb.SegmentInfo, bool) {
			existing.State = commonpb.SegmentState_Dropped
			existing.DroppedAt = uint64(time.Now().UnixNano())
			existing.Compacted = true
			return existing, true
		})
	}
	results, err := txn.Commit()
	if err != nil {
		log.Warn("fail to alter segments for compaction", zap.Error(err))
		return nil, nil, err
	}
	toCount := len(compactToSegments)
	for i, info := range compactToSegments {
		m.segments.SetSegment(info.GetID(), info, results[i].Version)
	}
	for i, seg := range compactFromCached {
		newInfo := NewSegmentInfo(results[toCount+i].Value)
		old, existed := m.segments.SetSegment(seg.GetID(), newInfo, results[toCount+i].Version)
		if existed && old.GetState() != newInfo.GetState() {
			metricMutation.append(old.GetState(), newInfo.GetState(), newInfo.GetLevel(), newInfo.GetIsSorted(), newInfo.GetStorageVersion(), newInfo.GetNumOfRows())
		}
	}

	log.Info("meta update: alter in memory meta after compaction - complete")
	return compactToSegments, metricMutation, nil
}

func (m *meta) ValidateSegmentStateBeforeCompleteCompactionMutation(t *datapb.CompactionTask) error {

	for _, segmentID := range t.GetInputSegments() {
		segment := m.segments.GetSegment(segmentID)
		if !isSegmentHealthy(segment) {
			// SHOULD NOT HAPPEN: input segment was dropped.
			// This indicates that compaction tasks, which should be mutually exclusive,
			// may have executed concurrently.
			log.Warn("should not happen! input segment was dropped",
				zap.Int64("planID", t.GetPlanID()),
				zap.String("type", t.GetType().String()),
				zap.String("channel", t.GetChannel()),
				zap.Int64("partitionID", t.GetPartitionID()),
				zap.Int64("segmentID", segmentID),
			)
			return merr.WrapErrSegmentNotFound(segmentID, "input segment was dropped")
		}
	}
	return nil
}

func (m *meta) CompleteCompactionMutation(ctx context.Context, t *datapb.CompactionTask, result *datapb.CompactionPlanResult) ([]*SegmentInfo, *segMetricMutation, error) {

	switch t.GetType() {
	case datapb.CompactionType_MixCompaction:
		return m.completeMixCompactionMutation(t, result)
	case datapb.CompactionType_ClusteringCompaction:
		return m.completeClusterCompactionMutation(t, result)
	case datapb.CompactionType_SortCompaction:
		return m.completeSortCompactionMutation(t, result)
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

// UpdateChannelCheckpoint updates and saves channel checkpoint.
func (m *meta) UpdateChannelCheckpoint(ctx context.Context, vChannel string, pos *msgpb.MsgPosition) error {
	if pos == nil || pos.GetMsgID() == nil {
		return fmt.Errorf("channelCP is nil, vChannel=%s", vChannel)
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
		log.Ctx(context.TODO()).Info("UpdateChannelCheckpoint done",
			zap.String("vChannel", vChannel),
			zap.Uint64("ts", pos.GetTimestamp()),
			zap.ByteString("msgID", pos.GetMsgID()),
			zap.Stringer("walName", pos.WALName),
			zap.Time("time", ts))
		metrics.DataCoordCheckpointUnixSeconds.WithLabelValues(paramtable.GetStringNodeID(), vChannel).
			Set(float64(ts.Unix()))
	}
	return nil
}

// MarkChannelCheckpointDropped set channel checkpoint to MaxUint64 preventing future update
// and remove the metrics for channel checkpoint lag.
func (m *meta) MarkChannelCheckpointDropped(ctx context.Context, channel string) error {
	m.channelCPs.Lock()
	defer m.channelCPs.Unlock()

	cp := &msgpb.MsgPosition{
		ChannelName: channel,
		Timestamp:   math.MaxUint64,
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
	m.channelCPs.Lock()
	defer m.channelCPs.Unlock()
	toUpdates := lo.Filter(positions, func(pos *msgpb.MsgPosition, _ int) bool {
		if pos == nil || (pos.GetMsgID() == nil && pos.GetWALName() != commonpb.WALName_WoodPecker) || pos.GetChannelName() == "" {
			log.Warn("illegal channel cp", zap.Any("pos", pos))
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
		log.Info("UpdateChannelCheckpoint done", zap.String("channel", channel),
			zap.Stringer("walName", pos.WALName),
			zap.Uint64("ts", pos.GetTimestamp()),
			zap.Time("time", tsoutil.PhysicalTime(pos.GetTimestamp())))
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
	log.Ctx(context.TODO()).Info("DropChannelCheckpoint done", zap.String("vChannel", vChannel))
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

func (m *meta) GetCompactableSegmentGroupByCollection() map[int64][]*SegmentInfo {
	allSegs := m.SelectSegments(m.ctx, SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return isSegmentHealthy(segment) &&
			isFlushed(segment) && // sealed segment
			!segment.isCompacting && // not compacting now
			!segment.GetIsImporting() // not importing now
	}))

	ret := make(map[int64][]*SegmentInfo)
	for _, seg := range allSegs {
		if _, ok := ret[seg.CollectionID]; !ok {
			ret[seg.CollectionID] = make([]*SegmentInfo, 0)
		}

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

// initStateChangeEntry initializes the nested map structure for the given keys and returns the innermost map.
func (s *segMetricMutation) initStateChangeEntry(level, state, sortedStatus string) map[string]int {
	if _, ok := s.stateChange[level]; !ok {
		s.stateChange[level] = make(map[string]map[string]map[string]int)
	}
	if _, ok := s.stateChange[level][state]; !ok {
		s.stateChange[level][state] = make(map[string]map[string]int)
	}
	if _, ok := s.stateChange[level][state][sortedStatus]; !ok {
		s.stateChange[level][state][sortedStatus] = make(map[string]int)
	}
	return s.stateChange[level][state][sortedStatus]
}

// addNewSeg update metrics update for a new segment.
func (s *segMetricMutation) addNewSeg(state commonpb.SegmentState, level datapb.SegmentLevel, isSorted bool, storageVersion int64, rowCount int64) {
	storageVersionStr := fmt.Sprint(storageVersion)
	sortedStatus := getSortStatus(isSorted)
	entry := s.initStateChangeEntry(level.String(), state.String(), sortedStatus)
	entry[storageVersionStr] += 1

	s.rowCountChange += rowCount
	s.rowCountAccChange += rowCount
}

// commit persists all updates in current segMetricMutation, should and must be called AFTER segment state change
// has persisted in Etcd.
func (s *segMetricMutation) commit() {
	for level, submap := range s.stateChange {
		for state, sortedMap := range submap {
			for sortedLabel, versionMap := range sortedMap {
				for storageVersion, change := range versionMap {
					metrics.DataCoordNumSegments.WithLabelValues(state, level, sortedLabel, storageVersion).Add(float64(change))
				}
			}
		}
	}
}

// append updates current segMetricMutation when segment state change happens.
func (s *segMetricMutation) append(oldState, newState commonpb.SegmentState, level datapb.SegmentLevel, isSorted bool, storageVersion int64, rowCountUpdate int64) {
	if oldState != newState {
		storageVersionStr := fmt.Sprint(storageVersion)
		sortedStatus := getSortStatus(isSorted)
		levelStr := level.String()
		oldEntry := s.initStateChangeEntry(levelStr, oldState.String(), sortedStatus)
		newEntry := s.initStateChangeEntry(levelStr, newState.String(), sortedStatus)
		oldEntry[storageVersionStr] -= 1
		newEntry[storageVersionStr] += 1
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

func isFlushState(state commonpb.SegmentState) bool {
	return state == commonpb.SegmentState_Flushing || state == commonpb.SegmentState_Flushed
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

	log.Ctx(ctx).Debug("remove clustering compaction stats files",
		zap.Int64("collectionID", info.GetCollectionID()),
		zap.Int64("partitionID", info.GetPartitionID()),
		zap.String("vChannel", info.GetVChannel()),
		zap.Int64("planID", info.GetVersion()),
		zap.Strings("removePaths", removePaths))
	err := m.chunkManager.MultiRemove(context.Background(), removePaths)
	if err != nil {
		log.Ctx(ctx).Warn("remove clustering compaction stats files failed", zap.Error(err))
		return err
	}

	// first clean analyze task
	if err = m.analyzeMeta.DropAnalyzeTask(ctx, info.GetAnalyzeTaskID()); err != nil {
		log.Ctx(ctx).Warn("remove analyze task failed", zap.Int64("analyzeTaskID", info.GetAnalyzeTaskID()), zap.Error(err))
		return err
	}

	// finally, clean up the partition stats info, and make sure the analysis task is cleaned up
	err = m.partitionStatsMeta.DropPartitionStatsInfo(ctx, info)
	log.Ctx(ctx).Debug("drop partition stats meta",
		zap.Int64("collectionID", info.GetCollectionID()),
		zap.Int64("partitionID", info.GetPartitionID()),
		zap.String("vChannel", info.GetVChannel()),
		zap.Int64("planID", info.GetVersion()))
	if err != nil {
		return err
	}
	return nil
}

func (m *meta) completeSortCompactionMutation(
	t *datapb.CompactionTask,
	result *datapb.CompactionPlanResult,
) ([]*SegmentInfo, *segMetricMutation, error) {
	log := log.Ctx(context.TODO()).With(zap.Int64("planID", t.GetPlanID()),
		zap.String("type", t.GetType().String()),
		zap.Int64("collectionID", t.CollectionID),
		zap.Int64("partitionID", t.PartitionID),
		zap.String("channel", t.GetChannel()))

	metricMutation := &segMetricMutation{stateChange: make(map[string]map[string]map[string]map[string]int)}
	compactFromSegID := t.GetInputSegments()[0]
	oldSegment := m.segments.GetSegment(compactFromSegID)
	if oldSegment == nil {
		return nil, nil, merr.WrapErrSegmentNotFound(compactFromSegID)
	}

	// Re-validate segment health to prevent race condition with drop collection
	// between ValidateSegmentStateBeforeCompleteCompactionMutation and here
	if !isSegmentHealthy(oldSegment) {
		log.Warn("input segment was dropped during compaction mutation",
			zap.Int64("planID", t.GetPlanID()),
			zap.Int64("segmentID", compactFromSegID),
			zap.String("state", oldSegment.GetState().String()))
		return nil, nil, merr.WrapErrSegmentNotFound(compactFromSegID, "input segment was dropped")
	}

	resultInvisible := oldSegment.GetIsInvisible()
	if !oldSegment.GetCreatedByCompaction() {
		resultInvisible = false
	}

	resultSegment := result.GetSegments()[0]

	segmentInfo := &datapb.SegmentInfo{
		CollectionID:              oldSegment.GetCollectionID(),
		PartitionID:               oldSegment.GetPartitionID(),
		InsertChannel:             oldSegment.GetInsertChannel(),
		MaxRowNum:                 oldSegment.GetMaxRowNum(),
		LastExpireTime:            oldSegment.GetLastExpireTime(),
		StartPosition:             oldSegment.GetStartPosition(),
		DmlPosition:               oldSegment.GetDmlPosition(),
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
	}

	segment := NewSegmentInfo(segmentInfo)
	if segment.GetNumOfRows() > 0 {
		metricMutation.addNewSeg(segment.GetState(), segment.GetLevel(), segment.GetIsSorted(), segment.GetStorageVersion(), segment.GetNumOfRows())
	} else {
		segment.State = commonpb.SegmentState_Dropped
		segment.DroppedAt = uint64(time.Now().UnixNano())
		log.Info("drop segment due to 0 rows", zap.Int64("segmentID", segment.GetID()))
	}

	log = log.With(zap.Int64s("compactFrom", []int64{oldSegment.GetID()}), zap.Int64("compactTo", segment.GetID()))

	log.Info("meta update: prepare for complete stats mutation - complete",
		zap.Int64("num rows", segment.GetNumOfRows()),
		zap.Int64("segment size", segment.getSegmentSize()),
		zap.Int64s("expirQuantiles", segment.GetExpirQuantiles()))
	// Persist old (dropped) and new segments atomically — all modification in UpdateFunc.
	oldKey := m.segmentKey(oldSegment.GetCollectionID(), oldSegment.GetPartitionID(), oldSegment.GetID())
	newKey := m.segmentKey(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())
	txn := m.segmentPersist.Txn(m.ctx)
	txn.Update(oldKey, func(existing *datapb.SegmentInfo) (*datapb.SegmentInfo, bool) {
		existing.State = commonpb.SegmentState_Dropped
		existing.DroppedAt = uint64(time.Now().UnixNano())
		existing.Compacted = true
		return existing, true
	})
	txn.Insert(newKey, segment.SegmentInfo)
	results, err := txn.Commit()
	if err != nil {
		log.Warn("fail to persist segments for sort compaction", zap.Error(err))
		return nil, nil, err
	}

	// Update cache and compute metrics from returned old values.
	oldRetSeg := NewSegmentInfo(results[0].Value)
	old, existed := m.segments.SetSegment(oldSegment.GetID(), oldRetSeg, results[0].Version)
	if existed && old.GetState() != oldRetSeg.GetState() {
		metricMutation.append(old.GetState(), oldRetSeg.GetState(), oldRetSeg.GetLevel(), oldRetSeg.GetIsSorted(), oldRetSeg.GetStorageVersion(), oldRetSeg.GetNumOfRows())
	}
	m.segments.SetSegment(segment.GetID(), segment, results[1].Version)
	log.Info("meta update: alter in memory meta after compaction - complete")
	return []*SegmentInfo{segment}, metricMutation, nil
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
	type segRef struct {
		id  int64
		key string
	}
	var segRefs []segRef
	for _, seg := range m.segments.GetSegments() {
		if contains(partitionIDs, seg.PartitionID) {
			segRefs = append(segRefs, segRef{
				id:  seg.GetID(),
				key: m.segmentKey(seg.GetCollectionID(), seg.GetPartitionID(), seg.GetID()),
			})
		}
	}

	// All modification inside UpdateFunc.
	txn := m.segmentPersist.Txn(m.ctx)
	for _, ref := range segRefs {
		txn.Update(ref.key, func(existing *datapb.SegmentInfo) (*datapb.SegmentInfo, bool) {
			existing.State = commonpb.SegmentState_Dropped
			existing.DroppedAt = uint64(time.Now().UnixNano())
			return existing, true
		})
	}
	results, err := txn.Commit()
	if err != nil {
		return err
	}

	// Compute metrics and update cache from returned persist values.
	metricMutation := &segMetricMutation{
		stateChange: make(map[string]map[string]map[string]map[string]int),
	}
	for i, ref := range segRefs {
		newSeg := NewSegmentInfo(results[i].Value)
		oldSeg, existed := m.segments.SetSegment(ref.id, newSeg, results[i].Version)
		if existed && oldSeg.GetState() != newSeg.GetState() {
			metricMutation.append(oldSeg.GetState(), newSeg.GetState(), newSeg.GetLevel(), newSeg.GetIsSorted(), newSeg.GetStorageVersion(), newSeg.GetNumOfRows())
		}
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
			return nil, errors.Errorf("file resource %d not found", id)
		}
	}
	return resources, nil
}

// TruncateChannelByTime drops segments of a channel that were updated before the flush timestamp
func (m *meta) TruncateChannelByTime(ctx context.Context, vChannel string, flushTs uint64) error {

	segments := m.segments.GetSegmentsBySelector(SegmentFilterFunc(isSegmentHealthy), WithChannel(vChannel))

	// Collect segments to drop (read-only from cache for key construction and filtering).
	type segRef struct {
		id  int64
		key string
	}
	var segRefs []segRef
	for _, segment := range segments {
		if segment.GetDmlPosition().GetTimestamp() <= flushTs && segment.GetState() != commonpb.SegmentState_Dropped {
			segRefs = append(segRefs, segRef{
				id:  segment.GetID(),
				key: m.segmentKey(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID()),
			})
		}
	}

	if len(segRefs) == 0 {
		return nil
	}

	// All modification inside UpdateFunc.
	txn := m.segmentPersist.Txn(ctx)
	for _, ref := range segRefs {
		txn.Update(ref.key, func(existing *datapb.SegmentInfo) (*datapb.SegmentInfo, bool) {
			existing.State = commonpb.SegmentState_Dropped
			existing.DroppedAt = uint64(time.Now().UnixNano())
			return existing, true
		})
	}
	results, err := txn.Commit()
	if err != nil {
		log.Ctx(ctx).Warn("Failed to batch set segments state to dropped", zap.Error(err))
		return err
	}

	// Compute metrics and update cache from returned persist values.
	metricMutation := &segMetricMutation{
		stateChange: make(map[string]map[string]map[string]map[string]int),
	}
	for i, ref := range segRefs {
		newSeg := NewSegmentInfo(results[i].Value)
		oldSeg, existed := m.segments.SetSegment(ref.id, newSeg, results[i].Version)
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
