// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package delegator

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	reopenBM25RepairInitialBackoff = 200 * time.Millisecond
	reopenBM25RepairMaxBackoff     = 3 * time.Second
)

// Reopen repair is additive: a key identifies the segment incarnation, WAL
// schema, and storage payload that may contain a missing BM25 field. Replacing
// an already installed field generation needs an atomic swap protocol and is
// intentionally outside this path.
type reopenBM25RepairKey struct {
	segmentID      int64
	schemaVersion  uint64
	nodeID         int64
	partitionID    int64
	segmentVersion int64
	dataVersion    int32
	manifestPath   string
	legacyPayload  string
}

type reopenBM25RepairEntry struct {
	key            reopenBM25RepairKey
	info           *querypb.SegmentLoadInfo
	schema         *schemapb.CollectionSchema
	requestVersion int64
	workerID       int64
	incarnationMu  sync.Mutex
	incarnation    SegmentEntry
	incarnationSet bool
	started        bool
}

func (sd *shardDelegator) newReopenBM25RepairEntry(req *querypb.LoadSegmentsRequest, info *querypb.SegmentLoadInfo) *reopenBM25RepairEntry {
	schema := req.GetSchema()
	if schema == nil {
		schema, _ = sd.delegatorSchemaSnapshot()
	} else {
		schema = typeutil.Clone(schema)
	}
	if len(newBM25FunctionSet(schema)) == 0 {
		return nil
	}

	entry := &reopenBM25RepairEntry{
		info:           typeutil.Clone(info),
		schema:         schema,
		requestVersion: req.GetVersion(),
		workerID:       req.GetDstNodeID(),
	}
	entry.key = reopenBM25RepairKey{
		segmentID:     info.GetSegmentID(),
		schemaVersion: uint64(schema.GetVersion()),
		nodeID:        req.GetDstNodeID(),
		partitionID:   info.GetPartitionID(),
		dataVersion:   info.GetDataVersion(),
		manifestPath:  info.GetManifestPath(),
		legacyPayload: reopenBM25LegacyPayloadKey(info),
	}

	// Reopen operates on an existing route. Capture it when possible so retries
	// of the same QueryCoord request deduplicate by the real incarnation, while a
	// release/reload of the same segment ID gets an independent repair.
	sd.distribution.mut.RLock()
	incarnation, ok := sd.distribution.sealedSegments[info.GetSegmentID()]
	if ok && incarnation.NodeID == entry.workerID &&
		incarnation.PartitionID == info.GetPartitionID() &&
		(entry.requestVersion == 0 || incarnation.Version <= entry.requestVersion) {
		entry.incarnation = incarnation
		entry.incarnationSet = true
		entry.key.segmentVersion = incarnation.Version
	}
	sd.distribution.mut.RUnlock()
	return entry
}

func reopenBM25LegacyPayloadKey(info *querypb.SegmentLoadInfo) string {
	pathsByField := make(map[int64][]string, len(info.GetBm25Logs()))
	for _, field := range info.GetBm25Logs() {
		fieldID := field.GetFieldID()
		if _, ok := pathsByField[fieldID]; !ok {
			pathsByField[fieldID] = nil
		}
		for _, binlog := range field.GetBinlogs() {
			pathsByField[fieldID] = append(pathsByField[fieldID], binlog.GetLogPath())
		}
	}

	fieldIDs := make([]int64, 0, len(pathsByField))
	for fieldID := range pathsByField {
		fieldIDs = append(fieldIDs, fieldID)
	}
	slices.Sort(fieldIDs)

	var builder strings.Builder
	for _, fieldID := range fieldIDs {
		paths := pathsByField[fieldID]
		slices.Sort(paths)
		builder.WriteString(strconv.FormatInt(fieldID, 10))
		builder.WriteByte(':')
		builder.WriteString(strconv.Itoa(len(paths)))
		builder.WriteByte(':')
		for _, path := range paths {
			// Length-prefix each path so distinct payloads cannot collide on
			// delimiter characters that are valid in object-storage paths.
			builder.WriteString(strconv.Itoa(len(path)))
			builder.WriteByte(':')
			builder.WriteString(path)
		}
		builder.WriteByte(';')
	}
	return builder.String()
}

func (sd *shardDelegator) reserveReopenBM25Repair(entry *reopenBM25RepairEntry) *reopenBM25RepairEntry {
	if entry == nil || entry.info == nil {
		return nil
	}

	sd.reopenBM25RepairMu.Lock()
	defer sd.reopenBM25RepairMu.Unlock()
	if sd.reopenBM25Repairs == nil {
		sd.reopenBM25Repairs = make(map[reopenBM25RepairKey]*reopenBM25RepairEntry)
	}
	if existing, ok := sd.reopenBM25Repairs[entry.key]; ok {
		return existing
	}
	sd.reopenBM25Repairs[entry.key] = entry
	return entry
}

func (sd *shardDelegator) reserveReopenBM25Repairs(req *querypb.LoadSegmentsRequest) []*reopenBM25RepairEntry {
	entries := make([]*reopenBM25RepairEntry, 0, len(req.GetInfos()))
	for _, info := range req.GetInfos() {
		entry := sd.newReopenBM25RepairEntry(req, info)
		if reserved := sd.reserveReopenBM25Repair(entry); reserved != nil {
			entries = append(entries, reserved)
		}
	}
	return entries
}

func (sd *shardDelegator) finishReopenBM25Repair(entry *reopenBM25RepairEntry) {
	sd.reopenBM25RepairMu.Lock()
	defer sd.reopenBM25RepairMu.Unlock()
	if sd.reopenBM25Repairs[entry.key] == entry {
		delete(sd.reopenBM25Repairs, entry.key)
	}
}

func (sd *shardDelegator) startReopenBM25Repair(entry *reopenBM25RepairEntry) {
	sd.reopenBM25RepairMu.Lock()
	if sd.reopenBM25Repairs[entry.key] != entry || entry.started {
		sd.reopenBM25RepairMu.Unlock()
		return
	}
	if err := sd.lifetime.Add(sd.NotStopped); err != nil {
		sd.reopenBM25RepairMu.Unlock()
		return
	}
	entry.started = true
	sd.reopenBM25RepairMu.Unlock()
	go sd.runReopenBM25Repair(entry)
}

func (sd *shardDelegator) runReopenBM25Repair(entry *reopenBM25RepairEntry) {
	defer sd.lifetime.Done()
	_ = retry.Handle(sd.reopenBM25RepairCtx, func() (bool, error) {
		var obsolete bool
		err := sd.withPostLoadLimit(sd.reopenBM25RepairCtx, func() error {
			var err error
			obsolete, err = sd.loadReopenBM25Stats(sd.reopenBM25RepairCtx, entry)
			return err
		})
		if obsolete || err == nil {
			sd.finishReopenBM25Repair(entry)
			return false, nil
		}
		return true, merr.Wrapf(err, "repair reopened BM25 stats for segment %d", entry.info.GetSegmentID())
	}, retry.Attempts(0), retry.Sleep(reopenBM25RepairInitialBackoff), retry.MaxSleepTime(reopenBM25RepairMaxBackoff))
}

func (sd *shardDelegator) loadReopenBM25Stats(ctx context.Context, entry *reopenBM25RepairEntry) (bool, error) {
	return sd.loadReopenBM25StatsInternal(ctx, entry, false)
}

// loadReopenBM25StatsWithSchemaLease is the foreground Reopen path. The caller
// must hold schemaChangeMutex.RLock for the whole call so a successful worker
// Reopen cannot race a newer schema transition before its BM25 stats install.
func (sd *shardDelegator) loadReopenBM25StatsWithSchemaLease(ctx context.Context, entry *reopenBM25RepairEntry) (bool, error) {
	return sd.loadReopenBM25StatsInternal(ctx, entry, true)
}

func (sd *shardDelegator) loadReopenBM25StatsInternal(ctx context.Context, entry *reopenBM25RepairEntry, schemaLeaseHeld bool) (bool, error) {
	var (
		idfOracle reopenBM25IDFOracle
		obsolete  bool
		err       error
	)
	if schemaLeaseHeld {
		idfOracle, obsolete, err = sd.validateReopenBM25PrerequisitesLocked(entry)
	} else {
		sd.schemaChangeMutex.RLock()
		idfOracle, obsolete, err = sd.validateReopenBM25PrerequisitesLocked(entry)
		sd.schemaChangeMutex.RUnlock()
	}
	if obsolete || err != nil {
		return obsolete, err
	}

	sd.distribution.mut.RLock()
	obsolete, err = sd.validateReopenBM25DistributionLocked(entry, idfOracle)
	sd.distribution.mut.RUnlock()
	if obsolete || err != nil {
		return obsolete, err
	}

	bm25Paths, err := packed.NewStatsResolverFromLoadInfo(entry.info).BM25StatsPaths()
	if err != nil {
		return false, err
	}
	if len(bm25Paths) == 0 {
		return true, nil
	}

	var compatiblePaths map[int64][]string
	if schemaLeaseHeld {
		idfOracle, compatiblePaths, obsolete, err = sd.validateReopenBM25SchemaLocked(entry, bm25Paths)
	} else {
		sd.schemaChangeMutex.RLock()
		idfOracle, compatiblePaths, obsolete, err = sd.validateReopenBM25SchemaLocked(entry, bm25Paths)
		sd.schemaChangeMutex.RUnlock()
	}
	if obsolete || err != nil {
		return obsolete, err
	}

	sd.distribution.mut.RLock()
	obsolete, err = sd.validateReopenBM25DistributionLocked(entry, idfOracle)
	sd.distribution.mut.RUnlock()
	if obsolete || err != nil {
		return obsolete, err
	}

	cm := sd.loader.GetChunkManager()
	return idfOracle.loadSealedForReopenWithFence(ctx, entry.info.GetSegmentID(), entry.info, compatiblePaths, cm, func() (func(), bool, error) {
		lockedSchemaHere := false
		if !schemaLeaseHeld {
			// This fence runs while the per-segment IDF load lock is held. Do not
			// wait behind a schema writer: a foreground Reopen may already hold a
			// schema read lease while waiting for the same segment lock.
			if !sd.schemaChangeMutex.TryRLock() {
				return nil, false, merr.WrapErrServiceNotReadyMsg(
					"reopen BM25 install waits for the current schema transition")
			}
			lockedSchemaHere = true
		}
		_, stillCompatible, obsolete, err := sd.validateReopenBM25SchemaLocked(entry, compatiblePaths)
		if !obsolete && err == nil && len(stillCompatible) != len(compatiblePaths) {
			obsolete = true
		}
		if obsolete || err != nil {
			if lockedSchemaHere {
				sd.schemaChangeMutex.RUnlock()
			}
			return nil, obsolete, err
		}

		sd.distribution.mut.RLock()
		obsolete, err = sd.validateReopenBM25DistributionLocked(entry, idfOracle)
		if obsolete || err != nil {
			sd.distribution.mut.RUnlock()
			if lockedSchemaHere {
				sd.schemaChangeMutex.RUnlock()
			}
			return nil, obsolete, err
		}
		return func() {
			sd.distribution.mut.RUnlock()
			if lockedSchemaHere {
				sd.schemaChangeMutex.RUnlock()
			}
		}, false, nil
	})
}

func (sd *shardDelegator) validateReopenBM25PrerequisitesLocked(entry *reopenBM25RepairEntry) (reopenBM25IDFOracle, bool, error) {
	currentSchema, currentVersion := sd.delegatorSchemaSnapshotLocked()
	if currentVersion < entry.key.schemaVersion {
		return nil, false, merr.WrapErrServiceNotReadyMsg(
			"reopen BM25 schema version %d is ahead of delegator schema version %d",
			entry.key.schemaVersion, currentVersion)
	}

	compatible := false
	for fieldID := range newBM25FunctionSet(entry.schema) {
		if sameBM25SchemaDefinition(entry.schema, currentSchema, fieldID) {
			compatible = true
			break
		}
	}
	if !compatible {
		return nil, true, nil
	}

	oracle := sd.getIDFOracle()
	if oracle == nil {
		return nil, false, merr.WrapErrServiceNotReadyMsg(
			"reopen contains BM25 stats before delegator BM25 oracle is initialized")
	}
	idfOracle, ok := oracle.(reopenBM25IDFOracle)
	if !ok {
		return nil, false, merr.WrapErrServiceInternal("BM25 oracle does not support fenced reopen repair")
	}
	return idfOracle, false, nil
}

// validateReopenBM25SchemaLocked accepts a newer WAL schema only when the BM25
// function and its referenced fields still have identical semantics.
func (sd *shardDelegator) validateReopenBM25SchemaLocked(
	entry *reopenBM25RepairEntry,
	bm25Paths map[int64][]string,
) (reopenBM25IDFOracle, map[int64][]string, bool, error) {
	idfOracle, obsolete, err := sd.validateReopenBM25PrerequisitesLocked(entry)
	if obsolete || err != nil {
		return nil, nil, obsolete, err
	}
	currentSchema, _ := sd.delegatorSchemaSnapshotLocked()
	compatiblePaths := make(map[int64][]string, len(bm25Paths))
	for fieldID, paths := range bm25Paths {
		if sameBM25SchemaDefinition(entry.schema, currentSchema, fieldID) {
			compatiblePaths[fieldID] = paths
		}
	}
	if len(compatiblePaths) == 0 {
		return nil, nil, true, nil
	}
	return idfOracle, compatiblePaths, false, nil
}

func sameBM25SchemaDefinition(expected, current *schemapb.CollectionSchema, outputFieldID int64) bool {
	expectedFunction, ok := newBM25FunctionSet(expected)[outputFieldID]
	if !ok {
		return false
	}
	currentFunction, ok := newBM25FunctionSet(current)[outputFieldID]
	if !ok || !sameBM25Function(expectedFunction, currentFunction) {
		return false
	}

	expectedFields := fieldsByID(expected.GetFields())
	currentFields := fieldsByID(current.GetFields())
	sameField := func(fieldID int64) bool {
		expectedField, expectedOK := expectedFields[fieldID]
		currentField, currentOK := currentFields[fieldID]
		return expectedOK && currentOK && proto.Equal(
			normalizeBM25FieldSchema(expectedField),
			normalizeBM25FieldSchema(currentField),
		)
	}
	for _, fieldID := range expectedFunction.GetInputFieldIds() {
		if !sameField(fieldID) {
			return false
		}
	}
	for _, fieldID := range expectedFunction.GetOutputFieldIds() {
		if !sameField(fieldID) {
			return false
		}
	}
	return true
}

// QueryCoord materializes load-only mmap and warmup settings into request
// field TypeParams. They do not change BM25 stats semantics and are therefore
// excluded from the WAL/request schema fence.
func normalizeBM25FieldSchema(field *schemapb.FieldSchema) *schemapb.FieldSchema {
	if field == nil {
		return nil
	}
	normalized := typeutil.Clone(field)
	normalized.TypeParams = normalized.TypeParams[:0]
	for _, param := range field.GetTypeParams() {
		if param.GetKey() != common.MmapEnabledKey && param.GetKey() != common.WarmupKey {
			normalized.TypeParams = append(normalized.TypeParams, typeutil.Clone(param))
		}
	}
	return normalized
}

func fieldsByID(fields []*schemapb.FieldSchema) map[int64]*schemapb.FieldSchema {
	result := make(map[int64]*schemapb.FieldSchema, len(fields))
	for _, field := range fields {
		result[field.GetFieldID()] = field
	}
	return result
}

func sameReopenBM25SegmentIncarnation(a, b SegmentEntry) bool {
	return a.SegmentID == b.SegmentID &&
		a.NodeID == b.NodeID &&
		a.PartitionID == b.PartitionID &&
		a.Version == b.Version &&
		a.Level == b.Level
}

// validateReopenBM25DistributionLocked requires distribution.mut to be held.
func (sd *shardDelegator) validateReopenBM25DistributionLocked(entry *reopenBM25RepairEntry, idfOracle reopenBM25IDFOracle) (bool, error) {
	segmentID := entry.info.GetSegmentID()
	_, inTarget := sd.distribution.queryView.sealedSegmentRowCount[segmentID]
	if sd.distribution.queryView.syncedByCoord && !inTarget {
		return true, nil
	}

	current, routed := sd.distribution.sealedSegments[segmentID]
	if !routed {
		return false, merr.WrapErrServiceNotReadyMsg(
			"reopened segment %d is still in target but has no delegator distribution", segmentID)
	}
	if current.NodeID != entry.workerID || current.PartitionID != entry.info.GetPartitionID() {
		return true, nil
	}
	if entry.requestVersion != 0 && current.Version > entry.requestVersion {
		return true, nil
	}
	entry.incarnationMu.Lock()
	defer entry.incarnationMu.Unlock()
	if entry.incarnationSet {
		if !sameReopenBM25SegmentIncarnation(entry.incarnation, current) {
			return true, nil
		}
	} else {
		entry.incarnation = current
		entry.incarnationSet = true
	}
	if !inTarget {
		return false, merr.WrapErrServiceNotReadyMsg(
			"reopened segment %d waits for the first authoritative delegator target", segmentID)
	}

	targetVersion := sd.distribution.queryView.GetVersion()
	if idfOracle.TargetVersion() != targetVersion {
		return false, merr.WrapErrServiceNotReadyMsg(
			"reopened segment %d BM25 target version has not reached distribution target version %d",
			segmentID, targetVersion)
	}
	if current.Level == datapb.SegmentLevel_L0 ||
		!sd.distribution.queryView.partitions.Contain(current.PartitionID) ||
		(current.TargetVersion != targetVersion && current.TargetVersion != initialTargetVersion) {
		return false, merr.WrapErrServiceNotReadyMsg(
			"reopened segment %d is not readable in delegator distribution", segmentID)
	}
	return false, nil
}
