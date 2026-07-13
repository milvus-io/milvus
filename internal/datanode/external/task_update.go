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

package external

// RefreshExternalCollectionTask handles updating external collection segments by fetching fragments from external sources
// and organizing them into segments with balanced row counts.
//
// SEGMENT ID ALLOCATION WORKFLOW:
// - DataCoord pre-allocates a batch of segment IDs (default 1000) via allocator.AllocN()
// - Pre-allocated ID range is passed to DataNode via RefreshExternalCollectionTaskRequest.PreAllocatedSegmentIds
// - DataNode extracts the IDRange and uses pre-allocated IDs sequentially for each new segment
// - Manifest files are written directly to final StorageV3 insert_log paths
//
// NO TEMPORARY PATHS OR CLEANUP:
// - All segments use real, pre-allocated IDs (no temporary negative IDs)
// - All manifest files are written to final paths immediately
// - No cleanup logic is needed because:
//   * IDs are pre-allocated, ensuring uniqueness
//   * Paths are final, so failed manifests remain in final locations but are not registered in meta
//   * DataCoord validates and registers only successful manifests
//
// KEY IMPROVEMENTS OVER PREVIOUS DESIGN:
// - Eliminates temporary paths and associated cleanup complexity
// - Reduces risk of ID collision or exhaustion
// - Simplifies the segment organization workflow
// - Directly writes to final storage locations

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storageprofile"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func ensureContext(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// RefreshExternalCollectionTask handles updating external collection segments
type RefreshExternalCollectionTask struct {
	ctx context.Context

	req *datapb.RefreshExternalCollectionTaskRequest
	tr  *timerecord.TimeRecorder

	state      indexpb.JobState
	failReason string

	// Cached parsed spec (populated in PreExecute, reused in Execute)
	parsedSpec *externalspec.ExternalSpec
	columns    []string

	milvusTableSourcePKFieldMu sync.Mutex
	milvusTableSourcePKField   *schemapb.FieldSchema

	milvusTableSourceDeltalogsMu sync.Mutex
	milvusTableSourceDeltalogs   map[string][]*datapb.FieldBinlog

	// Result after execution — tracked separately for correct response building
	keptSegmentIDs  []int64               // IDs of current segments that were kept unchanged
	updatedSegments []*datapb.SegmentInfo // upsert payload: patched current segments plus newly created segments

	// Pre-allocated segment IDs
	preallocatedIDRange *datapb.IDRange // pre-allocated segment ID range (begin, end)
	nextAllocID         int64           // next available segment ID from pre-allocated range
	profileScope        *storageprofile.Scope
}

// NewRefreshExternalCollectionTask creates a new refresh-external-collection task.
// ctx owns the task lifetime; cancellation is driven by the caller's context
// (typically the manager's worker-pool closure).
func NewRefreshExternalCollectionTask(
	ctx context.Context,
	req *datapb.RefreshExternalCollectionTaskRequest,
) *RefreshExternalCollectionTask {
	attribution := storageprofile.Attribution{
		ScopeType:     storageprofile.ScopeTypeTask,
		TaskID:        fmt.Sprint(req.GetTaskID()),
		Component:     "datanode",
		NodeID:        paramtable.GetNodeID(),
		CollectionID:  req.GetCollectionID(),
		WorkloadClass: storageprofile.WorkloadClassBackground,
		WorkloadKind:  storageprofile.WorkloadKindExternalSync,
		Phase:         storageprofile.WorkloadPhaseReadSource,
		StorageRole:   storageprofile.StorageRoleSource,
	}
	profileScope := storageprofile.NewTaskScope(attribution)
	return &RefreshExternalCollectionTask{
		ctx:          profileScope.Bind(storageprofile.WithDefaultAttribution(ctx, attribution)),
		req:          req,
		tr:           timerecord.NewTimeRecorder(fmt.Sprintf("RefreshExternalCollectionTask: %d", req.GetTaskID())),
		state:        indexpb.JobState_JobStateInit,
		profileScope: profileScope,
	}
}

func (t *RefreshExternalCollectionTask) Ctx() context.Context {
	return t.ctx
}

func (t *RefreshExternalCollectionTask) Name() string {
	return fmt.Sprintf("RefreshExternalCollectionTask-%d", t.req.GetTaskID())
}

func (t *RefreshExternalCollectionTask) OnEnqueue(ctx context.Context) error {
	t.tr.RecordSpan()
	mlog.Info(ctx, "RefreshExternalCollectionTask enqueued",
		mlog.Int64("taskID", t.req.GetTaskID()),
		mlog.Int64("collectionID", t.req.GetCollectionID()))
	return nil
}

func (t *RefreshExternalCollectionTask) SetState(state indexpb.JobState, failReason string) {
	t.state = state
	t.failReason = failReason
}

func (t *RefreshExternalCollectionTask) GetState() indexpb.JobState {
	return t.state
}

func (t *RefreshExternalCollectionTask) GetSlot() int64 {
	return 1
}

func (t *RefreshExternalCollectionTask) Reset() {
	if t.profileScope != nil {
		t.profileScope.Finish()
	}
	t.ctx = nil
	t.req = nil
	t.tr = nil
	t.keptSegmentIDs = nil
	t.updatedSegments = nil
	t.profileScope = nil
}

func (t *RefreshExternalCollectionTask) PreExecute(ctx context.Context) error {
	if err := ensureContext(ctx); err != nil {
		return err
	}
	mlog.Info(ctx, "RefreshExternalCollectionTask PreExecute",
		mlog.Int64("taskID", t.req.GetTaskID()),
		mlog.Int64("collectionID", t.req.GetCollectionID()))

	if t.req == nil {
		return merr.WrapErrParameterInvalidMsg("request is nil")
	}
	if t.req.GetSchema() == nil {
		return merr.WrapErrParameterInvalidMsg("schema is nil in request")
	}
	if t.req.GetStorageConfig() == nil {
		return merr.WrapErrParameterInvalidMsg("storage config is nil in request")
	}
	if t.req.GetExternalSource() == "" {
		return merr.WrapErrParameterInvalidMsg("external source is empty in request")
	}

	// Parse and cache external spec for reuse during Execute
	spec, err := externalspec.ParseExternalSpec(t.req.GetExternalSpec())
	if err != nil {
		return merr.Wrap(err, "failed to parse external spec")
	}
	t.parsedSpec = spec
	schema := proto.Clone(t.req.GetSchema()).(*schemapb.CollectionSchema)
	if schema.GetExternalSource() == "" {
		schema.ExternalSource = t.req.GetExternalSource()
	}
	if schema.GetExternalSpec() == "" {
		schema.ExternalSpec = t.req.GetExternalSpec()
	}
	t.req.Schema = schema
	t.columns = packed.GetColumnNamesFromSchema(schema)

	return nil
}

func (t *RefreshExternalCollectionTask) Execute(ctx context.Context) error {
	if err := ensureContext(ctx); err != nil {
		return err
	}
	mlog.Info(context.TODO(), "RefreshExternalCollectionTask Execute",
		mlog.Int64("taskID", t.req.GetTaskID()),
		mlog.Int64("collectionID", t.req.GetCollectionID()))

	// Initialize pre-allocated segment IDs from request
	if t.req.GetPreAllocatedSegmentIds() == nil {
		return merr.WrapErrParameterInvalidMsg("pre-allocated segment IDs not provided in request")
	}

	t.preallocatedIDRange = t.req.GetPreAllocatedSegmentIds()
	t.nextAllocID = t.preallocatedIDRange.Begin

	mlog.Info(context.TODO(), "Initialized pre-allocated segment ID range",
		mlog.Int64("idBegin", t.preallocatedIDRange.Begin),
		mlog.Int64("idEnd", t.preallocatedIDRange.End),
		mlog.Int64("count", t.preallocatedIDRange.End-t.preallocatedIDRange.Begin))

	// Fetch fragments from external source
	newFragments, err := t.fetchFragmentsFromExternalSource(ctx)
	if err != nil {
		return merr.Wrap(err, "failed to fetch fragments")
	}

	// Build current segment -> fragments mapping
	currentSegmentFragments, err := t.buildCurrentSegmentFragments()
	if err != nil {
		return merr.Wrap(err, "failed to build current segment fragments")
	}

	// Compare and organize segments
	_, err = t.organizeSegments(ctx, currentSegmentFragments, newFragments)
	if err != nil {
		return err
	}

	return nil
}

// fetchFragmentsFromExternalSource reads file info from the explore manifest
// written by DataCoord and returns fragments for the assigned file range.
func (t *RefreshExternalCollectionTask) fetchFragmentsFromExternalSource(ctx context.Context) ([]packed.Fragment, error) {
	manifestPath := t.req.GetExploreManifestPath()
	if manifestPath == "" {
		return nil, merr.WrapErrParameterMissingMsg("explore manifest path is required but not provided")
	}

	mlog.Info(ctx, "reading file list from explore manifest",
		mlog.String("manifestPath", manifestPath),
		mlog.Int64("fileIndexBegin", t.req.GetFileIndexBegin()),
		mlog.Int64("fileIndexEnd", t.req.GetFileIndexEnd()))

	targetRowsPerSegment := paramtable.Get().DataNodeCfg.ExternalCollectionTargetRowsPerSegment.GetAsInt64()

	return packed.FetchFragmentsFromExternalSourceWithRange(
		ctx,
		t.parsedSpec.Format,
		t.columns,
		t.req.GetExternalSource(),
		t.req.GetStorageConfig(),
		t.req.GetFileIndexBegin(),
		t.req.GetFileIndexEnd(),
		manifestPath,
		packed.ExternalFetchOptions{
			CollectionID: t.req.GetCollectionID(),
			ExternalSpec: t.req.GetExternalSpec(),
			RowLimit:     targetRowsPerSegment,
		},
	)
}

func (t *RefreshExternalCollectionTask) PostExecute(ctx context.Context) error {
	if err := ensureContext(ctx); err != nil {
		return err
	}
	mlog.Info(ctx, "RefreshExternalCollectionTask PostExecute",
		mlog.Int64("taskID", t.req.GetTaskID()),
		mlog.Int64("collectionID", t.req.GetCollectionID()),
		mlog.Int("updatedSegments", len(t.updatedSegments)))
	return nil
}

// GetUpdatedSegments returns segments that DataCoord should upsert after execution.
// This includes patched same-ID current segments and newly created segments, but
// excludes unchanged kept segments.
func (t *RefreshExternalCollectionTask) GetUpdatedSegments() []*datapb.SegmentInfo {
	return t.updatedSegments
}

// GetKeptSegmentIDs returns IDs of current segments that were kept unchanged
func (t *RefreshExternalCollectionTask) GetKeptSegmentIDs() []int64 {
	return t.keptSegmentIDs
}

// fragmentKey identifies the L1 data fragment. Delete overlays are handled as
// manifest-only updates so L0 changes do not force a new target segment ID.
func fragmentKey(f packed.Fragment) string {
	return fmt.Sprintf("%s:%d:%d", f.FilePath, f.StartRow, f.EndRow)
}

// buildCurrentSegmentFragments builds segment to fragments mapping from current segments
func (t *RefreshExternalCollectionTask) buildCurrentSegmentFragments() (packed.SegmentFragments, error) {
	return packed.BuildCurrentSegmentFragments(t.req.GetCurrentSegments(), t.req.GetStorageConfig(), t.columns)
}

// organizeSegments compares fragments and organizes them into segments
func (t *RefreshExternalCollectionTask) organizeSegments(
	ctx context.Context,
	currentSegmentFragments packed.SegmentFragments,
	newFragments []packed.Fragment,
) ([]*datapb.SegmentInfo, error) {
	if err := ensureContext(ctx); err != nil {
		return nil, err
	}
	t.keptSegmentIDs = nil
	t.updatedSegments = nil

	// Build new fragment map using composite key (FilePath + StartRow + EndRow)
	// This is necessary because a single file can be split into multiple fragments
	// with different row ranges, and they must be tracked independently
	newFragmentMap := make(map[string]packed.Fragment)
	for _, f := range newFragments {
		key := fragmentKey(f)
		newFragmentMap[key] = f
	}

	// Track which fragments are used by kept segments (use composite key)
	usedFragments := make(map[string]bool)
	var keptSegments []*datapb.SegmentInfo
	var patchedSegments []*datapb.SegmentInfo

	var outputColumns []string
	if t.hasFunctions() {
		var err error
		outputColumns, err = functionOutputColumnNames(t.req.GetSchema())
		if err != nil {
			return nil, merr.Wrap(err, "resolve function output columns")
		}
	}

	// Check each current segment
	for _, seg := range t.req.GetCurrentSegments() {
		if err := ensureContext(ctx); err != nil {
			return nil, err
		}
		fragments := currentSegmentFragments[seg.GetID()]
		allFragmentsExist := true
		matchedNewFragments := make([]packed.Fragment, 0, len(fragments))

		// Check if all fragments of this segment still exist
		for _, f := range fragments {
			key := fragmentKey(f)
			newFragment, exists := newFragmentMap[key]
			if !exists {
				allFragmentsExist = false
				mlog.Info(context.TODO(), "Fragment removed from segment",
					mlog.Int64("segmentID", seg.GetID()),
					mlog.String("filePath", f.FilePath),
					mlog.Int64("startRow", f.StartRow),
					mlog.Int64("endRow", f.EndRow))
				break
			}
			matchedNewFragments = append(matchedNewFragments, newFragment)
		}

		if !allFragmentsExist {
			// Segment invalidated - its remaining fragments become orphans
			mlog.Info(context.TODO(), "Segment invalidated due to removed fragments",
				mlog.Int64("segmentID", seg.GetID()))
			continue
		}

		reusableSegment := true
		if len(outputColumns) > 0 {
			hasOutputs, err := t.segmentHasFunctionOutputColumns(seg, outputColumns)
			if err != nil {
				return nil, err
			}
			if !hasOutputs {
				reusableSegment = false
				mlog.Info(context.TODO(), "Segment invalidated due to missing function output columns",
					mlog.Int64("segmentID", seg.GetID()),
					mlog.String("manifestPath", seg.GetManifestPath()))
			}
		}

		if !reusableSegment {
			continue
		}

		missingColumns := missingExternalColumns(seg, t.req.GetSchema())
		shouldRefreshDeltalogs, err := t.shouldRefreshMilvusTableDeltalogs(seg, fragments, matchedNewFragments)
		if err != nil {
			return nil, err
		}
		var patchedSegment *datapb.SegmentInfo
		if shouldRefreshDeltalogs {
			updatedSegment, err := t.refreshMilvusTableSegmentManifest(ctx, seg, matchedNewFragments)
			if err != nil {
				return nil, err
			}
			patchedSegment = updatedSegment
			mlog.Info(context.TODO(), "Segment kept with refreshed milvus-table deltalogs",
				mlog.FieldSegmentID(seg.GetID()),
				mlog.String("oldManifestPath", seg.GetManifestPath()),
				mlog.String("newManifestPath", updatedSegment.GetManifestPath()))
		}

		if len(missingColumns) > 0 {
			segmentToPatch := seg
			patchFragments := fragments
			if patchedSegment != nil {
				segmentToPatch = patchedSegment
				patchFragments = matchedNewFragments
			}
			patchedWithColumns, err := t.patchSegmentForMissingColumns(ctx, segmentToPatch, patchFragments, missingColumns)
			if err != nil {
				return nil, err
			}
			patchedSegment = patchedWithColumns
			mlog.Info(context.TODO(), "Segment patched with missing external columns",
				mlog.FieldSegmentID(seg.GetID()),
				mlog.Strings("missingColumns", missingColumns))
		}

		for _, f := range fragments {
			if err := ensureContext(ctx); err != nil {
				return nil, err
			}
			key := fragmentKey(f)
			usedFragments[key] = true
		}
		if patchedSegment == nil {
			// Keep this segment unchanged
			keptSegments = append(keptSegments, seg)
			mlog.Debug(context.TODO(), "Segment kept unchanged",
				mlog.Int64("segmentID", seg.GetID()))
		} else {
			patchedSegments = append(patchedSegments, patchedSegment)
		}
	}

	// Collect orphan fragments (new + from invalidated segments)
	var orphanFragments []packed.Fragment
	for _, f := range newFragments {
		if err := ensureContext(ctx); err != nil {
			return nil, err
		}
		key := fragmentKey(f)
		if !usedFragments[key] {
			orphanFragments = append(orphanFragments, f)
		}
	}

	// Organize orphan fragments into new segments with balanced row counts
	createdSegments, err := t.balanceFragmentsToSegments(ctx, orphanFragments)
	if err != nil {
		return nil, err
	}

	// Track kept vs new separately for correct response building
	keptSegmentIDs := make([]int64, 0, len(keptSegments))
	for _, seg := range keptSegments {
		keptSegmentIDs = append(keptSegmentIDs, seg.GetID())
	}
	updatedSegments := append(patchedSegments, createdSegments...)
	t.keptSegmentIDs = keptSegmentIDs
	t.updatedSegments = updatedSegments

	// Visible result contains unchanged kept segments plus upsert segments.
	result := append(keptSegments, updatedSegments...)

	mlog.Info(context.TODO(), "Segment organization complete",
		mlog.Int("keptSegments", len(keptSegments)),
		mlog.Int("newSegments", len(createdSegments)),
		mlog.Int("totalSegments", len(result)))

	return result, nil
}

func (t *RefreshExternalCollectionTask) segmentHasFunctionOutputColumns(seg *datapb.SegmentInfo, outputColumns []string) (bool, error) {
	if len(outputColumns) == 0 {
		return true, nil
	}
	if segmentChildFieldsContainColumns(seg, outputColumns) {
		return true, nil
	}
	if seg.GetManifestPath() == "" {
		return false, nil
	}
	hasColumns, err := packed.ManifestHasColumns(seg.GetManifestPath(), t.req.GetStorageConfig(), outputColumns)
	if err != nil {
		return false, merr.Wrapf(err, "check function output columns for segment %d", seg.GetID())
	}
	return hasColumns, nil
}

func segmentChildFieldsContainColumns(seg *datapb.SegmentInfo, columns []string) bool {
	required := make(map[int64]struct{}, len(columns))
	for _, column := range columns {
		fieldID, err := strconv.ParseInt(column, 10, 64)
		if err != nil {
			return false
		}
		required[fieldID] = struct{}{}
	}

	seen := make(map[int64]struct{}, len(required))
	for _, binlog := range seg.GetBinlogs() {
		for _, fieldID := range binlog.GetChildFields() {
			if _, ok := required[fieldID]; ok {
				seen[fieldID] = struct{}{}
			}
		}
	}
	return len(seen) == len(required)
}

func functionOutputColumnNames(schema *schemapb.CollectionSchema) ([]string, error) {
	outputFields, err := functionOutputFields(schema)
	if err != nil {
		return nil, err
	}
	columns := make([]string, 0, len(outputFields))
	for _, field := range outputFields {
		columns = append(columns, strconv.FormatInt(field.GetFieldID(), 10))
	}
	return columns, nil
}

func targetExternalFields(schema *schemapb.CollectionSchema) map[int64]string {
	result := make(map[int64]string)
	if schema == nil {
		return result
	}
	for _, field := range schema.GetFields() {
		if field.GetExternalField() == "" {
			continue
		}
		result[field.GetFieldID()] = field.GetExternalField()
	}
	return result
}

func coveredFieldsFromChildFields(seg *datapb.SegmentInfo) map[int64]struct{} {
	result := make(map[int64]struct{})
	for _, fieldBinlog := range seg.GetBinlogs() {
		childFields := fieldBinlog.GetChildFields()
		if len(childFields) == 0 && fieldBinlog.GetFieldID() > 0 {
			result[fieldBinlog.GetFieldID()] = struct{}{}
			continue
		}
		for _, fieldID := range childFields {
			result[fieldID] = struct{}{}
		}
	}
	return result
}

func missingExternalColumns(seg *datapb.SegmentInfo, schema *schemapb.CollectionSchema) []string {
	if schema == nil {
		return nil
	}
	targetFields := targetExternalFields(schema)
	coveredFields := coveredFieldsFromChildFields(seg)
	var missingColumns []string
	for _, field := range schema.GetFields() {
		externalField, ok := targetFields[field.GetFieldID()]
		if !ok {
			continue
		}
		if _, ok := coveredFields[field.GetFieldID()]; !ok {
			missingColumns = append(missingColumns, externalField)
		}
	}
	return missingColumns
}

func (t *RefreshExternalCollectionTask) patchSegmentForMissingColumns(
	ctx context.Context,
	seg *datapb.SegmentInfo,
	fragments []packed.Fragment,
	missingColumns []string,
) (*datapb.SegmentInfo, error) {
	schema := t.req.GetSchema()
	newManifestPath, err := packed.AppendSegmentManifestColumns(
		ctx,
		seg.GetManifestPath(),
		t.parsedSpec.Format,
		missingColumns,
		fragments,
		t.req.GetStorageConfig(),
	)
	if err != nil {
		return nil, merr.Wrapf(err, "failed to append manifest columns for segment %d", seg.GetID())
	}

	sampleRows := paramtable.Get().QueryNodeCfg.ExternalCollectionSampleRows.GetAsInt()
	if int64(sampleRows) > seg.GetNumOfRows() {
		sampleRows = int(seg.GetNumOfRows())
	}
	fieldSizes, err := packed.SampleExternalFieldSizes(
		newManifestPath,
		sampleRows,
		t.req.GetCollectionID(),
		t.req.GetExternalSource(),
		t.req.GetExternalSpec(),
		schema,
		t.req.GetStorageConfig(),
	)
	if err != nil {
		return nil, merr.Wrapf(err, "failed to sample external field sizes for segment %d", seg.GetID())
	}
	externalAvgBytes := sumFieldSizes(fieldSizes, schema)
	if externalAvgBytes <= 0 {
		return nil, merr.WrapErrParameterInvalidMsg(
			fmt.Sprintf("external field size sample for segment %d produced non-positive average size %d", seg.GetID(), externalAvgBytes))
	}
	functionOutputAvgBytes, err := estimateFunctionOutputBytesPerRow(schema)
	if err != nil {
		return nil, err
	}
	memorySize := (externalAvgBytes + functionOutputAvgBytes) * seg.GetNumOfRows()
	if memorySize <= 0 {
		return nil, merr.WrapErrParameterInvalidMsg(
			fmt.Sprintf("external field size sample for segment %d produced non-positive memory size %d", seg.GetID(), memorySize))
	}

	patched := proto.Clone(seg).(*datapb.SegmentInfo)
	patched.ManifestPath = newManifestPath
	patched.SchemaVersion = schema.GetVersion()
	patched.StorageVersion = storage.StorageV3
	patched.Binlogs = buildFakeBinlogs(seg.GetID(), seg.GetNumOfRows(), memorySize, schema, t.parsedSpec.Format)
	return patched, nil
}

// balanceFragmentsToSegments organizes fragments into segments with balanced row counts
func (t *RefreshExternalCollectionTask) balanceFragmentsToSegments(ctx context.Context, fragments []packed.Fragment) ([]*datapb.SegmentInfo, error) {
	if len(fragments) == 0 {
		return nil, nil
	}
	if err := ensureContext(ctx); err != nil {
		return nil, err
	}

	// Calculate total rows
	var totalRows int64
	for _, f := range fragments {
		if err := ensureContext(ctx); err != nil {
			return nil, err
		}
		totalRows += f.RowCount
	}

	// Guard against zero-row parquet files that would cause divide-by-zero below.
	// Fragment-level RowCount comes from manifest (endRow - startRow) and reflects real row counts,
	// unlike datacoord's pre-split FileInfo.NumRows which carries a -1 sentinel from PlainFormat::explore.
	if totalRows == 0 {
		return nil, merr.WrapErrParameterInvalidMsg(
			fmt.Sprintf("external source has %d fragments but zero total rows", len(fragments)))
	}

	// Phase 1: Allocate segment IDs (sequential, lightweight)
	type segmentWork struct {
		segmentID   int64
		binlogLogID int64
		rowCount    int64
		fragments   []packed.Fragment
	}
	var works []segmentWork
	isMilvusTableVirtualPKTask := t.parsedSpec != nil &&
		t.parsedSpec.Format == externalspec.FormatMilvusTable &&
		!packed.HasExternalPrimaryKey(t.req.GetSchema())
	isMilvusTableTask := t.parsedSpec != nil && t.parsedSpec.Format == externalspec.FormatMilvusTable
	appendWork := func(rowCount int64, fragments []packed.Fragment) error {
		// Each segment needs 2 IDs: one for segment, one for fake binlog logID
		if t.nextAllocID+1 >= t.preallocatedIDRange.End {
			return merr.WrapErrParameterInvalidMsg("insufficient pre-allocated IDs: need 2 more but only have %d IDs in range [%d, %d)",
				t.preallocatedIDRange.End-t.nextAllocID,
				t.preallocatedIDRange.Begin,
				t.preallocatedIDRange.End)
		}
		segmentID := t.nextAllocID
		binlogLogID := t.nextAllocID + 1
		t.nextAllocID += 2
		workFragments := fragments
		if t.parsedSpec != nil && t.parsedSpec.Format == externalspec.FormatMilvusTable {
			var err error
			workFragments, err = t.prepareMilvusTableDeltalogFragments(fragments)
			if err != nil {
				return err
			}
		}
		works = append(works, segmentWork{
			segmentID:   segmentID,
			binlogLogID: binlogLogID,
			rowCount:    rowCount,
			fragments:   workFragments,
		})
		return nil
	}
	if isMilvusTableTask {
		// milvus-table maps each source fragment to exactly one target segment
		// (1:1) instead of bin-packing fragments by row count. This keeps the
		// target segment aligned with a single source segment manifest, which is
		// what makes manifest reuse and the virtual-PK offset mapping well defined.
		mlog.Info(context.TODO(), "Assigning milvus-table fragments to one segment each",
			mlog.Int("numFragments", len(fragments)),
			mlog.Int64("totalRows", totalRows),
			mlog.Int("numSegments", len(fragments)))
		for _, fragment := range fragments {
			if err := appendWork(fragment.RowCount, []packed.Fragment{fragment}); err != nil {
				return nil, err
			}
		}
	} else {
		// Get target rows per segment from configuration
		targetRowsPerSegment := paramtable.Get().DataNodeCfg.ExternalCollectionTargetRowsPerSegment.GetAsInt64()
		if totalRows < targetRowsPerSegment {
			targetRowsPerSegment = totalRows
		}

		numSegments := (totalRows + targetRowsPerSegment - 1) / targetRowsPerSegment
		if numSegments == 0 {
			numSegments = 1
		}

		avgRowsPerSegment := totalRows / numSegments

		mlog.Info(context.TODO(), "Balancing fragments to segments",
			mlog.Int("numFragments", len(fragments)),
			mlog.Int64("totalRows", totalRows),
			mlog.Int64("numSegments", numSegments),
			mlog.Int64("avgRowsPerSegment", avgRowsPerSegment))

		// Sort fragments by row count descending for better bin-packing
		sortedFragments := make([]packed.Fragment, len(fragments))
		copy(sortedFragments, fragments)
		sort.Slice(sortedFragments, func(i, j int) bool {
			return sortedFragments[i].RowCount > sortedFragments[j].RowCount
		})

		// Initialize segment bins
		type segmentBin struct {
			fragments []packed.Fragment
			rowCount  int64
		}
		bins := make([]segmentBin, numSegments)

		// Greedy bin-packing: assign each fragment to the bin with lowest current row count
		for _, f := range sortedFragments {
			if err := ensureContext(ctx); err != nil {
				return nil, err
			}
			// Find bin with minimum row count
			minIdx := 0
			for i := 1; i < len(bins); i++ {
				if bins[i].rowCount < bins[minIdx].rowCount {
					minIdx = i
				}
			}
			bins[minIdx].fragments = append(bins[minIdx].fragments, f)
			bins[minIdx].rowCount += f.RowCount
		}

		for _, bin := range bins {
			if len(bin.fragments) == 0 {
				continue
			}
			if err := appendWork(bin.rowCount, bin.fragments); err != nil {
				return nil, err
			}
		}
	}

	mlog.Info(context.TODO(), "Allocated segment IDs, starting manifest creation",
		mlog.Int("numSegments", len(works)))

	// Phase 2: Create manifests concurrently with a fixed-size worker pool.
	const createManifestWorkers = 16
	const createManifestVirtualPKWorkers = 4
	manifestStart := time.Now()

	workerLimit := createManifestWorkers
	if isMilvusTableVirtualPKTask {
		workerLimit = createManifestVirtualPKWorkers
	}
	workers := workerLimit
	if workers > len(works) {
		workers = len(works)
	}
	pool := conc.NewPool[string](workers)
	defer pool.Release()

	manifestPaths := make([]string, len(works))
	futures := make([]*conc.Future[string], len(works))
	for i := range works {
		i, work := i, works[i]
		futures[i] = pool.Submit(func() (string, error) {
			// Honor ctx cancellation so workers bail out quickly once the
			// caller gives up instead of running createManifestForSegment
			// for every still-queued segment.
			if err := ctx.Err(); err != nil {
				return "", err
			}
			var manifestPath string
			var err error
			if t.hasFunctions() {
				manifestPath, err = t.createManifestWithFunctions(ctx, work.segmentID, work.fragments)
			} else {
				manifestPath, err = t.createManifestForSegment(ctx, work.segmentID, work.fragments)
			}
			if err != nil {
				return "", merr.Wrapf(err, "failed to create manifest for segment %d", work.segmentID)
			}
			return manifestPath, nil
		})
	}

	// Wait for all futures; record the first error (if any) but still wait
	// for every future to finish so no goroutine is left running.
	var firstErr error
	for i, f := range futures {
		path, err := f.Await()
		if err != nil && firstErr == nil {
			firstErr = err
		}
		if err == nil {
			manifestPaths[i] = path
		}
	}

	manifestDuration := time.Since(manifestStart)
	mlog.Info(context.TODO(), "CreateManifest phase completed",
		mlog.Int("numSegments", len(works)),
		mlog.Int("workers", workers),
		mlog.Duration("duration", manifestDuration))

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if firstErr != nil {
		return nil, firstErr
	}

	// Phase 3: Sample field sizes so that downstream code (QueryNode memory
	// estimation) can use Binlog.MemorySize directly, eliminating the need
	// for a separate Take-sampling step at load time. Two modes:
	//   - samplePerSegment=false (default): sample only the first segment
	//     and reuse that average for all segments in this task. Cheap, but
	//     can be off when row density varies across files.
	//   - samplePerSegment=true: sample each segment independently. More
	//     I/O, but per-segment MemorySize is accurate.
	//
	// If a later segment's sampling fails, we fall back to the first
	// successful avgBytesPerRow so MemorySize is never written as 0 — a
	// zero MemorySize would make QueryNode's memory estimation collapse
	// and risk OOM on load. If ALL samples fail (or return a 0 sum because
	// the schema has no ExternalField-mapped fields, or the sampled rows
	// somehow evaluate to 0 bytes), we fail the task rather than silently
	// producing zero-sized segments that would skew QN's resource estimator
	// and risk OOM on load.
	samplePerSegment := paramtable.Get().QueryNodeCfg.ExternalCollectionSamplePerSegment.GetAsBool()
	sampleRows := paramtable.Get().QueryNodeCfg.ExternalCollectionSampleRows.GetAsInt()
	segmentAvgBytes := make([]int64, len(works))
	var fallbackAvg int64
	functionOutputAvgBytes, err := estimateFunctionOutputBytesPerRow(t.req.GetSchema())
	if err != nil {
		return nil, err
	}

	// firstSampleErr captures the first underlying sampling failure so we
	// can surface the real root cause (e.g. "Column 'xxx' not found in
	// schema" from an external_field typo, issue #48637) to the client
	// instead of a generic "sampling failed for all N segment(s)" message.
	var firstSampleErr error
	recordErr := func(err error) {
		if firstSampleErr == nil {
			firstSampleErr = err
		}
	}

	sampleOne := func(manifestPath string) (int64, bool) {
		fieldSizes, err := packed.SampleExternalFieldSizes(
			manifestPath, sampleRows,
			t.req.GetCollectionID(),
			t.req.GetExternalSource(),
			t.req.GetExternalSpec(),
			t.req.GetSchema(),
			t.req.GetStorageConfig(),
		)
		if err != nil {
			mlog.Warn(context.TODO(), "failed to sample external field sizes",
				mlog.String("manifestPath", manifestPath),
				mlog.Err(err))
			recordErr(err)
			return 0, false
		}
		total := sumFieldSizes(fieldSizes, t.req.GetSchema())
		if total <= 0 {
			// A non-positive total is treated as a sample failure so the
			// caller can decide whether to fall back or fail the task. The
			// zero can come from (a) a schema with no ExternalField-mapped
			// fields, or (b) a Parquet file whose sampled rows really are
			// empty — both are degenerate and must not feed QN a zero.
			mlog.Warn(context.TODO(), "external field size sample produced non-positive total",
				mlog.String("manifestPath", manifestPath),
				mlog.Int64("total", total))
			recordErr(merr.WrapErrParameterInvalidMsg("sampled field sizes sum to %d (schema may have no external_field mappings, or sampled rows are empty)", total))
			return 0, false
		}
		return total, true
	}

	if len(manifestPaths) > 0 {
		if samplePerSegment {
			for i, mp := range manifestPaths {
				if avg, ok := sampleOne(mp); ok {
					segmentAvgBytes[i] = avg
					if fallbackAvg == 0 {
						fallbackAvg = avg
					}
				}
			}
			mlog.Info(context.TODO(), "per-segment sampling complete",
				mlog.Int("numSegments", len(manifestPaths)),
				mlog.Int64("fallbackAvgBytesPerRow", fallbackAvg))
		} else {
			if avg, ok := sampleOne(manifestPaths[0]); ok {
				fallbackAvg = avg
				for i := range segmentAvgBytes {
					segmentAvgBytes[i] = avg
				}
			}
			mlog.Info(context.TODO(), "single-sample complete",
				mlog.Int64("avgBytesPerRow", fallbackAvg))
		}
		// If every sample failed, fail the task rather than emitting
		// zero-MemorySize fake binlogs that would collapse QueryNode's
		// resource estimator and risk OOM on load. We prefer a loud
		// failure (retry-eligible via the task state machine) over a
		// silent success that corrupts downstream accounting.
		//
		// Embed firstSampleErr (issue #48637) so the client-facing
		// RefreshFailed reason names the root cause (e.g. column not
		// found in parquet) rather than forcing the operator to SSH
		// into datanode logs.
		if fallbackAvg == 0 {
			rootCause := "unknown (no sample attempted)"
			if firstSampleErr != nil {
				rootCause = firstSampleErr.Error()
			}
			hint := ""
			if strings.Contains(rootCause, "not found in schema") {
				hint = "; check external_field mappings in collection schema against actual parquet columns"
			}
			return nil, merr.WrapErrParameterInvalidMsg(
				"external field size sampling failed for all %d segment(s): %s%s",
				len(manifestPaths), rootCause, hint)
		}
		// Fill any zero slots (sampling failed mid-loop) with the first
		// successful average so every segment gets a non-zero MemorySize.
		for i, v := range segmentAvgBytes {
			if v == 0 {
				segmentAvgBytes[i] = fallbackAvg
			}
		}
	}

	// Phase 4: Build result and mappings (sequential, lightweight)
	result := make([]*datapb.SegmentInfo, 0, len(works))
	for i, work := range works {
		memorySize := (segmentAvgBytes[i] + functionOutputAvgBytes) * work.rowCount
		seg := &datapb.SegmentInfo{
			ID:             work.segmentID,
			CollectionID:   t.req.GetCollectionID(),
			PartitionID:    t.req.GetPartitionID(),
			NumOfRows:      work.rowCount,
			ManifestPath:   manifestPaths[i],
			StorageVersion: storage.StorageV3,
			Level:          datapb.SegmentLevel_L1,
			// Fake binlog so downstream treats external segments like normal
			// StorageV3 segments. MemorySize is pre-computed from Take sampling
			// so QueryNode skips the external-specific sampling path.
			Binlogs: buildFakeBinlogs(work.binlogLogID, work.rowCount, memorySize, t.req.GetSchema(), t.parsedSpec.Format),
		}
		result = append(result, seg)
	}

	return result, nil
}

// createManifestForSegment creates a manifest file for the segment
func (t *RefreshExternalCollectionTask) createManifestForSegment(
	ctx context.Context,
	segmentID int64,
	fragments []packed.Fragment,
) (string, error) {
	// All segments now use final paths with real IDs (no temporary paths needed)
	// Pre-allocated IDs ensure we can write directly to final locations
	basePath := segmentInsertLogBasePath(
		t.req.GetStorageConfig(),
		t.req.GetCollectionID(),
		t.req.GetPartitionID(),
		segmentID,
	)

	manifestPath, err := packed.CreateSegmentManifestWithBasePathAndExtfs(
		ctx,
		basePath,
		t.parsedSpec.Format,
		t.columns,
		fragments,
		t.req.GetStorageConfig(),
		packed.ExternalSpecContext{
			CollectionID:      t.req.GetCollectionID(),
			Source:            t.req.GetExternalSource(),
			Spec:              t.req.GetExternalSpec(),
			MilvusTablePKMode: packed.MilvusTablePrimaryKeyModeFromSchema(t.req.GetSchema()),
		},
	)
	if err != nil {
		return "", err
	}
	if t.parsedSpec.Format == externalspec.FormatMilvusTable {
		return t.postProcessMilvusTableDeltalogs(ctx, basePath, manifestPath, segmentID, fragments)
	}
	return manifestPath, nil
}

func segmentInsertLogBasePath(
	storageConfig *indexpb.StorageConfig,
	collectionID int64,
	partitionID int64,
	segmentID int64,
) string {
	rootPath := ""
	if storageConfig != nil {
		rootPath = storageConfig.GetRootPath()
	}
	return path.Join(rootPath, common.SegmentInsertLogPath, metautil.JoinIDPath(collectionID, partitionID, segmentID))
}

// hasFunctions returns true if the schema defines any functions.
func (t *RefreshExternalCollectionTask) hasFunctions() bool {
	return len(t.req.GetSchema().GetFunctions()) > 0
}

// createManifestWithFunctions builds an input manifest from the segment's
// fragments, runs schema functions, and appends a function-output column group
// on top. External input files are referenced, never copied.
func (t *RefreshExternalCollectionTask) createManifestWithFunctions(
	ctx context.Context,
	segmentID int64,
	fragments []packed.Fragment,
) (string, error) {
	clusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()
	basePath := segmentInsertLogBasePath(
		t.req.GetStorageConfig(),
		t.req.GetCollectionID(),
		t.req.GetPartitionID(),
		segmentID,
	)

	manifestPath, err := ExecuteFunctionsForSegment(
		ctx,
		t.req.GetSchema(),
		fragments,
		t.parsedSpec.Format,
		t.req.GetStorageConfig(),
		t.req.GetCollectionID(),
		segmentID,
		basePath,
		clusterID,
	)
	if err != nil {
		return "", err
	}
	if t.parsedSpec.Format == externalspec.FormatMilvusTable {
		return t.postProcessMilvusTableDeltalogs(ctx, basePath, manifestPath, segmentID, fragments)
	}
	return manifestPath, nil
}

// buildFakeBinlogs creates a synthetic FieldBinlog slice for an external segment.
// It uses DefaultShortColumnGroupID (0) to match StorageV3 convention, so that
// downstream code (row count calculation, index association, memory estimation)
// treats external segments the same as normal packed segments.
// ChildFields must list all field IDs so that QueryNode can resolve field schemas.
func buildFakeBinlogs(logID, numRows, memorySize int64, schema *schemapb.CollectionSchema, format string) []*datapb.FieldBinlog {
	var childFields []int64
	if schema != nil {
		for _, field := range schema.GetFields() {
			childFields = append(childFields, field.GetFieldID())
		}
	}
	return []*datapb.FieldBinlog{
		{
			FieldID:     int64(storagecommon.DefaultShortColumnGroupID),
			ChildFields: childFields,
			Format:      format,
			Binlogs: []*datapb.Binlog{
				{
					LogID:      logID,
					EntriesNum: numRows,
					MemorySize: memorySize,
					LogSize:    memorySize, // conservative: assume no compression
				},
			},
		},
	}
}

// estimateFunctionOutputBytesPerRow computes the per-row memory estimate for
// fields generated during refresh. These fields are not present in external
// source samples, so add them explicitly to the fake binlog memory size.
func estimateFunctionOutputBytesPerRow(schema *schemapb.CollectionSchema) (int64, error) {
	var total int64
	outputFields, err := functionOutputFields(schema)
	if err != nil {
		return 0, err
	}
	for _, field := range outputFields {
		size, err := typeutil.EstimateSizePerRecord(&schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{field},
		})
		if err != nil {
			return 0, merr.Wrapf(err, "estimate function output field %s", field.GetName())
		}
		total += int64(size)
	}
	return total, nil
}

// sumFieldSizes computes total avgBytesPerRow from per-field sampling results.
func sumFieldSizes(fieldSizes map[string]int64, schema *schemapb.CollectionSchema) int64 {
	if schema == nil {
		var total int64
		for _, v := range fieldSizes {
			total += v
		}
		return total
	}
	columnResolver := typeutil.NewStorageColumnResolver(schema)
	var total int64
	for _, field := range schema.GetFields() {
		columnName, ok := columnResolver.SourceDataColumnName(field)
		if !ok {
			continue
		}
		if avgBytes, ok := fieldSizes[columnName]; ok && avgBytes > 0 {
			total += avgBytes
		}
	}
	if columnResolver.IsMilvusTable() && packed.HasExternalPrimaryKey(schema) {
		timestampColumn := strconv.FormatInt(common.TimeStampField, 10)
		if avgBytes, ok := fieldSizes[timestampColumn]; ok && avgBytes > 0 {
			total += avgBytes
		}
	}
	return total
}
