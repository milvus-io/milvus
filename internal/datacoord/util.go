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

package datacoord

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/samber/lo"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/internal/util/vecindexmgr"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Response response interface for verification
type Response interface {
	GetStatus() *commonpb.Status
}

// VerifyResponse verify grpc Response 1. check error is nil 2. check response.GetStatus() with status success
func VerifyResponse(response interface{}, err error) error {
	if err != nil {
		return err
	}
	if response == nil {
		return errNilResponse
	}
	switch resp := response.(type) {
	case Response:
		// note that resp will not be nil here, since it's still an interface
		if resp.GetStatus() == nil {
			return errNilStatusResponse
		}
		return merr.Error(resp.GetStatus())

	case *commonpb.Status:
		if resp == nil {
			return errNilResponse
		}
		return merr.Error(resp)
	default:
		return errUnknownResponseType
	}
}

func FilterInIndexedSegments(ctx context.Context, handler Handler, mt *meta, skipNoIndexCollection bool, segments ...*SegmentInfo) []*SegmentInfo {
	if len(segments) == 0 {
		return nil
	}

	if ctx.Err() != nil {
		return nil
	}

	collectionSegments := lo.GroupBy(segments, func(segment *SegmentInfo) int64 {
		return segment.GetCollectionID()
	})

	ret := make([]*SegmentInfo, 0)
	for collection, segmentList := range collectionSegments {
		// No segments will be filtered if there are no indices in the collection.
		if skipNoIndexCollection && !mt.indexMeta.HasIndex(collection) {
			ret = append(ret, segmentList...)
			continue
		}

		timeoutCtx, cancel := context.WithTimeout(ctx, time.Second*2)

		coll, err := handler.GetCollection(timeoutCtx, collection)
		cancel()
		if err != nil {
			mlog.Warn(ctx, "failed to get collection schema", mlog.Err(err))
			continue
		}

		// get vector field id
		var targetFieldIds []int64
		// wait all vector datatype fields only
		for _, field := range coll.Schema.GetFields() {
			if typeutil.IsVectorType(field.GetDataType()) {
				targetFieldIds = append(targetFieldIds, field.GetFieldID())
			}
		}

		// include all scalar fields with index
		if paramtable.Get().DataCoordCfg.DVForceAllIndexReady.GetAsBool() {
			indices := mt.indexMeta.GetIndexesForCollection(collection, "")
			for _, index := range indices {
				targetFieldIds = append(targetFieldIds, index.FieldID)
			}
		}
		segmentIDs := lo.Map(segmentList, func(seg *SegmentInfo, _ int) UniqueID {
			return seg.GetID()
		})

		// get indexed segments which finish build index on all vector field
		indexed := mt.indexMeta.GetIndexedSegments(collection, segmentIDs, targetFieldIds)
		if len(indexed) > 0 {
			indexedSet := typeutil.NewUniqueSet(indexed...)
			for _, segment := range segmentList {
				if !isFlushState(segment.GetState()) && segment.GetState() != commonpb.SegmentState_Dropped {
					continue
				}

				if indexedSet.Contain(segment.GetID()) {
					ret = append(ret, segment)
				}
			}
		}
	}

	return ret
}

func getZeroTime() time.Time {
	var t time.Time
	return t
}

func UpdateCompactionSegmentSizeMetrics(segments []*datapb.CompactionSegment) {
	var totalSize int64
	for _, seg := range segments {
		totalSize += getCompactedSegmentSize(seg)
	}
	// observe size in bytes
	metrics.DataCoordCompactedSegmentSize.WithLabelValues().Observe(float64(totalSize))
}

func getCompactedSegmentSize(s *datapb.CompactionSegment) int64 {
	var segmentSize int64
	if s != nil {
		for _, binlogs := range s.GetInsertLogs() {
			for _, l := range binlogs.GetBinlogs() {
				segmentSize += l.GetMemorySize()
			}
		}

		for _, deltaLogs := range s.GetDeltalogs() {
			for _, l := range deltaLogs.GetBinlogs() {
				segmentSize += l.GetMemorySize()
			}
		}

		for _, statsLogs := range s.GetField2StatslogPaths() {
			for _, l := range statsLogs.GetBinlogs() {
				segmentSize += l.GetMemorySize()
			}
		}
	}

	return segmentSize
}

// getCollectionAutoCompactionEnabled returns whether auto compaction for collection is enabled.
// if not set, returns global auto compaction config.
func getCollectionAutoCompactionEnabled(properties map[string]string) (bool, error) {
	// when collection is on truncating, disable auto compaction.
	if _, ok := properties[common.CollectionOnTruncatingKey]; ok {
		return false, nil
	}
	v, ok := properties[common.CollectionAutoCompactionKey]
	if ok {
		enabled, err := strconv.ParseBool(v)
		if err != nil {
			return false, err
		}
		return enabled, nil
	}
	return Params.DataCoordCfg.EnableAutoCompaction.GetAsBool(), nil
}

func GetIndexType(indexParams []*commonpb.KeyValuePair) string {
	for _, param := range indexParams {
		if param.Key == common.IndexTypeKey {
			return param.Value
		}
	}
	return invalidIndex
}

func isNoTrainIndex(indexType string) bool {
	return vecindexmgr.GetVecIndexMgrInstance().IsNoTrainIndex(indexType)
}

func isMvSupported(indexType string) bool {
	return vecindexmgr.GetVecIndexMgrInstance().IsMvSupported(indexType)
}

func isDiskANNIndex(indexType string) bool {
	return vecindexmgr.GetVecIndexMgrInstance().IsDiskANN(indexType)
}

func parseBuildIDFromFilePath(key string) (UniqueID, error) {
	ss := strings.Split(key, "/")
	if strings.HasSuffix(key, "/") {
		return strconv.ParseInt(ss[len(ss)-2], 10, 64)
	}
	return strconv.ParseInt(ss[len(ss)-1], 10, 64)
}

func getFieldBinlogs(id UniqueID, binlogs []*datapb.FieldBinlog) *datapb.FieldBinlog {
	for _, binlog := range binlogs {
		if id == binlog.GetFieldID() {
			return binlog
		}
	}
	return nil
}

func mergeFieldBinlogs(currentBinlogs []*datapb.FieldBinlog, newBinlogs []*datapb.FieldBinlog) []*datapb.FieldBinlog {
	for _, newBinlog := range newBinlogs {
		fieldBinlogs := getFieldBinlogs(newBinlog.GetFieldID(), currentBinlogs)
		if fieldBinlogs == nil {
			currentBinlogs = append(currentBinlogs, newBinlog)
		} else {
			if len(fieldBinlogs.ChildFields) == 0 {
				fieldBinlogs.ChildFields = newBinlog.GetChildFields()
			}
			if fieldBinlogs.Format == "" {
				fieldBinlogs.Format = newBinlog.GetFormat()
			}
			fieldBinlogs.Binlogs = append(fieldBinlogs.Binlogs, newBinlog.Binlogs...)
		}
	}
	return currentBinlogs
}

// filterDuplicateFieldBinlogs removes FieldBinlog entries from newLogs whose (fieldID, logID)
// pairs already exist in existingLogs. Used to make crash-replay idempotent when the same
// set of binlog results may be applied twice (e.g. backfill task completion after a datacoord
// restart between the etcd write and the task state transition).
func filterDuplicateFieldBinlogs(existingLogs, newLogs []*datapb.FieldBinlog) []*datapb.FieldBinlog {
	if len(existingLogs) == 0 || len(newLogs) == 0 {
		return newLogs
	}
	existing := make(map[int64]map[int64]struct{}, len(existingLogs))
	for _, fb := range existingLogs {
		logIDs, ok := existing[fb.GetFieldID()]
		if !ok {
			logIDs = make(map[int64]struct{})
			existing[fb.GetFieldID()] = logIDs
		}
		for _, b := range fb.GetBinlogs() {
			logIDs[b.GetLogID()] = struct{}{}
		}
	}
	result := make([]*datapb.FieldBinlog, 0, len(newLogs))
	for _, fb := range newLogs {
		existingSet, hasField := existing[fb.GetFieldID()]
		if !hasField {
			result = append(result, fb)
			continue
		}
		filteredBinlogs := make([]*datapb.Binlog, 0, len(fb.GetBinlogs()))
		for _, b := range fb.GetBinlogs() {
			if _, dup := existingSet[b.GetLogID()]; !dup {
				filteredBinlogs = append(filteredBinlogs, b)
			}
		}
		if len(filteredBinlogs) > 0 {
			result = append(result, &datapb.FieldBinlog{
				FieldID:     fb.GetFieldID(),
				ChildFields: fb.GetChildFields(),
				Format:      fb.GetFormat(),
				Binlogs:     filteredBinlogs,
			})
		}
	}
	return result
}

func calculateL0SegmentSize(fields []*datapb.FieldBinlog) float64 {
	size := int64(0)
	for _, field := range fields {
		for _, binlog := range field.GetBinlogs() {
			size += binlog.GetMemorySize()
		}
	}
	return float64(size)
}

func getCompactionMergeInfo(task *datapb.CompactionTask) *milvuspb.CompactionMergeInfo {
	/*
		segments := task.GetPlan().GetSegmentBinlogs()
		var sources []int64
		for _, s := range segments {
			sources = append(sources, s.GetSegmentID())
		}
	*/
	var target int64 = -1
	if len(task.GetResultSegments()) > 0 {
		target = task.GetResultSegments()[0]
	}
	return &milvuspb.CompactionMergeInfo{
		Sources: task.GetInputSegments(),
		Target:  target,
	}
}

func getBinLogIDs(segment *SegmentInfo, fieldID int64) []int64 {
	binlogIDs := make([]int64, 0)
	for _, fieldBinLog := range segment.GetBinlogs() {
		if fieldBinLog.GetFieldID() == fieldID {
			for _, binLog := range fieldBinLog.GetBinlogs() {
				binlogIDs = append(binlogIDs, binLog.GetLogID())
			}
			break
		}
	}
	return binlogIDs
}

func getTotalBinlogRows(segment *SegmentInfo, fieldID int64) int64 {
	var total int64
	for _, fieldBinLog := range segment.GetBinlogs() {
		if fieldBinLog.GetFieldID() == fieldID {
			for _, binLog := range fieldBinLog.GetBinlogs() {
				total += binLog.EntriesNum
			}
		}
	}
	return total
}

func CheckCheckPointsHealth(meta *meta) error {
	for channel, cp := range meta.GetChannelCheckpoints() {
		collectionID := funcutil.GetCollectionIDFromVChannel(channel)
		if collectionID == -1 {
			mlog.RatedWarn(context.TODO(), rate.Limit(60), "can't parse collection id from vchannel, skip check cp lag", mlog.FieldVChannel(channel))
			continue
		}
		if meta.GetCollection(collectionID) == nil {
			mlog.RatedWarn(context.TODO(), rate.Limit(60), "corresponding the collection doesn't exists, skip check cp lag", mlog.FieldVChannel(channel))
			continue
		}
		ts, _ := tsoutil.ParseTS(cp.Timestamp)
		lag := time.Since(ts)
		if lag > paramtable.Get().DataCoordCfg.ChannelCheckpointMaxLag.GetAsDuration(time.Second) {
			return merr.WrapErrChannelCPExceededMaxLag(channel, fmt.Sprintf("checkpoint lag: %f(min)", lag.Minutes()))
		}
	}
	return nil
}

func createStorageConfig() *indexpb.StorageConfig {
	var storageConfig *indexpb.StorageConfig

	if Params.CommonCfg.StorageType.GetValue() == "local" {
		storageConfig = &indexpb.StorageConfig{
			RootPath:    Params.LocalStorageCfg.Path.GetValue(),
			StorageType: Params.CommonCfg.StorageType.GetValue(),
			// External collections may reference an s3:// source even when the
			// primary storage is local, so the connection cap still applies.
			MaxConnections: uint32(Params.MinioCfg.MaxConnections.GetAsInt()),
		}
	} else {
		storageConfig = &indexpb.StorageConfig{
			Address:           Params.MinioCfg.Address.GetValue(),
			AccessKeyID:       Params.MinioCfg.AccessKeyID.GetValue(),
			SecretAccessKey:   Params.MinioCfg.SecretAccessKey.GetValue(),
			UseSSL:            Params.MinioCfg.UseSSL.GetAsBool(),
			SslCACert:         Params.MinioCfg.SslCACert.GetValue(),
			BucketName:        Params.MinioCfg.BucketName.GetValue(),
			RootPath:          Params.MinioCfg.RootPath.GetValue(),
			UseIAM:            Params.MinioCfg.UseIAM.GetAsBool(),
			IAMEndpoint:       Params.MinioCfg.IAMEndpoint.GetValue(),
			StorageType:       Params.CommonCfg.StorageType.GetValue(),
			Region:            Params.MinioCfg.Region.GetValue(),
			UseVirtualHost:    Params.MinioCfg.UseVirtualHost.GetAsBool(),
			CloudProvider:     Params.MinioCfg.CloudProvider.GetValue(),
			RequestTimeoutMs:  Params.MinioCfg.RequestTimeoutMs.GetAsInt64(),
			MaxConnections:    uint32(Params.MinioCfg.MaxConnections.GetAsInt()),
			GcpCredentialJSON: Params.MinioCfg.GcpCredentialJSON.GetValue(),
			SslTlsMinVersion:  Params.MinioCfg.SslTLSMinVersion.GetValue(),
			UseCrc32CChecksum: Params.MinioCfg.UseCRC32C.GetAsBool(),
		}
	}

	return storageConfig
}

func getSortStatus(sorted bool) string {
	if sorted {
		return "sorted"
	}
	return "unsorted"
}

const (
	fmIndexDefaultSASampleRate = int64(8)
	fmIndexDefaultBlockBytes   = int64(64)
	fmIndexSuffixArrayExtra    = int64(6144)
	bytesPerWorkerMemoryUnit   = int64(8 * 1024 * 1024 * 1024)
)

func saturatingAdd(values ...int64) int64 {
	var result int64
	for _, value := range values {
		if value > math.MaxInt64-result {
			return math.MaxInt64
		}
		result += value
	}
	return result
}

func saturatingMul(left, right int64) int64 {
	if left <= 0 || right <= 0 {
		return 0
	}
	if left > math.MaxInt64/right {
		return math.MaxInt64
	}
	return left * right
}

func ceilDiv(value, divisor int64) int64 {
	if value <= 0 {
		return 0
	}
	return 1 + (value-1)/divisor
}

func getFMIndexBuildParam(indexParams []*commonpb.KeyValuePair, key string, defaultValue int64) int64 {
	for _, param := range indexParams {
		if param.GetKey() != key {
			continue
		}
		value, err := strconv.ParseInt(param.GetValue(), 10, 64)
		if err == nil && value > 0 {
			return value
		}
		break
	}
	return defaultValue
}

// estimateFMIndexBuildPeakBytes mirrors the allocations in
// fmindex::FMIndex::Build. fieldSize is the uncompressed VARCHAR payload size;
// numRows determines the per-row object/view/boundary overhead and separator
// count. The larger of suffix-array construction and wavelet construction is
// returned because their scratch buffers do not all overlap.
func estimateFMIndexBuildPeakBytes(fieldSize, numRows int64, indexParams []*commonpb.KeyValuePair) int64 {
	fieldSize = max(fieldSize, 0)
	numRows = max(numRows, 0)
	saSampleRate := getFMIndexBuildParam(indexParams, indexparamcheck.FmSaSampleRateKey, fmIndexDefaultSASampleRate)
	blockBytes := getFMIndexBuildParam(indexParams, indexparamcheck.FmBlockBytesKey, fmIndexDefaultBlockBytes)

	// One separator per row and one trailing sentinel are appended to the text.
	textSymbols := saturatingAdd(fieldSize, numRows, 1)
	rowBitmaps := saturatingMul(ceilDiv(numRows, 8), 2) // valid + null bitmap
	inputAndRows := saturatingAdd(
		fieldSize,
		saturatingMul(numRows, 32),                  // FieldData std::string objects (libstdc++)
		saturatingMul(numRows, 16),                  // build-time std::string_view array
		saturatingMul(saturatingAdd(numRows, 1), 8), // document boundaries
		rowBitmaps,
	)

	// The sampled-SA bitmap and its rank9 directory coexist with the suffix
	// array and remain live through wavelet construction.
	sampleWords := ceilDiv(textSymbols, 64)
	sampleBitmap := saturatingMul(sampleWords, 8)
	sampleDirectory := saturatingMul(saturatingAdd(ceilDiv(sampleWords, 8), 1), 16)
	sampleCount := saturatingAdd((textSymbols-1)/saSampleRate, 1)
	sampleWidth := int64(4)
	if textSymbols >= 1<<32 {
		sampleWidth = 8
	}
	samples := saturatingMul(sampleCount, sampleWidth)
	shared := saturatingAdd(inputAndRows, sampleBitmap, sampleDirectory, samples)

	// Compact builds hold int32 text + int32 SA (including libsais scratch).
	// The C++ decision uses the pre-sentinel length, so textSymbols itself can
	// equal INT32_MAX and still use the compact path.
	var suffixArrayScratch int64
	if textSymbols <= math.MaxInt32 {
		suffixArrayScratch = saturatingAdd(
			saturatingMul(textSymbols, 4),
			saturatingMul(saturatingAdd(textSymbols, fmIndexSuffixArrayExtra), 4),
		)
	} else {
		suffixArrayScratch = saturatingMul(textSymbols, 16)
	}
	suffixArrayPeak := saturatingAdd(shared, suffixArrayScratch)

	// Wavelet construction holds two uint16 ping-pong buffers plus one packed
	// 2-bit vector and rank directory per quad level. Five levels is the maximum
	// for the byte alphabet plus separator/sentinel and is conservative.
	wordsPerBlock := max(blockBytes/8, 1)
	waveletWords := ceilDiv(textSymbols, 32)
	waveletBlocks := ceilDiv(waveletWords, wordsPerBlock)
	waveletSuperBlocks := ceilDiv(waveletBlocks, 64)
	waveletDirectoryPerLevel := saturatingAdd(
		saturatingMul(saturatingAdd(waveletSuperBlocks, 1), 32),
		saturatingMul(saturatingAdd(waveletBlocks, 1), 8),
	)
	waveletLevelBytes := saturatingAdd(saturatingMul(waveletWords, 8), waveletDirectoryPerLevel)
	waveletPeak := saturatingAdd(
		shared,
		saturatingMul(textSymbols, 4),
		saturatingMul(waveletLevelBytes, 5),
	)

	return max(suffixArrayPeak, waveletPeak)
}

func fmIndexBuildTaskSlots(fieldSize, numRows int64, indexParams []*commonpb.KeyValuePair) int64 {
	// CalculateNodeSlots exposes WorkerSlotUnit * BuildParallel slots per 8 GiB
	// memory unit. Use the inverse conversion so the coordinator and worker
	// account for the same amount of memory per slot.
	workerSlotsPerMemoryUnit := saturatingMul(
		max(Params.DataNodeCfg.WorkerSlotUnit.GetAsInt64(), 1),
		max(Params.DataNodeCfg.BuildParallel.GetAsInt64(), 1),
	)
	if paramtable.GetRole() == typeutil.StandaloneRole {
		workerSlotsPerMemoryUnit = max(
			int64(float64(workerSlotsPerMemoryUnit)*Params.DataNodeCfg.StandaloneSlotRatio.GetAsFloat()),
			1,
		)
	}
	bytesPerSlot := max(bytesPerWorkerMemoryUnit/workerSlotsPerMemoryUnit, 1)
	return max(ceilDiv(estimateFMIndexBuildPeakBytes(fieldSize, numRows, indexParams), bytesPerSlot), 1)
}

func calculateIndexTaskSlot(fieldSize, numRows int64, indexParams []*commonpb.KeyValuePair) int64 {
	indexType := GetIndexType(indexParams)
	if indexType == indexparamcheck.IndexFMINDEX {
		return fmIndexBuildTaskSlots(fieldSize, numRows, indexParams)
	}
	defaultSlots := Params.DataCoordCfg.IndexTaskSlotUsage.GetAsInt64()
	isHeavyIndex := vecindexmgr.GetVecIndexMgrInstance().IsVecIndex(indexType)
	if !isHeavyIndex {
		defaultSlots = Params.DataCoordCfg.ScalarIndexTaskSlotUsage.GetAsInt64()
	}
	if fieldSize > 512*1024*1024 {
		taskSlot := max(fieldSize/512/1024/1024, 1) * defaultSlots
		return max(taskSlot, 1)
	} else if fieldSize > 100*1024*1024 {
		return max(defaultSlots/4, 1)
	} else if fieldSize > 10*1024*1024 {
		return max(defaultSlots/16, 1)
	}
	return max(defaultSlots/64, 1)
}

// isFixedWidthType tells whether the schema alone fully determines the size of
// a field, i.e. the estimation cannot be off. Variable length types only get a
// configured guess out of the schema (see typeutil.EstimateSizePerRecord), so
// they are attributed from the measured data instead whenever possible.
//
// Bool is deliberately absent: the schema charges it 1 byte per row while the
// measured group size accounts it bit-packed (~1/8 byte per row, see
// ActualSizeInBytes handling of arrow.BOOL), and external sampling can round
// it down to 0. Treating it as exact would over-deduct from the residual and
// understate the variable length columns sharing its group.
func isFixedWidthType(dataType schemapb.DataType) bool {
	switch dataType {
	case schemapb.DataType_Int8, schemapb.DataType_Int16,
		schemapb.DataType_Int32, schemapb.DataType_Int64, schemapb.DataType_Float,
		schemapb.DataType_Double, schemapb.DataType_Timestamptz,
		schemapb.DataType_BinaryVector, schemapb.DataType_FloatVector,
		schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector,
		schemapb.DataType_Int8Vector:
		return true
	default:
		return false
	}
}

// fieldSizeEstimate is the schema-derived size of one field, qualified by how
// far it can be trusted. The two flags serve the two distinct uses of a size:
//
//   - exact: the stored size is fully determined by the schema (fixed width,
//     not nullable). Safe to deduct from the measured group budget — a
//     deduction must never exceed the field's real bytes, or the remaining
//     variable length fields get understated.
//   - fixedWidth: the schema width bounds the stored data from above even when
//     the field is nullable (null rows only shrink it). Safe to charge the
//     requested field with, never to deduct.
type fieldSizeEstimate struct {
	sizePerRecord int64
	exact         bool
	fixedWidth    bool
}

// fieldSizePerRecord returns the per row size of a field derived from the
// schema.
//
// A nullable field is never exact, whatever its data type: rows holding null
// store less than the full width (nullable vectors are even stored with a
// variable length encoding, see add_vector_payload in
// internal/core/src/storage/Util.cpp), so its real size is data dependent.
// For a fixed width type the schema width still bounds it from above, which
// fixedWidth records.
func fieldSizePerRecord(schema *schemapb.CollectionSchema, fieldID int64) (fieldSizeEstimate, error) {
	field := typeutil.GetFieldByID(schema, fieldID)
	if field == nil {
		return fieldSizeEstimate{}, merr.WrapErrFieldNotFound(fieldID)
	}
	sizePerRecord, err := typeutil.EstimateSizePerRecord(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{field},
	})
	if err != nil {
		return fieldSizeEstimate{}, err
	}
	if sizePerRecord <= 0 {
		// An unrecognized data type estimates to zero, which would understate
		// the slot usage. Let the caller keep the conservative binlog size.
		return fieldSizeEstimate{}, merr.WrapErrParameterInvalidMsg("cannot estimate size of field %d with data type %s",
			fieldID, field.GetDataType().String())
	}
	fixedWidth := isFixedWidthType(field.GetDataType())
	return fieldSizeEstimate{
		sizePerRecord: int64(sizePerRecord),
		exact:         fixedWidth && !field.GetNullable(),
		fixedWidth:    fixedWidth,
	}, nil
}

// columnGroup is a set of fields stored together, which is what a binlog entry
// describes for storage v3 segments. Storage v1 binlogs hold a single field and
// therefore form a group of one.
type columnGroup struct {
	fields []int64
	size   int64
}

// segmentColumnGroups returns the column groups of a segment, keyed by every
// field they hold.
func segmentColumnGroups(segment *SegmentInfo) map[int64]*columnGroup {
	groups := make(map[int64]*columnGroup)
	for _, fieldBinlog := range segment.GetBinlogs() {
		fields := fieldBinlog.GetChildFields()
		if len(fields) == 0 {
			fields = []int64{fieldBinlog.GetFieldID()}
		}
		var size int64
		for _, binlog := range fieldBinlog.GetBinlogs() {
			size += binlog.GetMemorySize()
		}
		group := &columnGroup{fields: fields, size: size}
		for _, fieldID := range fields {
			groups[fieldID] = group
		}
	}
	return groups
}

// estimateFieldsReadSize estimates how much data a task reads for fieldIDs of a
// segment.
//
// It works off segment.GetBinlogs(), which for manifest-backed StorageV3
// segments only exists in memory: the catalog does not persist their
// FieldBinlog arrays (see isV3Segment), so after a DataCoord restart such
// segments carry no binlogs and every field errors out here, letting the
// callers keep their conservative whole-segment fallback until the segment is
// rewritten.
//
// Both the index build and the json/text stats tasks request a single column at
// a time. Per-field attribution is safe only for a StorageV3 segment with a
// manifest: GetFieldDatasFromManifest asks the reader for the target column,
// while GetFieldDatasFromStorageV2 materializes every column of the selected
// column group. The binlog size however is recorded per column group, so a
// projected field sharing a group with others otherwise gets charged for all of
// them. This is worst for external collections, whose segments carry one
// synthetic group holding every column at once (see buildFakeBinlogs in
// internal/datanode/external).
//
// The group size is the measured truth for the group as a whole, so it is used
// as the budget: fields whose schema gives an exact size take exactly that,
// nullable fixed width fields take their schema width plus the nullable
// encoding overhead (an upper bound — nulls only shrink the stored data), and
// whatever remains is charged to the variable length fields. That keeps a 128
// dim vector at (close to) its real size — including on non milvus-table
// external collections, where every user field is forced nullable at create
// time — while a json column is charged what the data really contains instead
// of the configured per row guess (common.dynamicFieldLengthAvg), which can be
// an order of magnitude off. The residual is not split proportionally between
// several variable length fields: the schema weights are guesses, and
// splitting by them could understate a fat column, i.e. over-admit tasks.
// Charging it once keeps the result an upper bound, which is what a slot must
// be. The flip side is that a variable length column of a group with nothing
// exact to deduct (e.g. any non milvus-table external group, all nullable) is
// charged the whole group.
func estimateFieldsReadSize(schema *schemapb.CollectionSchema, segment *SegmentInfo, fieldIDs []int64) (int64, error) {
	if len(fieldIDs) == 0 {
		return 0, merr.WrapErrParameterInvalidMsg("no field to estimate size for")
	}
	numRows := segment.GetNumOfRows()
	if numRows <= 0 {
		return 0, nil
	}
	if !supportsFieldProjection(segment) {
		// Callers gate this optimization on supportsFieldProjection. Reaching
		// this branch means an internal contract was violated, not bad input.
		return 0, merr.WrapErrServiceInternalMsg(
			"segment %d does not have a StorageV3 manifest for projected field reads", segment.GetID())
	}

	groups := segmentColumnGroups(segment)
	// requested fields grouped by the column group holding them
	requested := make(map[*columnGroup][]int64)
	for _, fieldID := range fieldIDs {
		group, ok := groups[fieldID]
		if !ok {
			return 0, merr.WrapErrFieldNotFound(fieldID, "field has no binlog in segment")
		}
		requested[group] = append(requested[group], fieldID)
	}

	var total int64
	for group, groupFieldIDs := range requested {
		size, err := estimateGroupFieldsSize(schema, group, groupFieldIDs, numRows)
		if err != nil {
			return 0, err
		}
		total += size
	}
	return total, nil
}

// supportsFieldProjection matches the actual reader selection in the index and
// stats workers. A non-empty manifest selects GetFieldDatasFromManifest, which
// projects the requested column. StorageV2 files without a manifest are read by
// GetFieldDatasFromStorageV2, which materializes the whole column group.
func supportsFieldProjection(segment *SegmentInfo) bool {
	return segment.GetStorageVersion() == storage.StorageV3 && segment.GetManifestPath() != ""
}

// nullableFixedWidthPadPerRow bounds the per row encoding overhead a nullable
// fixed width column adds on top of its schema width: nullable vectors are
// binary encoded (4 byte offsets, see add_vector_payload in
// internal/core/src/storage/Util.cpp), and every nullable column carries a
// validity bitmap (1/8 byte per row).
const nullableFixedWidthPadPerRow = 8

// estimateGroupFieldsSize attributes the measured size of one column group to
// the requested fields of that group.
//
// Each requested field is charged by the strongest bound available: an exact
// field its schema size, a nullable fixed width field its schema width plus
// the nullable encoding overhead (nulls only shrink the stored data), and a
// variable length field the measured residual of the group — the group size
// minus the exact fields, which only exact fields may reduce (deducting a
// nullable field's full width could overstate its real bytes and understate
// everyone else's).
func estimateGroupFieldsSize(schema *schemapb.CollectionSchema, group *columnGroup, fieldIDs []int64, numRows int64) (int64, error) {
	if group.size <= 0 {
		// nothing measured to attribute, e.g. binlogs written before the size
		// was recorded. Let the caller keep its own conservative fallback.
		return 0, merr.WrapErrParameterInvalidMsg("column group of fields %v has no recorded size", group.fields)
	}
	if len(group.fields) <= 1 {
		// the group holds nothing but the requested field, its size is exact
		return group.size, nil
	}

	requestedFields := make(map[int64]struct{}, len(fieldIDs))
	for _, fieldID := range fieldIDs {
		requestedFields[fieldID] = struct{}{}
	}

	estimates := make(map[int64]fieldSizeEstimate, len(group.fields))
	var fixedPerRecord int64
	var hasVariableField bool
	var externalColumnResolver *typeutil.StorageColumnResolver
	if typeutil.IsExternalCollection(schema) {
		externalColumnResolver = typeutil.NewStorageColumnResolver(schema)
	}
	for _, fieldID := range group.fields {
		estimate, err := fieldSizePerRecord(schema, fieldID)
		if err != nil {
			if _, requested := requestedFields[fieldID]; requested {
				return 0, err
			}
			// A group member the schema cannot resolve, e.g. a field dropped
			// after this segment was flushed and before compaction rewrote it.
			// Skipping it leaves its bytes in the residual instead of giving up
			// on the whole group.
			continue
		}
		estimates[fieldID] = estimate
		if estimate.exact {
			measured := true
			if externalColumnResolver != nil {
				field := typeutil.GetFieldByID(schema, fieldID)
				_, measured = externalColumnResolver.SourceDataColumnName(field)
				// Refresh adds generated function outputs to MemorySize after
				// sampling the source columns, so they are measured too.
				measured = measured || typeutil.IsFunctionOutputField(schema, field)
			}
			if measured {
				fixedPerRecord += estimate.sizePerRecord
			}
			continue
		}
		hasVariableField = true
	}

	// bytes the variable length fields of this group take in total, measured.
	// Kept in whole bytes rather than per record: a per record truncation
	// could lose up to numRows bytes and drop the estimate into a lower slot
	// bucket.
	residualBytes := group.size - fixedPerRecord*numRows
	if residualBytes < 0 || !hasVariableField {
		// the schema does not add up to the measured data, e.g. because some
		// field of the group is not materialized in it. Fall back to the schema
		// estimation alone, still bounded by the group size.
		residualBytes = 0
	}

	var total int64
	var residualCharged bool
	for _, fieldID := range fieldIDs {
		estimate := estimates[fieldID]
		switch {
		case estimate.exact:
			total += estimate.sizePerRecord * numRows
		case estimate.fixedWidth:
			// Nullable fixed width: null rows only shrink the stored data, so
			// the schema width plus the nullable encoding overhead bounds it
			// from above; the measured residual it lives in caps it further.
			// This branch is what keeps the attribution alive on non
			// milvus-table external collections, where create time forces
			// every user field nullable (see Pass 2 of
			// NormalizeAndValidateExternalCollectionSchema) and nothing is
			// left to deduct from the group budget.
			charge := (estimate.sizePerRecord + nullableFixedWidthPadPerRow) * numRows
			if residualBytes > 0 && charge > residualBytes {
				charge = residualBytes
			}
			total += charge
		case residualBytes > 0:
			// every variable length field of the group shares the same
			// residual, so it is charged once no matter how many of them are
			// requested
			if !residualCharged {
				total += residualBytes
				residualCharged = true
			}
		default:
			total += estimate.sizePerRecord * numRows
		}
	}
	if total > group.size {
		return group.size, nil
	}
	return total, nil
}

func calculateStatsTaskSlot(segmentSize int64) int64 {
	defaultSlots := Params.DataCoordCfg.StatsTaskSlotUsage.GetAsInt64()
	if segmentSize > 512*1024*1024 {
		taskSlot := max(segmentSize/512/1024/1024, 1) * defaultSlots
		return max(taskSlot, 1)
	} else if segmentSize > 100*1024*1024 {
		return max(defaultSlots/2, 1)
	} else if segmentSize > 10*1024*1024 {
		return max(defaultSlots/4, 1)
	}
	return max(defaultSlots/8, 1)
}

func enableSortCompaction() bool {
	return paramtable.Get().DataCoordCfg.EnableSortCompaction.GetAsBool() && paramtable.Get().DataCoordCfg.EnableCompaction.GetAsBool()
}

// stringifyBinlogs is used for logging, it's not used for other purposes.
func stringifyBinlogs(binlogs []*datapb.FieldBinlog) []string {
	strs := make([]string, 0, len(binlogs))
	byIDs := lo.GroupBy(binlogs, func(binlog *datapb.FieldBinlog) int64 {
		return binlog.GetFieldID()
	})
	for _, binlogs := range byIDs {
		fieldsStrs := make([]string, 0, len(binlogs))
		for _, binlog := range binlogs {
			for _, b := range binlog.GetBinlogs() {
				fieldsStrs = append(fieldsStrs,
					fmt.Sprintf("l%d(e%d,m%d,t%d-%d)", b.LogID, b.EntriesNum, b.MemorySize, b.TimestampFrom, b.TimestampTo),
				)
			}
		}
		strs = append(strs, fmt.Sprintf("f%d:%s", binlogs[0].GetFieldID(), strings.Join(fieldsStrs, "|")))
	}
	return strs
}
