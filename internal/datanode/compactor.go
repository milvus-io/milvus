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

package datanode

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/allocator"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/datanode/syncmgr"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var (
	errCompactionTypeUndifined = errors.New("compaction type undefined")
	errIllegalCompactionPlan   = errors.New("compaction plan illegal")
	errTransferType            = errors.New("transfer intferface to type wrong")
	errUnknownDataType         = errors.New("unknown shema DataType")
	errContext                 = errors.New("context done or timeout")
)

type iterator = storage.Iterator

type compactor interface {
	complete()
	compact() (*datapb.CompactionPlanResult, error)
	injectDone()
	stop()
	getPlanID() UniqueID
	getCollection() UniqueID
	getChannelName() string
}

// make sure compactionTask implements compactor interface
var _ compactor = (*compactionTask)(nil)

type compactionTask struct {
	downloader
	uploader
	compactor
	metaCache metacache.MetaCache
	syncMgr   syncmgr.SyncManager
	allocator.Allocator

	plan *datapb.CompactionPlan

	ctx    context.Context
	cancel context.CancelFunc

	done chan struct{}
	tr   *timerecord.TimeRecorder
}

func newCompactionTask(
	ctx context.Context,
	dl downloader,
	ul uploader,
	metaCache metacache.MetaCache,
	syncMgr syncmgr.SyncManager,
	alloc allocator.Allocator,
	plan *datapb.CompactionPlan,
) *compactionTask {
	ctx1, cancel := context.WithCancel(ctx)
	return &compactionTask{
		ctx:    ctx1,
		cancel: cancel,

		downloader: dl,
		uploader:   ul,
		syncMgr:    syncMgr,
		metaCache:  metaCache,
		Allocator:  alloc,
		plan:       plan,
		tr:         timerecord.NewTimeRecorder("levelone compaction"),
		done:       make(chan struct{}, 1),
	}
}

func (t *compactionTask) complete() {
	t.done <- struct{}{}
}

func (t *compactionTask) stop() {
	t.cancel()
	<-t.done
	t.injectDone()
}

func (t *compactionTask) getPlanID() UniqueID {
	return t.plan.GetPlanID()
}

func (t *compactionTask) getChannelName() string {
	return t.plan.GetChannel()
}

// return num rows of all segment compaction from
func (t *compactionTask) getNumRows() (int64, error) {
	numRows := int64(0)
	for _, binlog := range t.plan.SegmentBinlogs {
		seg, ok := t.metaCache.GetSegmentByID(binlog.GetSegmentID())
		if !ok {
			return 0, merr.WrapErrSegmentNotFound(binlog.GetSegmentID(), "get compaction segments num rows failed")
		}

		numRows += seg.NumOfRows()
	}

	return numRows, nil
}

func (t *compactionTask) mergeDeltalogs(dBlobs map[UniqueID][]*Blob) (map[interface{}]Timestamp, error) {
	log := log.With(zap.Int64("planID", t.getPlanID()))
	mergeStart := time.Now()
	dCodec := storage.NewDeleteCodec()

	pk2ts := make(map[interface{}]Timestamp)

	for _, blobs := range dBlobs {
		_, _, dData, err := dCodec.Deserialize(blobs)
		if err != nil {
			log.Warn("merge deltalogs wrong", zap.Error(err))
			return nil, err
		}

		for i := int64(0); i < dData.RowCount; i++ {
			pk := dData.Pks[i]
			ts := dData.Tss[i]

			pk2ts[pk.GetValue()] = ts
		}
	}

	log.Info("mergeDeltalogs end",
		zap.Int("number of deleted pks to compact in insert logs", len(pk2ts)),
		zap.Duration("elapse", time.Since(mergeStart)))

	return pk2ts, nil
}

func (t *compactionTask) uploadRemainLog(
	ctxTimeout context.Context,
	targetSegID UniqueID,
	partID UniqueID,
	meta *etcdpb.CollectionMeta,
	stats *storage.PrimaryKeyStats,
	totRows int64,
	fID2Content map[UniqueID][]interface{},
	fID2Type map[UniqueID]schemapb.DataType,
) (map[UniqueID]*datapb.FieldBinlog, map[UniqueID]*datapb.FieldBinlog, error) {
	var iData *InsertData

	// remain insert data
	if len(fID2Content) != 0 {
		iData = &InsertData{Data: make(map[storage.FieldID]storage.FieldData)}
		for fID, content := range fID2Content {
			tp, ok := fID2Type[fID]
			if !ok {
				log.Warn("no field ID in this schema", zap.Int64("fieldID", fID))
				return nil, nil, errors.New("Unexpected error")
			}

			fData, err := interface2FieldData(tp, content, int64(len(content)))
			if err != nil {
				log.Warn("transfer interface to FieldData wrong", zap.Error(err))
				return nil, nil, err
			}
			iData.Data[fID] = fData
		}
	}

	inPaths, statPaths, err := t.uploadStatsLog(ctxTimeout, targetSegID, partID, iData, stats, totRows, meta)
	if err != nil {
		return nil, nil, err
	}

	return inPaths, statPaths, nil
}

func (t *compactionTask) uploadSingleInsertLog(
	ctxTimeout context.Context,
	targetSegID UniqueID,
	partID UniqueID,
	meta *etcdpb.CollectionMeta,
	fID2Content map[UniqueID][]interface{},
	fID2Type map[UniqueID]schemapb.DataType,
) (map[UniqueID]*datapb.FieldBinlog, error) {
	iData := &InsertData{
		Data: make(map[storage.FieldID]storage.FieldData),
	}

	for fID, content := range fID2Content {
		tp, ok := fID2Type[fID]
		if !ok {
			log.Warn("no field ID in this schema", zap.Int64("fieldID", fID))
			return nil, errors.New("Unexpected error")
		}

		fData, err := interface2FieldData(tp, content, int64(len(content)))
		if err != nil {
			log.Warn("transfer interface to FieldData wrong", zap.Error(err))
			return nil, err
		}
		iData.Data[fID] = fData
	}

	inPaths, err := t.uploadInsertLog(ctxTimeout, targetSegID, partID, iData, meta)
	if err != nil {
		return nil, err
	}

	return inPaths, nil
}

func (t *compactionTask) merge(
	ctxTimeout context.Context,
	unMergedInsertlogs [][]string,
	targetSegID UniqueID,
	partID UniqueID,
	meta *etcdpb.CollectionMeta,
	delta map[interface{}]Timestamp,
) ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, int64, error) {
	log := log.With(zap.Int64("planID", t.getPlanID()))
	mergeStart := time.Now()

	var (
		maxRowsPerBinlog int   // maximum rows populating one binlog
		numBinlogs       int   // binlog number
		numRows          int64 // the number of rows uploaded
		expired          int64 // the number of expired entity

		fID2Type    = make(map[UniqueID]schemapb.DataType)
		fID2Content = make(map[UniqueID][]interface{})

		insertField2Path = make(map[UniqueID]*datapb.FieldBinlog)
		insertPaths      = make([]*datapb.FieldBinlog, 0)

		statField2Path = make(map[UniqueID]*datapb.FieldBinlog)
		statPaths      = make([]*datapb.FieldBinlog, 0)
	)

	isDeletedValue := func(v *storage.Value) bool {
		ts, ok := delta[v.PK.GetValue()]
		// insert task and delete task has the same ts when upsert
		// here should be < instead of <=
		// to avoid the upsert data to be deleted after compact
		if ok && uint64(v.Timestamp) < ts {
			return true
		}
		return false
	}

	addInsertFieldPath := func(inPaths map[UniqueID]*datapb.FieldBinlog, timestampFrom, timestampTo int64) {
		for fID, path := range inPaths {
			for _, binlog := range path.GetBinlogs() {
				binlog.TimestampTo = uint64(timestampTo)
				binlog.TimestampFrom = uint64(timestampFrom)
			}
			tmpBinlog, ok := insertField2Path[fID]
			if !ok {
				tmpBinlog = path
			} else {
				tmpBinlog.Binlogs = append(tmpBinlog.Binlogs, path.GetBinlogs()...)
			}
			insertField2Path[fID] = tmpBinlog
		}
	}

	addStatFieldPath := func(statPaths map[UniqueID]*datapb.FieldBinlog) {
		for fID, path := range statPaths {
			tmpBinlog, ok := statField2Path[fID]
			if !ok {
				tmpBinlog = path
			} else {
				tmpBinlog.Binlogs = append(tmpBinlog.Binlogs, path.GetBinlogs()...)
			}
			statField2Path[fID] = tmpBinlog
		}
	}

	// get pkID, pkType, dim
	var pkField *schemapb.FieldSchema
	for _, fs := range meta.GetSchema().GetFields() {
		fID2Type[fs.GetFieldID()] = fs.GetDataType()
		if fs.GetIsPrimaryKey() && fs.GetFieldID() >= 100 && typeutil.IsPrimaryFieldType(fs.GetDataType()) {
			pkField = fs
		}
	}

	if pkField == nil {
		log.Warn("failed to get pk field from schema")
		return nil, nil, 0, fmt.Errorf("no pk field in schema")
	}

	pkID := pkField.GetFieldID()
	pkType := pkField.GetDataType()

	// estimate Rows per binlog
	// TODO should not convert size to row because we already know the size, this is especially important on varchar types.
	size, err := typeutil.EstimateSizePerRecord(meta.GetSchema())
	if err != nil {
		log.Warn("failed to estimate size per record", zap.Error(err))
		return nil, nil, 0, err
	}

	maxRowsPerBinlog = int(Params.DataNodeCfg.BinLogMaxSize.GetAsInt64() / int64(size))
	if Params.DataNodeCfg.BinLogMaxSize.GetAsInt64()%int64(size) != 0 {
		maxRowsPerBinlog++
	}

	expired = 0
	numRows = 0
	numBinlogs = 0
	currentTs := t.GetCurrentTime()
	currentRows := 0
	downloadTimeCost := time.Duration(0)
	uploadInsertTimeCost := time.Duration(0)

	oldRowNums, err := t.getNumRows()
	if err != nil {
		return nil, nil, 0, err
	}

	stats, err := storage.NewPrimaryKeyStats(pkID, int64(pkType), oldRowNums)
	if err != nil {
		return nil, nil, 0, err
	}
	// initial timestampFrom, timestampTo = -1, -1 is an illegal value, only to mark initial state
	var (
		timestampTo   int64 = -1
		timestampFrom int64 = -1
	)

	for _, path := range unMergedInsertlogs {
		downloadStart := time.Now()
		data, err := t.download(ctxTimeout, path)
		if err != nil {
			log.Warn("download insertlogs wrong", zap.Strings("path", path), zap.Error(err))
			return nil, nil, 0, err
		}
		downloadTimeCost += time.Since(downloadStart)

		iter, err := storage.NewInsertBinlogIterator(data, pkID, pkType)
		if err != nil {
			log.Warn("new insert binlogs Itr wrong", zap.Strings("path", path), zap.Error(err))
			return nil, nil, 0, err
		}

		for iter.HasNext() {
			vInter, _ := iter.Next()
			v, ok := vInter.(*storage.Value)
			if !ok {
				log.Warn("transfer interface to Value wrong", zap.Strings("path", path))
				return nil, nil, 0, errors.New("unexpected error")
			}

			if isDeletedValue(v) {
				continue
			}

			ts := Timestamp(v.Timestamp)
			// Filtering expired entity
			if t.isExpiredEntity(ts, currentTs) {
				expired++
				continue
			}

			// Update timestampFrom, timestampTo
			if v.Timestamp < timestampFrom || timestampFrom == -1 {
				timestampFrom = v.Timestamp
			}
			if v.Timestamp > timestampTo || timestampFrom == -1 {
				timestampTo = v.Timestamp
			}

			row, ok := v.Value.(map[UniqueID]interface{})
			if !ok {
				log.Warn("transfer interface to map wrong", zap.Strings("path", path))
				return nil, nil, 0, errors.New("unexpected error")
			}

			for fID, vInter := range row {
				if _, ok := fID2Content[fID]; !ok {
					fID2Content[fID] = make([]interface{}, 0)
				}
				fID2Content[fID] = append(fID2Content[fID], vInter)
			}
			// update pk to new stats log
			stats.Update(v.PK)

			currentRows++
			if currentRows >= maxRowsPerBinlog {
				uploadInsertStart := time.Now()
				inPaths, err := t.uploadSingleInsertLog(ctxTimeout, targetSegID, partID, meta, fID2Content, fID2Type)
				if err != nil {
					log.Warn("failed to upload single insert log", zap.Error(err))
					return nil, nil, 0, err
				}
				uploadInsertTimeCost += time.Since(uploadInsertStart)
				addInsertFieldPath(inPaths, timestampFrom, timestampTo)
				timestampFrom = -1
				timestampTo = -1

				fID2Content = make(map[int64][]interface{})
				currentRows = 0
				numRows += int64(maxRowsPerBinlog)
				numBinlogs++
			}
		}
	}

	// upload stats log and remain insert rows
	if numRows != 0 || currentRows != 0 {
		uploadStart := time.Now()
		inPaths, statsPaths, err := t.uploadRemainLog(ctxTimeout, targetSegID, partID, meta,
			stats, numRows+int64(currentRows), fID2Content, fID2Type)
		if err != nil {
			return nil, nil, 0, err
		}

		uploadInsertTimeCost += time.Since(uploadStart)
		addInsertFieldPath(inPaths, timestampFrom, timestampTo)
		addStatFieldPath(statsPaths)
		numRows += int64(currentRows)
		numBinlogs += len(inPaths)
	}

	for _, path := range insertField2Path {
		insertPaths = append(insertPaths, path)
	}

	for _, path := range statField2Path {
		statPaths = append(statPaths, path)
	}

	log.Info("compact merge end",
		zap.Int64("remaining insert numRows", numRows),
		zap.Int64("expired entities", expired),
		zap.Int("binlog file number", numBinlogs),
		zap.Duration("download insert log elapse", downloadTimeCost),
		zap.Duration("upload insert log elapse", uploadInsertTimeCost),
		zap.Duration("merge elapse", time.Since(mergeStart)))

	return insertPaths, statPaths, numRows, nil
}

func (t *compactionTask) compact() (*datapb.CompactionPlanResult, error) {
	log := log.With(zap.Int64("planID", t.plan.GetPlanID()))
	compactStart := time.Now()
	if ok := funcutil.CheckCtxValid(t.ctx); !ok {
		log.Warn("compact wrong, task context done or timeout")
		return nil, errContext
	}

	durInQueue := t.tr.RecordSpan()
	ctxTimeout, cancelAll := context.WithTimeout(t.ctx, time.Duration(t.plan.GetTimeoutInSeconds())*time.Second)
	defer cancelAll()

	var targetSegID UniqueID
	var err error
	switch {
	case t.plan.GetType() == datapb.CompactionType_UndefinedCompaction:
		log.Warn("compact wrong, compaction type undefined")
		return nil, errCompactionTypeUndifined

	case len(t.plan.GetSegmentBinlogs()) < 1:
		log.Warn("compact wrong, there's no segments in segment binlogs")
		return nil, errIllegalCompactionPlan

	case t.plan.GetType() == datapb.CompactionType_MergeCompaction || t.plan.GetType() == datapb.CompactionType_MixCompaction:
		targetSegID, err = t.AllocOne()
		if err != nil {
			log.Warn("compact wrong", zap.Error(err))
			return nil, err
		}
	}

	log.Info("compact start", zap.Int32("timeout in seconds", t.plan.GetTimeoutInSeconds()))
	segIDs := make([]UniqueID, 0, len(t.plan.GetSegmentBinlogs()))
	for _, s := range t.plan.GetSegmentBinlogs() {
		segIDs = append(segIDs, s.GetSegmentID())
	}

	_, partID, meta, err := t.getSegmentMeta(segIDs[0])
	if err != nil {
		log.Warn("compact wrong", zap.Error(err))
		return nil, err
	}

	// Inject to stop flush
	injectStart := time.Now()
	for _, segID := range segIDs {
		t.syncMgr.Block(segID)
	}
	log.Info("compact inject elapse", zap.Duration("elapse", time.Since(injectStart)))
	defer func() {
		if err != nil {
			for _, segID := range segIDs {
				t.syncMgr.Unblock(segID)
			}
		}
	}()

	dblobs := make(map[UniqueID][]*Blob)
	allPath := make([][]string, 0)

	downloadStart := time.Now()
	for _, s := range t.plan.GetSegmentBinlogs() {
		// Get the number of field binlog files from non-empty segment
		var binlogNum int
		for _, b := range s.GetFieldBinlogs() {
			if b != nil {
				binlogNum = len(b.GetBinlogs())
				break
			}
		}
		// Unable to deal with all empty segments cases, so return error
		if binlogNum == 0 {
			log.Warn("compact wrong, all segments' binlogs are empty")
			return nil, errIllegalCompactionPlan
		}

		for idx := 0; idx < binlogNum; idx++ {
			var ps []string
			for _, f := range s.GetFieldBinlogs() {
				ps = append(ps, f.GetBinlogs()[idx].GetLogPath())
			}
			allPath = append(allPath, ps)
		}

		segID := s.GetSegmentID()
		paths := make([]string, 0)
		for _, d := range s.GetDeltalogs() {
			for _, l := range d.GetBinlogs() {
				path := l.GetLogPath()
				paths = append(paths, path)
			}
		}

		if len(paths) != 0 {
			bs, err := t.download(ctxTimeout, paths)
			if err != nil {
				log.Warn("compact download deltalogs wrong", zap.Int64("segment", segID), zap.Strings("path", paths), zap.Error(err))
				return nil, err
			}
			dblobs[segID] = append(dblobs[segID], bs...)
		}
	}

	log.Info("compact download deltalogs elapse", zap.Duration("elapse", time.Since(downloadStart)))

	if err != nil {
		log.Warn("compact IO wrong", zap.Error(err))
		return nil, err
	}

	deltaPk2Ts, err := t.mergeDeltalogs(dblobs)
	if err != nil {
		return nil, err
	}

	inPaths, statsPaths, numRows, err := t.merge(ctxTimeout, allPath, targetSegID, partID, meta, deltaPk2Ts)
	if err != nil {
		log.Warn("compact wrong", zap.Error(err))
		return nil, err
	}

	pack := &datapb.CompactionSegment{
		SegmentID:           targetSegID,
		InsertLogs:          inPaths,
		Field2StatslogPaths: statsPaths,
		NumOfRows:           numRows,
		Channel:             t.plan.GetChannel(),
	}

	log.Info("compact done",
		zap.Int64("targetSegmentID", targetSegID),
		zap.Int64s("compactedFrom", segIDs),
		zap.Int("num of binlog paths", len(inPaths)),
		zap.Int("num of stats paths", len(statsPaths)),
		zap.Int("num of delta paths", len(pack.GetDeltalogs())),
	)

	log.Info("compact overall elapse", zap.Duration("elapse", time.Since(compactStart)))
	metrics.DataNodeCompactionLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), t.plan.GetType().String()).Observe(float64(t.tr.ElapseSpan().Milliseconds()))
	metrics.DataNodeCompactionLatencyInQueue.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Observe(float64(durInQueue.Milliseconds()))

	planResult := &datapb.CompactionPlanResult{
		State:    commonpb.CompactionState_Completed,
		PlanID:   t.getPlanID(),
		Segments: []*datapb.CompactionSegment{pack},
	}

	return planResult, nil
}

func (t *compactionTask) injectDone() {
	for _, binlog := range t.plan.SegmentBinlogs {
		t.syncMgr.Unblock(binlog.SegmentID)
	}
}

// TODO copy maybe expensive, but this seems to be the only convinent way.
func interface2FieldData(schemaDataType schemapb.DataType, content []interface{}, numRows int64) (storage.FieldData, error) {
	var rst storage.FieldData
	switch schemaDataType {
	case schemapb.DataType_Bool:
		data := &storage.BoolFieldData{
			Data: make([]bool, 0, len(content)),
		}

		for _, c := range content {
			r, ok := c.(bool)
			if !ok {
				return nil, errTransferType
			}
			data.Data = append(data.Data, r)
		}
		rst = data

	case schemapb.DataType_Int8:
		data := &storage.Int8FieldData{
			Data: make([]int8, 0, len(content)),
		}

		for _, c := range content {
			r, ok := c.(int8)
			if !ok {
				return nil, errTransferType
			}
			data.Data = append(data.Data, r)
		}
		rst = data

	case schemapb.DataType_Int16:
		data := &storage.Int16FieldData{
			Data: make([]int16, 0, len(content)),
		}

		for _, c := range content {
			r, ok := c.(int16)
			if !ok {
				return nil, errTransferType
			}
			data.Data = append(data.Data, r)
		}
		rst = data

	case schemapb.DataType_Int32:
		data := &storage.Int32FieldData{
			Data: make([]int32, 0, len(content)),
		}

		for _, c := range content {
			r, ok := c.(int32)
			if !ok {
				return nil, errTransferType
			}
			data.Data = append(data.Data, r)
		}
		rst = data

	case schemapb.DataType_Int64:
		data := &storage.Int64FieldData{
			Data: make([]int64, 0, len(content)),
		}

		for _, c := range content {
			r, ok := c.(int64)
			if !ok {
				return nil, errTransferType
			}
			data.Data = append(data.Data, r)
		}
		rst = data

	case schemapb.DataType_Float:
		data := &storage.FloatFieldData{
			Data: make([]float32, 0, len(content)),
		}

		for _, c := range content {
			r, ok := c.(float32)
			if !ok {
				return nil, errTransferType
			}
			data.Data = append(data.Data, r)
		}
		rst = data

	case schemapb.DataType_Double:
		data := &storage.DoubleFieldData{
			Data: make([]float64, 0, len(content)),
		}

		for _, c := range content {
			r, ok := c.(float64)
			if !ok {
				return nil, errTransferType
			}
			data.Data = append(data.Data, r)
		}
		rst = data

	case schemapb.DataType_String, schemapb.DataType_VarChar:
		data := &storage.StringFieldData{
			Data: make([]string, 0, len(content)),
		}

		for _, c := range content {
			r, ok := c.(string)
			if !ok {
				return nil, errTransferType
			}
			data.Data = append(data.Data, r)
		}
		rst = data

	case schemapb.DataType_JSON:
		data := &storage.JSONFieldData{
			Data: make([][]byte, 0, len(content)),
		}

		for _, c := range content {
			r, ok := c.([]byte)
			if !ok {
				return nil, errTransferType
			}
			data.Data = append(data.Data, r)
		}
		rst = data

	case schemapb.DataType_Array:
		data := &storage.ArrayFieldData{
			Data: make([]*schemapb.ScalarField, 0, len(content)),
		}

		if len(content) > 0 {
			data.ElementType = content[0].(*schemapb.ScalarField).GetArrayData().GetElementType()
		}

		for _, c := range content {
			r, ok := c.(*schemapb.ScalarField)
			if !ok {
				return nil, errTransferType
			}
			data.Data = append(data.Data, r)
		}
		rst = data

	case schemapb.DataType_FloatVector:
		data := &storage.FloatVectorFieldData{
			Data: []float32{},
		}

		for _, c := range content {
			r, ok := c.([]float32)
			if !ok {
				return nil, errTransferType
			}
			data.Data = append(data.Data, r...)
		}

		data.Dim = len(data.Data) / int(numRows)
		rst = data

	case schemapb.DataType_Float16Vector:
		data := &storage.Float16VectorFieldData{
			Data: []byte{},
		}

		for _, c := range content {
			r, ok := c.([]byte)
			if !ok {
				return nil, errTransferType
			}
			data.Data = append(data.Data, r...)
		}

		data.Dim = len(data.Data) / 2 / int(numRows)
		rst = data

	case schemapb.DataType_BFloat16Vector:
		data := &storage.BFloat16VectorFieldData{
			Data: []byte{},
		}

		for _, c := range content {
			r, ok := c.([]byte)
			if !ok {
				return nil, errTransferType
			}
			data.Data = append(data.Data, r...)
		}

		data.Dim = len(data.Data) / 2 / int(numRows)
		rst = data

	case schemapb.DataType_BinaryVector:
		data := &storage.BinaryVectorFieldData{
			Data: []byte{},
		}

		for _, c := range content {
			r, ok := c.([]byte)
			if !ok {
				return nil, errTransferType
			}
			data.Data = append(data.Data, r...)
		}

		data.Dim = len(data.Data) * 8 / int(numRows)
		rst = data

	default:
		return nil, errUnknownDataType
	}

	return rst, nil
}

func (t *compactionTask) getSegmentMeta(segID UniqueID) (UniqueID, UniqueID, *etcdpb.CollectionMeta, error) {
	collID := t.metaCache.Collection()
	seg, ok := t.metaCache.GetSegmentByID(segID)
	if !ok {
		return -1, -1, nil, merr.WrapErrSegmentNotFound(segID)
	}
	partID := seg.PartitionID()
	sch := t.metaCache.Schema()

	meta := &etcdpb.CollectionMeta{
		ID:     collID,
		Schema: sch,
	}
	return collID, partID, meta, nil
}

func (t *compactionTask) getCollection() UniqueID {
	return t.metaCache.Collection()
}

func (t *compactionTask) GetCurrentTime() typeutil.Timestamp {
	return tsoutil.GetCurrentTime()
}

func (t *compactionTask) isExpiredEntity(ts, now Timestamp) bool {
	// entity expire is not enabled if duration <= 0
	if t.plan.GetCollectionTtl() <= 0 {
		return false
	}

	pts, _ := tsoutil.ParseTS(ts)
	pnow, _ := tsoutil.ParseTS(now)
	expireTime := pts.Add(time.Duration(t.plan.GetCollectionTtl()))
	return expireTime.Before(pnow)
}
