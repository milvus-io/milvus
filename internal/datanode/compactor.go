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
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
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
	start()
	complete()
	compact() (*datapb.CompactionResult, error)
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
	Replica
	flushManager
	allocatorInterface

	plan *datapb.CompactionPlan

	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup
	tr *timerecord.TimeRecorder
}

// check if compactionTask implements compactor
var _ compactor = (*compactionTask)(nil)

func newCompactionTask(
	ctx context.Context,
	dl downloader,
	ul uploader,
	replica Replica,
	fm flushManager,
	alloc allocatorInterface,
	plan *datapb.CompactionPlan) *compactionTask {

	ctx1, cancel := context.WithCancel(ctx)
	return &compactionTask{
		ctx:    ctx1,
		cancel: cancel,

		downloader:         dl,
		uploader:           ul,
		Replica:            replica,
		flushManager:       fm,
		allocatorInterface: alloc,
		plan:               plan,
		tr:                 timerecord.NewTimeRecorder("compactionTask"),
	}
}

func (t *compactionTask) start() {
	t.wg.Add(1)
}

func (t *compactionTask) complete() {
	t.wg.Done()
}

func (t *compactionTask) stop() {
	t.cancel()
	t.wg.Wait()
}

func (t *compactionTask) getPlanID() UniqueID {
	return t.plan.GetPlanID()
}

func (t *compactionTask) getChannelName() string {
	return t.plan.GetChannel()
}

func (t *compactionTask) getPlanTargetEntryNumber() int64 {
	if t.plan == nil {
		// if plan empty return default size
		return int64(bloomFilterSize)
	}
	var result int64
	for _, info := range t.plan.GetSegmentBinlogs() {
		for _, fieldLog := range info.GetFieldBinlogs() {
			for _, binlog := range fieldLog.GetBinlogs() {
				result += binlog.GetEntriesNum()
			}
		}
	}

	// prevent bloom filter too small
	if result == 0 {
		return int64(bloomFilterSize)
	}
	return result
}

func (t *compactionTask) mergeDeltalogs(dBlobs map[UniqueID][]*Blob, timetravelTs Timestamp) (map[interface{}]Timestamp, *DelDataBuf, error) {
	mergeStart := time.Now()
	dCodec := storage.NewDeleteCodec()

	var (
		pk2ts = make(map[interface{}]Timestamp)
		dbuff = &DelDataBuf{
			delData: &DeleteData{
				Pks: make([]primaryKey, 0),
				Tss: make([]Timestamp, 0)},
			Binlog: datapb.Binlog{
				TimestampFrom: math.MaxUint64,
				TimestampTo:   0,
			},
		}
	)

	for _, blobs := range dBlobs {
		_, _, dData, err := dCodec.Deserialize(blobs)
		if err != nil {
			log.Warn("merge deltalogs wrong", zap.Error(err))
			return nil, nil, err
		}

		for i := int64(0); i < dData.RowCount; i++ {
			pk := dData.Pks[i]
			ts := dData.Tss[i]

			if timetravelTs != Timestamp(0) && dData.Tss[i] <= timetravelTs {
				pk2ts[pk.GetValue()] = ts
				continue
			}

			dbuff.delData.Append(pk, ts)

			if ts < dbuff.TimestampFrom {
				dbuff.TimestampFrom = ts
			}

			if ts > dbuff.TimestampTo {
				dbuff.TimestampTo = ts
			}
		}
	}

	dbuff.updateSize(dbuff.delData.RowCount)
	log.Debug("mergeDeltalogs end", zap.Int64("PlanID", t.getPlanID()),
		zap.Int("number of pks to compact in insert logs", len(pk2ts)),
		zap.Float64("elapse in ms", nano2Milli(time.Since(mergeStart))))

	return pk2ts, dbuff, nil
}

// nano2Milli transfers nanoseconds to milliseconds in unit
func nano2Milli(nano time.Duration) float64 {
	return float64(nano) / float64(time.Millisecond)
}

func (t *compactionTask) merge(mergeItr iterator, delta map[interface{}]Timestamp, schema *schemapb.CollectionSchema, currentTs Timestamp) ([]*InsertData, []byte, int64, error) {
	mergeStart := time.Now()

	var (
		dim              int   // dimension of float/binary vector field
		maxRowsPerBinlog int   // maximum rows populating one binlog
		numBinlogs       int   // binlog number
		expired          int64 // the number of expired entity
		err              error

		// statslog generation
		segment *Segment // empty segment used for bf generation
		pkID    UniqueID
		pkType  schemapb.DataType

		iDatas      = make([]*InsertData, 0)
		fID2Type    = make(map[UniqueID]schemapb.DataType)
		fID2Content = make(map[UniqueID][]interface{})
	)

	isDeletedValue := func(v *storage.Value) bool {
		ts, ok := delta[v.PK.GetValue()]
		if ok && uint64(v.Timestamp) <= ts {
			return true
		}
		return false
	}

	//
	targetRowCount := t.getPlanTargetEntryNumber()
	log.Debug("merge estimate target row count", zap.Int64("row count", targetRowCount))
	segment = &Segment{
		pkFilter: bloom.NewWithEstimates(uint(targetRowCount), maxBloomFalsePositive),
	}

	// get dim
	for _, fs := range schema.GetFields() {
		fID2Type[fs.GetFieldID()] = fs.GetDataType()
		if fs.GetIsPrimaryKey() {
			pkID = fs.GetFieldID()
			pkType = fs.GetDataType()
		}
		if fs.GetDataType() == schemapb.DataType_FloatVector ||
			fs.GetDataType() == schemapb.DataType_BinaryVector {
			for _, t := range fs.GetTypeParams() {
				if t.Key == "dim" {
					if dim, err = strconv.Atoi(t.Value); err != nil {
						log.Warn("strconv wrong on get dim", zap.Error(err))
						return nil, nil, 0, err
					}
					break
				}
			}
		}
	}

	expired = 0
	for mergeItr.HasNext() {
		//  no error if HasNext() returns true
		vInter, _ := mergeItr.Next()

		v, ok := vInter.(*storage.Value)
		if !ok {
			log.Warn("transfer interface to Value wrong")
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

		row, ok := v.Value.(map[UniqueID]interface{})
		if !ok {
			log.Warn("transfer interface to map wrong")
			return nil, nil, 0, errors.New("unexpected error")
		}

		for fID, vInter := range row {
			if _, ok := fID2Content[fID]; !ok {
				fID2Content[fID] = make([]interface{}, 0)
			}
			fID2Content[fID] = append(fID2Content[fID], vInter)
		}
	}

	// calculate numRows from rowID field, fieldID 0
	numRows := int64(len(fID2Content[0]))
	maxRowsPerBinlog = int(Params.DataNodeCfg.FlushInsertBufferSize / (int64(dim) * 4))
	numBinlogs = int(numRows) / maxRowsPerBinlog
	if int(numRows)%maxRowsPerBinlog != 0 {
		numBinlogs++
	}

	for i := 0; i < numBinlogs; i++ {
		iDatas = append(iDatas, &InsertData{Data: make(map[storage.FieldID]storage.FieldData)})
	}

	for fID, content := range fID2Content {
		tp, ok := fID2Type[fID]
		if !ok {
			log.Warn("no field ID in this schema", zap.Int64("fieldID", fID))
			return nil, nil, 0, errors.New("Unexpected error")
		}

		for i := 0; i < numBinlogs; i++ {
			var c []interface{}

			if i == numBinlogs-1 {
				c = content[i*maxRowsPerBinlog:]
			} else {
				c = content[i*maxRowsPerBinlog : i*maxRowsPerBinlog+maxRowsPerBinlog]
			}

			fData, err := interface2FieldData(tp, c, int64(len(c)))

			if err != nil {
				log.Warn("transfer interface to FieldData wrong", zap.Error(err))
				return nil, nil, 0, err
			}
			if fID == pkID {
				err = segment.updatePKRange(fData)
				if err != nil {
					log.Warn("update pk range failed", zap.Error(err))
					return nil, nil, 0, err
				}
			}
			iDatas[i].Data[fID] = fData
		}
	}

	// marshal segment statslog
	segStats, err := segment.getSegmentStatslog(pkID, pkType)
	if err != nil {
		log.Warn("failed to generate segment statslog", zap.Int64("pkID", pkID), zap.Error(err))
		return nil, nil, 0, err
	}

	log.Debug("merge end", zap.Int64("planID", t.getPlanID()), zap.Int64("remaining insert numRows", numRows),
		zap.Int64("expired entities", expired),
		zap.Float64("elapse in ms", nano2Milli(time.Since(mergeStart))))
	return iDatas, segStats, numRows, nil
}

func (t *compactionTask) compact() (*datapb.CompactionResult, error) {
	compactStart := time.Now()
	if ok := funcutil.CheckCtxValid(t.ctx); !ok {
		log.Error("compact wrong, task context done or timeout")
		return nil, errContext
	}

	ctxTimeout, cancelAll := context.WithTimeout(t.ctx, time.Duration(t.plan.GetTimeoutInSeconds())*time.Second)
	defer cancelAll()

	var targetSegID UniqueID
	var err error
	switch {

	case t.plan.GetType() == datapb.CompactionType_UndefinedCompaction:
		log.Error("compact wrong, compaction type undefined")
		return nil, errCompactionTypeUndifined

	case len(t.plan.GetSegmentBinlogs()) < 1:
		log.Error("compact wrong, there's no segments in segment binlogs")
		return nil, errIllegalCompactionPlan

	case t.plan.GetType() == datapb.CompactionType_MergeCompaction || t.plan.GetType() == datapb.CompactionType_MixCompaction:
		targetSegID, err = t.allocID()
		if err != nil {
			log.Error("compact wrong", zap.Error(err))
			return nil, err
		}

	case t.plan.GetType() == datapb.CompactionType_InnerCompaction:
		targetSegID = t.plan.GetSegmentBinlogs()[0].GetSegmentID()
	}

	log.Debug("compaction start", zap.Int64("planID", t.plan.GetPlanID()), zap.Int32("timeout in seconds", t.plan.GetTimeoutInSeconds()))
	segIDs := make([]UniqueID, 0, len(t.plan.GetSegmentBinlogs()))
	for _, s := range t.plan.GetSegmentBinlogs() {
		segIDs = append(segIDs, s.GetSegmentID())
	}

	collID, partID, meta, err := t.getSegmentMeta(segIDs[0])
	if err != nil {
		log.Error("compact wrong", zap.Int64("planID", t.plan.GetPlanID()), zap.Error(err))
		return nil, err
	}

	// Inject to stop flush
	injectStart := time.Now()
	ti := newTaskInjection(len(segIDs), func(pack *segmentFlushPack) {
		pack.segmentID = targetSegID
	})
	defer close(ti.injectOver)

	t.injectFlush(ti, segIDs...)
	<-ti.Injected()
	injectEnd := time.Now()
	defer func() {
		log.Debug("inject elapse in ms", zap.Int64("planID", t.plan.GetPlanID()), zap.Float64("elapse", nano2Milli(injectEnd.Sub(injectStart))))
	}()

	var (
		iItr = make([]iterator, 0)
		imu  sync.Mutex

		// SegmentID to deltaBlobs
		dblobs = make(map[UniqueID][]*Blob)
		dmu    sync.Mutex

		PKfieldID UniqueID
		PkType    schemapb.DataType
	)

	// Get PK fieldID
	for _, fs := range meta.GetSchema().GetFields() {
		if fs.GetFieldID() >= 100 && typeutil.IsPrimaryFieldType(fs.GetDataType()) && fs.GetIsPrimaryKey() {
			PKfieldID = fs.GetFieldID()
			PkType = fs.GetDataType()
			break
		}
	}

	downloadStart := time.Now()
	g, gCtx := errgroup.WithContext(ctxTimeout)
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
			log.Error("compact wrong, all segments' binlogs are empty", zap.Int64("planID", t.plan.GetPlanID()))
			return nil, errIllegalCompactionPlan
		}

		for idx := 0; idx < binlogNum; idx++ {
			var ps []string
			for _, f := range s.GetFieldBinlogs() {
				ps = append(ps, f.GetBinlogs()[idx].GetLogPath())
			}

			g.Go(func() error {
				bs, err := t.download(gCtx, ps)
				if err != nil {
					log.Warn("download insertlogs wrong")
					return err
				}

				itr, err := storage.NewInsertBinlogIterator(bs, PKfieldID, PkType)
				if err != nil {
					log.Warn("new insert binlogs Itr wrong")
					return err
				}

				imu.Lock()
				iItr = append(iItr, itr)
				imu.Unlock()

				return nil
			})
		}

		segID := s.GetSegmentID()
		for _, d := range s.GetDeltalogs() {
			for _, l := range d.GetBinlogs() {
				path := l.GetLogPath()
				g.Go(func() error {
					bs, err := t.download(gCtx, []string{path})
					if err != nil {
						log.Warn("download deltalogs wrong")
						return err
					}

					dmu.Lock()
					dblobs[segID] = append(dblobs[segID], bs...)
					dmu.Unlock()

					return nil
				})
			}
		}
	}

	err = g.Wait()
	downloadEnd := time.Now()
	defer func() {
		log.Debug("download elapse in ms", zap.Int64("planID", t.plan.GetPlanID()), zap.Float64("elapse", nano2Milli(downloadEnd.Sub(downloadStart))))
	}()

	if err != nil {
		log.Error("compaction IO wrong", zap.Int64("planID", t.plan.GetPlanID()), zap.Error(err))
		return nil, err
	}

	mergeItr := storage.NewMergeIterator(iItr)

	deltaPk2Ts, deltaBuf, err := t.mergeDeltalogs(dblobs, t.plan.GetTimetravel())
	if err != nil {
		return nil, err
	}

	iDatas, segStats, numRows, err := t.merge(mergeItr, deltaPk2Ts, meta.GetSchema(), t.GetCurrentTime())
	if err != nil {
		log.Error("compact wrong", zap.Int64("planID", t.plan.GetPlanID()), zap.Error(err))
		return nil, err
	}

	uploadStart := time.Now()
	segPaths, err := t.upload(ctxTimeout, targetSegID, partID, iDatas, segStats, deltaBuf.delData, meta)
	if err != nil {
		log.Error("compact wrong", zap.Int64("planID", t.plan.GetPlanID()), zap.Error(err))
		return nil, err
	}

	uploadEnd := time.Now()
	defer func() {
		log.Debug("upload elapse in ms", zap.Int64("planID", t.plan.GetPlanID()), zap.Float64("elapse", nano2Milli(uploadEnd.Sub(uploadStart))))
	}()

	for _, fbl := range segPaths.deltaInfo {
		for _, deltaLogInfo := range fbl.GetBinlogs() {
			deltaLogInfo.LogSize = deltaBuf.GetLogSize()
			deltaLogInfo.TimestampFrom = deltaBuf.GetTimestampFrom()
			deltaLogInfo.TimestampTo = deltaBuf.GetTimestampTo()
			deltaLogInfo.EntriesNum = deltaBuf.GetEntriesNum()
		}
	}

	pack := &datapb.CompactionResult{
		PlanID:              t.plan.GetPlanID(),
		SegmentID:           targetSegID,
		InsertLogs:          segPaths.inPaths,
		Field2StatslogPaths: segPaths.statsPaths,
		Deltalogs:           segPaths.deltaInfo,
		NumOfRows:           numRows,
	}

	// rpcStart := time.Now()
	// status, err := t.dc.CompleteCompaction(ctxTimeout, pack)
	// if err != nil {
	// 	log.Error("complete compaction rpc wrong", zap.Int64("planID", t.plan.GetPlanID()), zap.Error(err))
	// 	return err
	// }
	// if status.ErrorCode != commonpb.ErrorCode_Success {
	// 	log.Error("complete compaction wrong", zap.Int64("planID", t.plan.GetPlanID()), zap.String("reason", status.GetReason()))
	// 	return fmt.Errorf("complete comapction wrong: %s", status.GetReason())
	// }
	// rpcEnd := time.Now()
	// defer func() {
	// 	log.Debug("rpc elapse in ms", zap.Int64("planID", t.plan.GetPlanID()), zap.Float64("elapse", nano2Milli(rpcEnd.Sub(rpcStart))))
	// }()
	//
	//  Compaction I: update pk range.
	//  Compaction II: remove the segments and add a new flushed segment with pk range.
	if t.hasSegment(targetSegID, true) {
		if numRows <= 0 {
			t.removeSegments(targetSegID)
		} else {
			t.refreshFlushedSegStatistics(targetSegID, numRows)
		}
		// no need to shorten the PK range of a segment, deleting dup PKs is valid
	} else {
		err = t.mergeFlushedSegments(targetSegID, collID, partID, t.plan.GetPlanID(), segIDs, t.plan.GetChannel(), numRows)
		if err != nil {
			log.Error("compact wrong", zap.Int64("planID", t.plan.GetPlanID()), zap.Error(err))
			return nil, err
		}
	}

	uninjectStart := time.Now()
	ti.injectDone(true)
	uninjectEnd := time.Now()
	defer func() {
		log.Debug("uninject elapse in ms", zap.Int64("planID", t.plan.GetPlanID()), zap.Float64("elapse", nano2Milli(uninjectEnd.Sub(uninjectStart))))
	}()

	log.Info("compaction done",
		zap.Int64("planID", t.plan.GetPlanID()),
		zap.Int64("targetSegmentID", targetSegID),
		zap.Int("num of binlog paths", len(segPaths.inPaths)),
		zap.Int("num of stats paths", len(segPaths.statsPaths)),
		zap.Int("num of delta paths", len(segPaths.deltaInfo)),
	)

	log.Info("overall elapse in ms", zap.Int64("planID", t.plan.GetPlanID()), zap.Float64("elapse", nano2Milli(time.Since(compactStart))))
	metrics.DataNodeCompactionLatency.WithLabelValues(fmt.Sprint(Params.DataNodeCfg.GetNodeID())).Observe(float64(t.tr.ElapseSpan().Milliseconds()))

	return pack, nil
}

// TODO copy maybe expensive, but this seems to be the only convinent way.
func interface2FieldData(schemaDataType schemapb.DataType, content []interface{}, numRows int64) (storage.FieldData, error) {
	var rst storage.FieldData
	numOfRows := []int64{numRows}
	switch schemaDataType {
	case schemapb.DataType_Bool:
		var data = &storage.BoolFieldData{
			NumRows: numOfRows,
			Data:    make([]bool, 0, len(content)),
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
		var data = &storage.Int8FieldData{
			NumRows: numOfRows,
			Data:    make([]int8, 0, len(content)),
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
		var data = &storage.Int16FieldData{
			NumRows: numOfRows,
			Data:    make([]int16, 0, len(content)),
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
		var data = &storage.Int32FieldData{
			NumRows: numOfRows,
			Data:    make([]int32, 0, len(content)),
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
		var data = &storage.Int64FieldData{
			NumRows: numOfRows,
			Data:    make([]int64, 0, len(content)),
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
		var data = &storage.FloatFieldData{
			NumRows: numOfRows,
			Data:    make([]float32, 0, len(content)),
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
		var data = &storage.DoubleFieldData{
			NumRows: numOfRows,
			Data:    make([]float64, 0, len(content)),
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
		var data = &storage.StringFieldData{
			NumRows: numOfRows,
			Data:    make([]string, 0, len(content)),
		}

		for _, c := range content {
			r, ok := c.(string)
			if !ok {
				return nil, errTransferType
			}
			data.Data = append(data.Data, r)
		}
		rst = data

	case schemapb.DataType_FloatVector:
		var data = &storage.FloatVectorFieldData{
			NumRows: numOfRows,
			Data:    []float32{},
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

	case schemapb.DataType_BinaryVector:
		var data = &storage.BinaryVectorFieldData{
			NumRows: numOfRows,
			Data:    []byte{},
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
	collID, partID, err := t.getCollectionAndPartitionID(segID)
	if err != nil {
		return -1, -1, nil, err
	}

	// TODO current compaction timestamp replace zero? why?
	//  Bad desgin of describe collection.
	sch, err := t.getCollectionSchema(collID, 0)
	if err != nil {
		return -1, -1, nil, err
	}

	meta := &etcdpb.CollectionMeta{
		ID:     collID,
		Schema: sch,
	}
	return collID, partID, meta, nil
}

func (t *compactionTask) getCollection() UniqueID {
	return t.getCollectionID()
}

func (t *compactionTask) GetCurrentTime() typeutil.Timestamp {
	return tsoutil.GetCurrentTime()
}

func (t *compactionTask) isExpiredEntity(ts, now Timestamp) bool {
	// entity expire is not enabled if duration <= 0
	if Params.CommonCfg.EntityExpirationTTL <= 0 {
		return false
	}

	pts, _ := tsoutil.ParseTS(ts)
	pnow, _ := tsoutil.ParseTS(now)
	expireTime := pts.Add(Params.CommonCfg.EntityExpirationTTL)
	return expireTime.Before(pnow)
}
