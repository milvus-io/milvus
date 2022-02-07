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

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/funcutil"
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
	compact() error
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

	dc   types.DataCoord
	plan *datapb.CompactionPlan

	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup
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
	dc types.DataCoord,
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
		dc:                 dc,
		plan:               plan,
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

func (t *compactionTask) mergeDeltalogs(dBlobs map[UniqueID][]*Blob, timetravelTs Timestamp) (map[UniqueID]Timestamp, *DelDataBuf, error) {

	dCodec := storage.NewDeleteCodec()

	var (
		pk2ts = make(map[UniqueID]Timestamp)
		dbuff = &DelDataBuf{
			delData: &DeleteData{
				Pks: make([]UniqueID, 0),
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
				pk2ts[pk] = ts
				continue
			}

			dbuff.delData.Append(pk, ts)

			if Timestamp(ts) < dbuff.TimestampFrom {
				dbuff.TimestampFrom = Timestamp(ts)
			}

			if Timestamp(ts) > dbuff.TimestampTo {
				dbuff.TimestampTo = Timestamp(ts)
			}
		}
	}

	dbuff.updateSize(dbuff.delData.RowCount)
	log.Debug("mergeDeltalogs end", zap.Int64("PlanID", t.getPlanID()),
		zap.Int("number of pks to compact in insert logs", len(pk2ts)))

	return pk2ts, dbuff, nil
}

func (t *compactionTask) merge(mergeItr iterator, delta map[UniqueID]Timestamp, schema *schemapb.CollectionSchema, currentTs Timestamp) ([]*InsertData, int64, error) {

	var (
		dim     int   // dimension of vector field
		num     int   // numOfRows in each binlog
		n       int   // binlog number
		expired int64 // the number of expired entity
		err     error

		iDatas      = make([]*InsertData, 0)
		fID2Type    = make(map[UniqueID]schemapb.DataType)
		fID2Content = make(map[UniqueID][]interface{})
	)

	// get dim
	for _, fs := range schema.GetFields() {
		fID2Type[fs.GetFieldID()] = fs.GetDataType()
		if fs.GetDataType() == schemapb.DataType_FloatVector ||
			fs.GetDataType() == schemapb.DataType_BinaryVector {
			for _, t := range fs.GetTypeParams() {
				if t.Key == "dim" {
					if dim, err = strconv.Atoi(t.Value); err != nil {
						log.Warn("strconv wrong on get dim", zap.Error(err))
						return nil, 0, err
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
			return nil, 0, errors.New("unexpected error")
		}

		if _, ok := delta[v.PK]; ok {
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
			return nil, 0, errors.New("unexpected error")
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
	num = int(Params.DataNodeCfg.FlushInsertBufferSize / (int64(dim) * 4))
	n = int(numRows)/num + 1

	for i := 0; i < n; i++ {
		iDatas = append(iDatas, &InsertData{Data: make(map[storage.FieldID]storage.FieldData)})
	}

	for fID, content := range fID2Content {
		tp, ok := fID2Type[fID]
		if !ok {
			log.Warn("no field ID in this schema", zap.Int64("fieldID", fID))
			return nil, 0, errors.New("Unexpected error")
		}

		for i := 0; i < n; i++ {
			var c []interface{}

			if i == n-1 {
				c = content[i*num:]
			} else {
				c = content[i*num : i*num+num]
			}

			fData, err := interface2FieldData(tp, c, int64(len(c)))

			if err != nil {
				log.Warn("transfer interface to FieldData wrong", zap.Error(err))
				return nil, 0, err
			}
			iDatas[i].Data[fID] = fData
		}

	}

	log.Debug("merge end", zap.Int64("planID", t.getPlanID()), zap.Int64("remaining insert numRows", numRows), zap.Int64("expired entities", expired))
	return iDatas, numRows, nil
}

func (t *compactionTask) compact() error {
	if ok := funcutil.CheckCtxValid(t.ctx); !ok {
		log.Error("compact wrong, task context done or timeout")
		return errContext
	}

	ctxTimeout, cancelAll := context.WithTimeout(t.ctx, time.Duration(t.plan.GetTimeoutInSeconds())*time.Second)
	defer cancelAll()

	var targetSegID UniqueID
	var err error
	switch {

	case t.plan.GetType() == datapb.CompactionType_UndefinedCompaction:
		log.Error("compact wrong, compaction type undefined")
		return errCompactionTypeUndifined

	case len(t.plan.GetSegmentBinlogs()) < 1:
		log.Error("compact wrong, there's no segments in segment binlogs")
		return errIllegalCompactionPlan

	case t.plan.GetType() == datapb.CompactionType_MergeCompaction:
		targetSegID, err = t.allocID()
		if err != nil {
			log.Error("compact wrong", zap.Error(err))
			return err
		}

	case t.plan.GetType() == datapb.CompactionType_InnerCompaction:
		targetSegID = t.plan.GetSegmentBinlogs()[0].GetSegmentID()
	}

	log.Debug("compaction start", zap.Int64("planID", t.plan.GetPlanID()), zap.Any("timeout in seconds", t.plan.GetTimeoutInSeconds()))
	segIDs := make([]UniqueID, 0, len(t.plan.GetSegmentBinlogs()))
	for _, s := range t.plan.GetSegmentBinlogs() {
		segIDs = append(segIDs, s.GetSegmentID())
	}

	collID, partID, meta, err := t.getSegmentMeta(segIDs[0])
	if err != nil {
		log.Error("compact wrong", zap.Int64("planID", t.plan.GetPlanID()), zap.Error(err))
		return err
	}

	// Inject to stop flush
	ti := newTaskInjection(len(segIDs), func(pack *segmentFlushPack) {
		pack.segmentID = targetSegID
	})
	defer close(ti.injectOver)

	t.injectFlush(ti, segIDs...)
	<-ti.Injected()

	var (
		iItr = make([]iterator, 0)
		imu  sync.Mutex

		// SegmentID to deltaBlobs
		dblobs = make(map[UniqueID][]*Blob)
		dmu    sync.Mutex

		PKfieldID UniqueID
	)

	// Get PK fieldID
	for _, fs := range meta.GetSchema().GetFields() {
		if fs.GetFieldID() >= 100 && fs.GetDataType() == schemapb.DataType_Int64 && fs.GetIsPrimaryKey() {
			PKfieldID = fs.GetFieldID()
			break
		}
	}

	g, gCtx := errgroup.WithContext(ctxTimeout)
	for _, s := range t.plan.GetSegmentBinlogs() {

		// TODO may panic
		fieldNum := len(s.GetFieldBinlogs()[0].GetBinlogs())

		for idx := 0; idx < fieldNum; idx++ {
			ps := make([]string, 0, fieldNum)
			for _, f := range s.GetFieldBinlogs() {
				ps = append(ps, f.GetBinlogs()[idx].GetLogPath())
			}

			g.Go(func() error {
				bs, err := t.download(gCtx, ps)
				if err != nil {
					log.Warn("download insertlogs wrong")
					return err
				}

				itr, err := storage.NewInsertBinlogIterator(bs, PKfieldID)
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

	if err := g.Wait(); err != nil {
		log.Error("compaction IO wrong", zap.Int64("planID", t.plan.GetPlanID()), zap.Error(err))
		return err
	}

	mergeItr := storage.NewMergeIterator(iItr)

	deltaPk2Ts, deltaBuf, err := t.mergeDeltalogs(dblobs, t.plan.GetTimetravel())
	if err != nil {
		return err
	}

	iDatas, numRows, err := t.merge(mergeItr, deltaPk2Ts, meta.GetSchema(), t.GetCurrentTime())
	if err != nil {
		log.Error("compact wrong", zap.Int64("planID", t.plan.GetPlanID()), zap.Error(err))
		return err
	}

	cpaths, err := t.upload(ctxTimeout, targetSegID, partID, iDatas, deltaBuf.delData, meta)
	if err != nil {
		log.Error("compact wrong", zap.Int64("planID", t.plan.GetPlanID()), zap.Error(err))
		return err
	}

	for _, fbl := range cpaths.deltaInfo {
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
		InsertLogs:          cpaths.inPaths,
		Field2StatslogPaths: cpaths.statsPaths,
		Deltalogs:           cpaths.deltaInfo,
		NumOfRows:           numRows,
	}

	status, err := t.dc.CompleteCompaction(ctxTimeout, pack)
	if err != nil {
		log.Error("complete compaction rpc wrong", zap.Int64("planID", t.plan.GetPlanID()), zap.Error(err))
		return err
	}
	if status.ErrorCode != commonpb.ErrorCode_Success {
		log.Error("complete compaction wrong", zap.Int64("planID", t.plan.GetPlanID()), zap.String("reason", status.GetReason()))
		return fmt.Errorf("complete comapction wrong: %s", status.GetReason())
	}

	//  Compaction I: update pk range.
	//  Compaction II: remove the segments and add a new flushed segment with pk range.
	if t.hasSegment(targetSegID, true) {
		t.refreshFlushedSegStatistics(targetSegID, numRows)
		// no need to shorten the PK range of a segment, deleting dup PKs is valid
	} else {
		t.mergeFlushedSegments(targetSegID, collID, partID, t.plan.GetPlanID(), segIDs, t.plan.GetChannel(), numRows)
	}

	ti.injectDone(true)
	log.Info("compaction done", zap.Int64("planID", t.plan.GetPlanID()),
		zap.Int("num of binlog paths", len(cpaths.inPaths)),
		zap.Int("num of stats paths", len(cpaths.statsPaths)),
		zap.Int("num of delta paths", len(cpaths.deltaInfo)),
	)
	return nil
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
	const MaxEntityExpiration = 9223372036 // the value was setup by math.MaxInt64 / time.Second
	// Check calculable range of milvus config value
	if Params.DataCoordCfg.CompactionEntityExpiration > MaxEntityExpiration {
		return false
	}

	duration := time.Duration(Params.DataCoordCfg.CompactionEntityExpiration) * time.Second
	// Prevent from duration overflow value
	if duration < 0 {
		return false
	}

	pts, _ := tsoutil.ParseTS(ts)
	pnow, _ := tsoutil.ParseTS(now)
	expireTime := pts.Add(duration)
	return expireTime.Before(pnow)
}
