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
	"math"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/allocator"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
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
	compact() (*datapb.CompactionResult, error)
	injectDone(success bool)
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
	Channel
	flushManager
	allocator.Allocator

	plan *datapb.CompactionPlan

	ctx    context.Context
	cancel context.CancelFunc

	done         chan struct{}
	tr           *timerecord.TimeRecorder
	chunkManager storage.ChunkManager
	inject       *taskInjection
}

// check if compactionTask implements compactor
var _ compactor = (*compactionTask)(nil)

func newCompactionTask(
	ctx context.Context,
	dl downloader,
	ul uploader,
	channel Channel,
	fm flushManager,
	alloc allocator.Allocator,
	plan *datapb.CompactionPlan,
	chunkManager storage.ChunkManager) *compactionTask {

	ctx1, cancel := context.WithCancel(ctx)
	return &compactionTask{
		ctx:    ctx1,
		cancel: cancel,

		downloader:   dl,
		uploader:     ul,
		Channel:      channel,
		flushManager: fm,
		Allocator:    alloc,
		plan:         plan,
		tr:           timerecord.NewTimeRecorder("compactionTask"),
		chunkManager: chunkManager,
		done:         make(chan struct{}, 1),
	}
}

func (t *compactionTask) complete() {
	t.done <- struct{}{}
}

func (t *compactionTask) stop() {
	t.cancel()
	<-t.done
	t.injectDone(true)
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
		seg := t.Channel.getSegment(binlog.GetSegmentID())
		if seg == nil {
			return 0, merr.WrapErrSegmentNotFound(binlog.GetSegmentID(), "get compaction segments num rows failed")
		}
		numRows += seg.numRows
	}

	return numRows, nil
}

func (t *compactionTask) mergeDeltalogs(dBlobs map[UniqueID][]*Blob, timetravelTs Timestamp) (
	map[interface{}]Timestamp, *DelDataBuf, error) {
	log := log.With(zap.Int64("planID", t.getPlanID()))
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

	dbuff.accumulateEntriesNum(dbuff.delData.RowCount)
	log.Info("mergeDeltalogs end",
		zap.Int("number of deleted pks to compact in insert logs", len(pk2ts)),
		zap.Float64("elapse in ms", nano2Milli(time.Since(mergeStart))))

	return pk2ts, dbuff, nil
}

// nano2Milli transfers nanoseconds to milliseconds in unit
func nano2Milli(nano time.Duration) float64 {
	return float64(nano) / float64(time.Millisecond)
}

func (t *compactionTask) uploadRemainLog(
	ctxTimeout context.Context,
	targetSegID UniqueID,
	partID UniqueID,
	meta *etcdpb.CollectionMeta,
	stats *storage.PrimaryKeyStats,
	totRows int64,
	fID2Content map[UniqueID][]interface{},
	fID2Type map[UniqueID]schemapb.DataType) (map[UniqueID]*datapb.FieldBinlog, map[UniqueID]*datapb.FieldBinlog, error) {
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
	fID2Type map[UniqueID]schemapb.DataType) (map[UniqueID]*datapb.FieldBinlog, error) {
	iData := &InsertData{
		Data: make(map[storage.FieldID]storage.FieldData)}

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
	delta map[interface{}]Timestamp) ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, int64, error) {
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

	addInsertFieldPath := func(inPaths map[UniqueID]*datapb.FieldBinlog) {
		for fID, path := range inPaths {
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

	stats := storage.NewPrimaryKeyStats(pkID, int64(pkType), oldRowNums)

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
			//update pk to new stats log
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
				addInsertFieldPath(inPaths)

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
		addInsertFieldPath(inPaths)
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

	log.Info("merge end", zap.Int64("remaining insert numRows", numRows),
		zap.Int64("expired entities", expired), zap.Int("binlog file number", numBinlogs),
		zap.Float64("download insert log elapse in ms", nano2Milli(downloadTimeCost)),
		zap.Float64("upload insert log elapse in ms", nano2Milli(uploadInsertTimeCost)),
		zap.Float64("merge elapse in ms", nano2Milli(time.Since(mergeStart))))

	return insertPaths, statPaths, numRows, nil
}

func (t *compactionTask) compact() (*datapb.CompactionResult, error) {
	compactStart := time.Now()
	if ok := funcutil.CheckCtxValid(t.ctx); !ok {
		log.Warn("compact wrong, task context done or timeout")
		return nil, errContext
	}

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

	log.Info("compaction start", zap.Int64("planID", t.plan.GetPlanID()), zap.Int32("timeout in seconds", t.plan.GetTimeoutInSeconds()))
	segIDs := make([]UniqueID, 0, len(t.plan.GetSegmentBinlogs()))
	for _, s := range t.plan.GetSegmentBinlogs() {
		segIDs = append(segIDs, s.GetSegmentID())
	}

	_, partID, meta, err := t.getSegmentMeta(segIDs[0])
	if err != nil {
		log.Warn("compact wrong", zap.Int64("planID", t.plan.GetPlanID()), zap.Error(err))
		return nil, err
	}

	// Inject to stop flush
	injectStart := time.Now()
	ti := newTaskInjection(len(segIDs), func(pack *segmentFlushPack) {
		collectionID := meta.GetID()
		pack.segmentID = targetSegID
		for _, insertLog := range pack.insertLogs {
			splits := strings.Split(insertLog.LogPath, "/")
			if len(splits) < 2 {
				pack.err = fmt.Errorf("bad insert log path: %s", insertLog.LogPath)
				return
			}
			logID, err := strconv.ParseInt(splits[len(splits)-1], 10, 64)
			if err != nil {
				pack.err = err
				return
			}
			fieldID, err := strconv.ParseInt(splits[len(splits)-2], 10, 64)
			if err != nil {
				pack.err = err
				return
			}
			blobKey := metautil.JoinIDPath(collectionID, partID, targetSegID, fieldID, logID)
			blobPath := path.Join(t.chunkManager.RootPath(), common.SegmentInsertLogPath, blobKey)
			blob, err := t.chunkManager.Read(t.ctx, insertLog.LogPath)
			if err != nil {
				pack.err = err
				return
			}
			err = t.chunkManager.Write(t.ctx, blobPath, blob)
			if err != nil {
				pack.err = err
				return
			}
			insertLog.LogPath = blobPath
		}

		for _, deltaLog := range pack.deltaLogs {
			splits := strings.Split(deltaLog.LogPath, "/")
			if len(splits) < 1 {
				pack.err = fmt.Errorf("delta stats log path: %s", deltaLog.LogPath)
				return
			}
			logID, err := strconv.ParseInt(splits[len(splits)-1], 10, 64)
			if err != nil {
				pack.err = err
				return
			}
			blobKey := metautil.JoinIDPath(collectionID, partID, targetSegID, logID)
			blobPath := path.Join(t.chunkManager.RootPath(), common.SegmentDeltaLogPath, blobKey)
			blob, err := t.chunkManager.Read(t.ctx, deltaLog.LogPath)
			if err != nil {
				pack.err = err
				return
			}
			err = t.chunkManager.Write(t.ctx, blobPath, blob)
			if err != nil {
				pack.err = err
				return
			}
			deltaLog.LogPath = blobPath
		}

		for _, statsLog := range pack.statsLogs {
			splits := strings.Split(statsLog.LogPath, "/")
			if len(splits) < 2 {
				pack.err = fmt.Errorf("bad stats log path: %s", statsLog.LogPath)
				return
			}
			logID, err := strconv.ParseInt(splits[len(splits)-1], 10, 64)
			if err != nil {
				pack.err = err
				return
			}
			fieldID, err := strconv.ParseInt(splits[len(splits)-2], 10, 64)
			if err != nil {
				pack.err = err
				return
			}
			blobKey := metautil.JoinIDPath(collectionID, partID, targetSegID, fieldID, logID)
			blobPath := path.Join(t.chunkManager.RootPath(), common.SegmentStatslogPath, blobKey)

			blob, err := t.chunkManager.Read(t.ctx, statsLog.LogPath)
			if err != nil {
				pack.err = err
				return
			}
			err = t.chunkManager.Write(t.ctx, blobPath, blob)
			if err != nil {
				pack.err = err
				return
			}
			statsLog.LogPath = blobPath
		}
	})
	defer func() {
		// the injection will be closed if fail to compact
		if t.inject == nil {
			close(ti.injectOver)
		}
	}()

	t.injectFlush(ti, segIDs...)
	<-ti.Injected()
	injectEnd := time.Now()
	defer func() {
		log.Info("inject elapse in ms", zap.Int64("planID", t.plan.GetPlanID()), zap.Float64("elapse", nano2Milli(injectEnd.Sub(injectStart))))
	}()

	var (
		// SegmentID to deltaBlobs
		dblobs = make(map[UniqueID][]*Blob)
		dmu    sync.Mutex
	)

	allPath := make([][]string, 0)

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
			log.Warn("compact wrong, all segments' binlogs are empty", zap.Int64("planID", t.plan.GetPlanID()))
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
		for _, d := range s.GetDeltalogs() {
			for _, l := range d.GetBinlogs() {
				path := l.GetLogPath()
				g.Go(func() error {
					bs, err := t.download(gCtx, []string{path})
					if err != nil {
						log.Warn("download deltalogs wrong", zap.String("path", path), zap.Error(err))
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
		log.Info("download deltalogs elapse in ms", zap.Int64("planID", t.plan.GetPlanID()), zap.Float64("elapse", nano2Milli(downloadEnd.Sub(downloadStart))))
	}()

	if err != nil {
		log.Warn("compaction IO wrong", zap.Int64("planID", t.plan.GetPlanID()), zap.Error(err))
		return nil, err
	}

	deltaPk2Ts, deltaBuf, err := t.mergeDeltalogs(dblobs, t.plan.GetTimetravel())
	if err != nil {
		return nil, err
	}

	inPaths, statsPaths, numRows, err := t.merge(ctxTimeout, allPath, targetSegID, partID, meta, deltaPk2Ts)
	if err != nil {
		log.Warn("compact wrong", zap.Int64("planID", t.plan.GetPlanID()), zap.Error(err))
		return nil, err
	}

	uploadDeltaStart := time.Now()
	deltaInfo, err := t.uploadDeltaLog(ctxTimeout, targetSegID, partID, deltaBuf.delData, meta)
	if err != nil {
		log.Warn("compact wrong", zap.Int64("planID", t.plan.GetPlanID()), zap.Error(err))
		return nil, err
	}
	log.Info("upload delta log elapse in ms", zap.Int64("planID", t.plan.GetPlanID()), zap.Float64("elapse", nano2Milli(time.Since(uploadDeltaStart))))

	for _, fbl := range deltaInfo {
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
		InsertLogs:          inPaths,
		Field2StatslogPaths: statsPaths,
		Deltalogs:           deltaInfo,
		NumOfRows:           numRows,
		Channel:             t.plan.GetChannel(),
	}

	t.inject = ti

	log.Info("compaction done",
		zap.Int64("planID", t.plan.GetPlanID()),
		zap.Int64("targetSegmentID", targetSegID),
		zap.Int64s("compactedFrom", segIDs),
		zap.Int("num of binlog paths", len(inPaths)),
		zap.Int("num of stats paths", len(statsPaths)),
		zap.Int("num of delta paths", len(deltaInfo)),
	)

	log.Info("overall elapse in ms", zap.Int64("planID", t.plan.GetPlanID()), zap.Float64("elapse", nano2Milli(time.Since(compactStart))))
	metrics.DataNodeCompactionLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Observe(float64(t.tr.ElapseSpan().Milliseconds()))

	return pack, nil
}

func (t *compactionTask) injectDone(success bool) {
	if t.inject != nil {
		uninjectStart := time.Now()
		t.inject.injectDone(success)
		uninjectEnd := time.Now()
		log.Info("uninject elapse in ms", zap.Int64("planID", t.plan.GetPlanID()), zap.Float64("elapse", nano2Milli(uninjectEnd.Sub(uninjectStart))))
	}
}

// TODO copy maybe expensive, but this seems to be the only convinent way.
func interface2FieldData(schemaDataType schemapb.DataType, content []interface{}, numRows int64) (storage.FieldData, error) {
	var rst storage.FieldData
	switch schemaDataType {
	case schemapb.DataType_Bool:
		var data = &storage.BoolFieldData{
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
		var data = &storage.Int8FieldData{
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
		var data = &storage.Int16FieldData{
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
		var data = &storage.Int32FieldData{
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
		var data = &storage.Int64FieldData{
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
		var data = &storage.FloatFieldData{
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
		var data = &storage.DoubleFieldData{
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
		var data = &storage.StringFieldData{
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
		var data = &storage.JSONFieldData{
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

	case schemapb.DataType_FloatVector:
		var data = &storage.FloatVectorFieldData{
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

	case schemapb.DataType_BinaryVector:
		var data = &storage.BinaryVectorFieldData{
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
	if t.plan.GetCollectionTtl() <= 0 {
		return false
	}

	pts, _ := tsoutil.ParseTS(ts)
	pnow, _ := tsoutil.ParseTS(now)
	expireTime := pts.Add(time.Duration(t.plan.GetCollectionTtl()))
	return expireTime.Before(pnow)
}
