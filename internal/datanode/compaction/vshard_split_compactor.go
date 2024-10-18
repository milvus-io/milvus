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

package compaction

import (
	"context"
	"fmt"
	sio "io"
	"math"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type vshardSplitCompactionTask struct {
	binlogIO   io.BinlogIO
	logIDAlloc allocator.Interface
	segIDAlloc allocator.Interface
	currentTs  typeutil.Timestamp

	plan *datapb.CompactionPlan

	ctx    context.Context
	cancel context.CancelFunc

	done chan struct{}
	tr   *timerecord.TimeRecorder

	primaryKeyField *schemapb.FieldSchema

	writer *SplitClusterWriter
}

// make sure compactionTask implements compactor interface
var _ Compactor = (*vshardSplitCompactionTask)(nil)

func NewVshardSplitCompactionTask(
	ctx context.Context,
	binlogIO io.BinlogIO,
	plan *datapb.CompactionPlan,
) *vshardSplitCompactionTask {
	ctx1, cancel := context.WithCancel(ctx)
	logIDAlloc := allocator.NewLocalAllocator(plan.GetBeginLogID(), math.MaxInt64)
	segIDAlloc := allocator.NewLocalAllocator(plan.GetPreAllocatedSegmentIDs().GetBegin(), plan.GetPreAllocatedSegmentIDs().GetEnd())
	return &vshardSplitCompactionTask{
		ctx:        ctx1,
		cancel:     cancel,
		binlogIO:   binlogIO,
		logIDAlloc: logIDAlloc,
		segIDAlloc: segIDAlloc,
		plan:       plan,
		tr:         timerecord.NewTimeRecorder("mix compaction"),
		currentTs:  tsoutil.GetCurrentTime(),
		done:       make(chan struct{}, 1),
	}
}

// return num rows of all segment compaction from
func (t *vshardSplitCompactionTask) getNumRows() int64 {
	numRows := int64(0)
	for _, binlog := range t.plan.SegmentBinlogs {
		if len(binlog.GetFieldBinlogs()) > 0 {
			for _, ct := range binlog.GetFieldBinlogs()[0].GetBinlogs() {
				numRows += ct.GetEntriesNum()
			}
		}
	}
	return numRows
}

func (t *vshardSplitCompactionTask) Compact() (*datapb.CompactionPlanResult, error) {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(t.ctx, fmt.Sprintf("MixCompact-%d", t.GetPlanID()))
	defer span.End()

	if len(t.plan.GetSegmentBinlogs()) < 1 {
		log.Warn("compact wrong, there's no segments in segment binlogs", zap.Int64("planID", t.plan.GetPlanID()))
		return nil, errors.New("compaction plan is illegal")
	}
	if len(t.plan.GetVshards()) <= 0 {
		log.Warn("compact wrong, vshardNum should larger than 0", zap.Int64("planID", t.plan.GetPlanID()), zap.Int("vshardNum", len(t.plan.GetVshards())))
		return nil, errors.New("compaction plan is illegal")
	}
	for _, vshard := range t.plan.GetVshards() {
		log.Info("vshardSplitCompactionTask vshards", zap.String("vshard", vshard.String()))
	}

	collectionID := t.plan.GetSegmentBinlogs()[0].GetCollectionID()
	partitionID := t.plan.GetSegmentBinlogs()[0].GetPartitionID()
	segmentIDs := lo.Map(t.plan.GetSegmentBinlogs(), func(segBinlog *datapb.CompactionSegmentBinlogs, _ int) int64 {
		return segBinlog.SegmentID
	})

	log := log.Ctx(ctx).With(zap.Int64("planID", t.plan.GetPlanID()),
		zap.Int64("collectionID", collectionID),
		zap.Int64("partitionID", partitionID))

	if ok := funcutil.CheckCtxValid(ctx); !ok {
		log.Warn("compact wrong, task context done or timeout")
		return nil, ctx.Err()
	}

	ctxTimeout, cancelAll := context.WithTimeout(ctx, time.Duration(t.plan.GetTimeoutInSeconds())*time.Second)
	defer cancelAll()

	log.Info("compact start", zap.Int64s("inputSegmentIDs", segmentIDs))

	deltaPaths, allPath, err := composePaths(t.plan.GetSegmentBinlogs())
	if err != nil {
		log.Warn("fail to merge deltalogs", zap.Error(err))
		return nil, err
	}

	// Unable to deal with all empty segments cases, so return error
	if len(allPath) == 0 {
		log.Warn("compact wrong, all segments' binlogs are empty")
		return nil, errors.New("illegal compaction plan")
	}

	deltaPk2Ts, err := mergeDeltalogs(ctxTimeout, t.binlogIO, deltaPaths)
	if err != nil {
		log.Warn("compact wrong, fail to merge deltalogs", zap.Error(err))
		return nil, err
	}

	var pkField *schemapb.FieldSchema
	if t.plan.Schema == nil {
		return nil, merr.WrapErrIllegalCompactionPlan("empty schema in compactionPlan")
	}
	for _, field := range t.plan.Schema.Fields {
		if field.GetIsPrimaryKey() && field.GetFieldID() >= 100 && typeutil.IsPrimaryFieldType(field.GetDataType()) {
			pkField = field
		}
	}
	t.primaryKeyField = pkField

	mappingFunc := func(value *storage.Value) (string, error) {
		pkHash, err := value.PK.Hash()
		if err != nil {
			return "", err
		}

		for _, vshard := range t.plan.GetVshards() {
			hashValueRes := int32(pkHash % uint32(vshard.GetVshardModulus()))
			if hashValueRes == vshard.GetVshardResidue() {
				bytes, err := proto.Marshal(vshard)
				if err != nil {
					return "", err
				}
				return string(bytes), nil
			}
		}

		return "", errors.New("no matching vshard")
	}

	splitKeys := lo.Map(t.plan.GetVshards(), func(vshard *datapb.VShardDesc, _ int) string {
		bytes, err := proto.Marshal(vshard)
		if err != nil {
			return ""
		}
		return string(bytes)
	})

	splitWriter, err := NewSplitClusterWriterBuilder().
		SetCollectionID(t.GetCollection()).
		SetPartitionID(partitionID).
		SetSchema(t.plan.GetSchema()).
		SetSegmentMaxSize(t.plan.GetMaxSize()).
		// todo use total rows as segment maxRow, if input many segments, this value might be too large
		SetSegmentMaxRowCount(t.plan.GetTotalRows()).
		SetSplitKeys(splitKeys).
		SetAllocator(&compactionAlloactor{
			segmentAlloc: t.segIDAlloc,
			logIDAlloc:   t.logIDAlloc,
		}).
		SetBinlogIO(t.binlogIO).
		SetMemoryBufferSize(math.MaxInt64).
		SetMappingFunc(mappingFunc).
		SetWorkerPoolSize(1).
		Build()
	if err != nil {
		log.Warn("fail to build splits buffer", zap.Error(err))
		return nil, err
	}
	t.writer = splitWriter
	err = t.mapping(ctxTimeout, deltaPk2Ts)
	if err != nil {
		log.Error("fail in process segment", zap.Error(err))
	}

	log.Info("written rows", zap.Int64("rows", t.writer.GetRowNum()))
	uploadSegments, err := t.writer.Finish()
	if err != nil {
		log.Error("fail in finish writer", zap.Error(err))
	}

	resultSegments := make([]*datapb.CompactionSegment, 0)
	for vshardDescStr, segments := range uploadSegments {
		vshardDesc := &datapb.VShardDesc{}
		err := proto.Unmarshal([]byte(vshardDescStr), vshardDesc)
		if err != nil {
			log.Error("fail in unmarshall vshardDesc", zap.String("vshardDesc", vshardDescStr), zap.Error(err))
		}
		for _, segment := range segments {
			segment.Vshard = vshardDesc
			resultSegments = append(resultSegments, segment)
		}
	}

	planResult := &datapb.CompactionPlanResult{
		State:    datapb.CompactionTaskState_completed,
		PlanID:   t.GetPlanID(),
		Channel:  t.GetChannelName(),
		Segments: resultSegments,
		Type:     t.plan.GetType(),
	}
	log.Info("compact end", zap.Int("resultSegments", len(uploadSegments)))

	return planResult, nil
}

// mapping read and split input segments into buffers
func (t *vshardSplitCompactionTask) mapping(ctx context.Context, deltaPk2Ts map[interface{}]typeutil.Timestamp) error {
	// todo support concurrent @wayblink
	// futures := make([]*conc.Future[any], 0, len(inputSegments))
	for _, segment := range t.plan.GetSegmentBinlogs() {
		segmentClone := &datapb.CompactionSegmentBinlogs{
			SegmentID: segment.SegmentID,
			// only FieldBinlogs needed
			FieldBinlogs: segment.FieldBinlogs,
		}

		err := t.mappingSegment(ctx, segmentClone, deltaPk2Ts)
		if err != nil {
			log.Error("fail in process segment", zap.Int64("id", segmentClone.GetSegmentID()))
			return err
		}
		//future := t.mappingPool.Submit(func() (any, error) {
		//	err := t.mappingSegment(ctx, segmentClone, deltaPk2Ts)
		//	return struct{}{}, err
		//})
		//futures = append(futures, future)
	}
	return nil
}

// read insert log of one segment, mappingSegment into buckets according to clusteringKey. flush data to file when necessary
func (t *vshardSplitCompactionTask) mappingSegment(
	ctx context.Context,
	segment *datapb.CompactionSegmentBinlogs,
	delta map[interface{}]typeutil.Timestamp,
) error {
	ctx, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, fmt.Sprintf("mappingSegment-%d-%d", t.GetPlanID(), segment.GetSegmentID()))
	defer span.End()
	log := log.With(zap.Int64("planID", t.GetPlanID()),
		zap.Int64("collectionID", t.GetCollection()),
		zap.Int64("segmentID", segment.GetSegmentID()))
	log.Info("mapping segment start")
	processStart := time.Now()
	fieldBinlogPaths := make([][]string, 0)
	var (
		expired  int64 = 0
		deleted  int64 = 0
		remained int64 = 0
	)

	// Get the number of field binlog files from non-empty segment
	var binlogNum int
	for _, b := range segment.GetFieldBinlogs() {
		if b != nil {
			binlogNum = len(b.GetBinlogs())
			break
		}
	}
	// Unable to deal with all empty segments cases, so return error
	if binlogNum == 0 {
		log.Warn("compact wrong, all segments' binlogs are empty")
		return merr.WrapErrIllegalCompactionPlan()
	}
	for idx := 0; idx < binlogNum; idx++ {
		var ps []string
		for _, f := range segment.GetFieldBinlogs() {
			ps = append(ps, f.GetBinlogs()[idx].GetLogPath())
		}
		fieldBinlogPaths = append(fieldBinlogPaths, ps)
	}

	for _, paths := range fieldBinlogPaths {
		allValues, err := t.binlogIO.Download(ctx, paths)
		if err != nil {
			log.Warn("compact wrong, fail to download insertLogs", zap.Error(err))
			return err
		}
		blobs := lo.Map(allValues, func(v []byte, i int) *storage.Blob {
			return &storage.Blob{Key: paths[i], Value: v}
		})
		pkIter, err := storage.NewBinlogDeserializeReader(blobs, t.primaryKeyField.GetFieldID())
		if err != nil {
			log.Warn("new insert binlogs Itr wrong", zap.Strings("paths", paths), zap.Error(err))
			return err
		}

		var offset int64 = -1
		for {
			err := pkIter.Next()
			if err != nil {
				if err == sio.EOF {
					pkIter.Close()
					break
				} else {
					log.Warn("compact wrong, failed to iter through data", zap.Error(err))
					return err
				}
			}
			v := pkIter.Value()
			offset++

			// Filtering deleted entity
			if isDeletedEntity(v, delta) {
				deleted++
				continue
			}
			// Filtering expired entity
			ts := typeutil.Timestamp(v.Timestamp)
			if isExpiredEntity(t.plan.GetCollectionTtl(), t.currentTs, ts) {
				expired++
				continue
			}

			err = t.writer.Write(v)
			if err != nil {
				return err
			}
			remained++
		}
	}

	log.Info("mapping segment end",
		zap.Int64("remained_entities", remained),
		zap.Int64("deleted_entities", deleted),
		zap.Int64("expired_entities", expired),
		zap.Duration("elapse", time.Since(processStart)))
	return nil
}

func (t *vshardSplitCompactionTask) GetCollection() typeutil.UniqueID {
	// The length of SegmentBinlogs is checked before task enqueueing.
	return t.plan.GetSegmentBinlogs()[0].GetCollectionID()
}

func (t *vshardSplitCompactionTask) GetSlotUsage() int64 {
	return t.plan.GetSlotUsage()
}

func (t *vshardSplitCompactionTask) Complete() {
	t.done <- struct{}{}
}

func (t *vshardSplitCompactionTask) Stop() {
	t.cancel()
	<-t.done
}

func (t *vshardSplitCompactionTask) GetPlanID() typeutil.UniqueID {
	return t.plan.GetPlanID()
}

func (t *vshardSplitCompactionTask) GetChannelName() string {
	return t.plan.GetChannel()
}

func (t *vshardSplitCompactionTask) GetCompactionType() datapb.CompactionType {
	return t.plan.GetType()
}
