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

package index

import (
	"context"
	"fmt"
	sio "io"
	"time"

	"github.com/apache/arrow/go/v17/arrow/array"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	_ "github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var _ Task = (*globalStatsTask)(nil)

type globalStatsTask struct {
	ident  string
	ctx    context.Context
	cancel context.CancelFunc
	req    *datapb.GlobalStatsTask

	tr       *timerecord.TimeRecorder
	queueDur time.Duration
	manager  *TaskManager
	binlogIO io.BinlogIO

	currentTime time.Time
}

func NewGlobalStatsTask(ctx context.Context,
	cancel context.CancelFunc,
	req *datapb.GlobalStatsTask,
	manager *TaskManager,
	binlogIO io.BinlogIO,
) *globalStatsTask {
	return &globalStatsTask{
		ident:    fmt.Sprintf("%d", req.GetTaskID()),
		ctx:      ctx,
		cancel:   cancel,
		req:      req,
		manager:  manager,
		binlogIO: binlogIO,
		tr:       timerecord.NewTimeRecorder(fmt.Sprintf("TaskID: %d", req.GetTaskID())),
	}
}

func (gt *globalStatsTask) Ctx() context.Context {
	return gt.ctx
}

func (gt *globalStatsTask) Name() string {
	return gt.ident
}

func (gt *globalStatsTask) OnEnqueue(ctx context.Context) error {
	gt.queueDur = 0
	gt.tr.RecordSpan()
	log.Ctx(ctx).Info("globalStatsTask enqueue",
		zap.Int64("taskID", gt.req.GetTaskID()),
		zap.Int64("collectionID", gt.req.GetCollectionID()),
		zap.Int64("partitionID", gt.req.GetPartitionID()))
	return nil
}

func (gt *globalStatsTask) SetState(state indexpb.JobState, failReason string) {
	gt.manager.StoreGlobalStatsTaskState(gt.req.GetClusterID(), gt.req.GetTaskID(), state, failReason)
}

func (gt *globalStatsTask) GetState() indexpb.JobState {
	return gt.manager.GetGlobalStatsTaskState(gt.req.GetClusterID(), gt.req.GetTaskID())
}

func (gt *globalStatsTask) GetSlot() int64 {
	return gt.req.GetTaskSlot()
}

func (gt *globalStatsTask) PreExecute(ctx context.Context) error {
	ctx, span := otel.Tracer(typeutil.IndexNodeRole).Start(ctx, fmt.Sprintf("Stats-PreExecute-%d", gt.req.GetTaskID()))
	defer span.End()

	gt.queueDur = gt.tr.RecordSpan()
	log.Ctx(ctx).Info("Begin to PreExecute global stats task",
		zap.Int64("taskID", gt.req.GetTaskID()),
		zap.Int64("collectionID", gt.req.GetCollectionID()),
		zap.Int64("partitionID", gt.req.GetPartitionID()),
		zap.Int64("queue duration(ms)", gt.queueDur.Milliseconds()),
		zap.Int64("version", gt.req.GetVersion()),
	)

	preExecuteRecordSpan := gt.tr.RecordSpan()

	log.Ctx(ctx).Info("successfully PreExecute stats task",
		zap.Int64("taskID", gt.req.GetTaskID()),
		zap.Int64("collectionID", gt.req.GetCollectionID()),
		zap.Int64("partitionID", gt.req.GetPartitionID()),
		zap.Int64("preExecuteRecordSpan(ms)", preExecuteRecordSpan.Milliseconds()),
	)
	return nil
}

func (gt *globalStatsTask) Execute(ctx context.Context) error {
	log.Ctx(ctx).Info("Begin to Execute global stats task",
		zap.Int64("taskID", gt.req.GetTaskID()),
		zap.Int64("collectionID", gt.req.GetCollectionID()),
		zap.Int64("partitionID", gt.req.GetPartitionID()),
		zap.Int64("queue duration(ms)", gt.queueDur.Milliseconds()),
	)

	pkField, err := typeutil.GetPrimaryFieldSchema(gt.req.GetSchema())
	if err != nil {
		return err
	}

	segmentInfos := gt.req.GetSegmentInfos()
	if len(segmentInfos) == 0 {
		log.Ctx(ctx).Warn("no segment infos provided in request")
		return nil
	}

	segmentPrimaryKeysMap := make(map[int64][]string)
	for _, seg := range segmentInfos {
		segmentPKs, err := gt.readSegmentPrimaryKeys(ctx, seg, pkField)
		if err != nil {
			log.Ctx(ctx).Error("failed to read segment primary keys",
				zap.Int64("segmentID", seg.GetID()),
				zap.Error(err))
			return err
		}

		pkStrings := make([]string, 0, len(segmentPKs))
		for _, pk := range segmentPKs {
			switch v := pk.(type) {
			case int64:
				pkStrings = append(pkStrings, fmt.Sprintf("%d", v))
			case string:
				pkStrings = append(pkStrings, v)
			default:
				log.Ctx(ctx).Warn("unsupported primary key type", zap.Any("pk", pk))
				continue
			}
		}
		segmentPrimaryKeysMap[seg.GetID()] = pkStrings
	}

	// segmentPrimaryKeys := make([]*indexcgopb.SegmentPrimaryKeys, 0, len(segmentPrimaryKeysMap))
	// for segID, pks := range segmentPrimaryKeysMap {
	// 	segmentPrimaryKeys = append(segmentPrimaryKeys, &indexcgopb.SegmentPrimaryKeys{
	// 		SegmentId:   segID,
	// 		PrimaryKeys: pks,
	// 	})
	// }
	// newStorageConfig, err := ParseStorageConfig(gt.req.GetStorageConfig())
	// if err != nil {
	// 	return err
	// }

	// buildIndexParams := &indexcgopb.BuildPrimaryIndexInfo{
	// 	BuildID:            gt.req.GetTaskID(),
	// 	CollectionID:       gt.req.GetCollectionID(),
	// 	StorageConfig:      newStorageConfig,
	// 	SegmentPrimaryKeys: segmentPrimaryKeys,
	// }

	// uploaded, err := indexcgowrapper.BuildPrimaryKeyIndex(ctx, buildIndexParams)
	// if err != nil {
	// 	return err
	// }

	// files := make([]string, 0, len(uploaded))
	// for file := range uploaded {
	// 	files = append(files, file)
	// }
	// gt.manager.StoreGlobalStatsFiles(gt.req.GetClusterID(), gt.req.GetTaskID(), files)

	// totalElapse := gt.tr.RecordSpan()
	// metrics.DataNodeBuildPrimaryKeyStatsLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10)).Observe(totalElapse.Seconds())
	// log.Ctx(ctx).Info("create primary key index done",
	// 	zap.Duration("totalElapse", totalElapse),
	// 	zap.Int64("taskID", gt.req.GetTaskID()),
	// 	zap.Int64("collectionID", gt.req.GetCollectionID()),
	// 	zap.String("vchannelName", gt.req.GetVChannel()),
	// 	zap.Strings("files", files),
	// )

	return nil
}

func (gt *globalStatsTask) PostExecute(ctx context.Context) error {
	return nil
}

func (gt *globalStatsTask) readSegmentPrimaryKeys(ctx context.Context, seg *datapb.SegmentInfo, pkField *schemapb.FieldSchema) ([]interface{}, error) {
	primaryKeys := make([]interface{}, 0)
	storageConfig := gt.req.GetStorageConfig()
	bucketName := storageConfig.GetBucketName()
	if bucketName == "" {
		bucketName = paramtable.Get().ServiceParam.MinioCfg.BucketName.GetValue()
	}

	reader, err := storage.NewBinlogRecordReader(ctx,
		seg.GetBinlogs(),
		gt.req.GetSchema(),
		storage.WithDownloader(gt.binlogIO.Download),
		storage.WithVersion(seg.GetStorageVersion()),
		storage.WithStorageConfig(storageConfig),
		storage.WithNeededFields(typeutil.NewSet(pkField.FieldID)),
	)
	if err != nil {
		log.Ctx(ctx).Warn("failed to new insert binlogs reader", zap.Error(err))
		return nil, err
	}
	defer reader.Close()

	for {
		var r storage.Record
		r, err = reader.Next()
		if err != nil {
			if err == sio.EOF {
				err = nil
				break
			} else {
				log.Ctx(ctx).Warn("failed to iter through data", zap.Error(err))
				return nil, err
			}
		}

		pkArray := r.Column(pkField.FieldID)
		for i := range r.Len() {
			var pk interface{}
			switch pkField.DataType {
			case schemapb.DataType_Int64:
				pk = pkArray.(*array.Int64).Value(i)
			case schemapb.DataType_VarChar:
				s := pkArray.(*array.String).Value(i)
				pk = string([]byte(s))
			default:
				log.Ctx(ctx).Warn("unsupported primary key data type", zap.String("dataType", pkField.DataType.String()))
				continue
			}
			primaryKeys = append(primaryKeys, pk)
		}
	}
	return primaryKeys, nil
}

func (gt *globalStatsTask) Reset() {
	gt.ident = ""
	gt.ctx = nil
	gt.req = nil
	gt.cancel = nil
	gt.tr = nil
	gt.manager = nil
}
