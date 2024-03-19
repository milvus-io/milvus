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

package indexnode

import (
	"context"
	"strconv"
	"time"

	"github.com/milvus-io/milvus/pkg/util/timerecord"

	"github.com/milvus-io/milvus/internal/proto/indexpb"

	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

type analysisTask struct {
	ident  string
	ctx    context.Context
	cancel context.CancelFunc
	req    *indexpb.AnalysisRequest

	tr       *timerecord.TimeRecorder
	queueDur time.Duration
	node     *IndexNode

	startTime int64
	endTime   int64
}

func (at *analysisTask) Ctx() context.Context {
	return at.ctx
}
func (at *analysisTask) Name() string {
	return at.ident
}

func (at *analysisTask) Prepare(context.Context) error {
	at.queueDur = at.tr.RecordSpan()
	log.Ctx(ctx).Info("Begin to prepare indexBuildTask", zap.Int64("buildID", it.BuildID),
		zap.Int64("Collection", it.collectionID), zap.Int64("SegmentID", it.segmentID))
	typeParams := make(map[string]string)
	indexParams := make(map[string]string)

	if len(it.req.DataPaths) == 0 {
		for _, id := range it.req.GetDataIds() {
			path := metautil.BuildInsertLogPath(it.req.GetStorageConfig().RootPath, it.req.GetCollectionID(), it.req.GetPartitionID(), it.req.GetSegmentID(), it.req.GetFieldID(), id)
			it.req.DataPaths = append(it.req.DataPaths, path)
		}
	}

	if it.req.OptionalScalarFields != nil {
		for _, optFields := range it.req.GetOptionalScalarFields() {
			if len(optFields.DataPaths) == 0 {
				for _, id := range optFields.DataIds {
					path := metautil.BuildInsertLogPath(it.req.GetStorageConfig().RootPath, it.req.GetCollectionID(), it.req.GetPartitionID(), it.req.GetSegmentID(), optFields.FieldID, id)
					optFields.DataPaths = append(optFields.DataPaths, path)
				}
			}
		}
	}

	// type params can be removed
	for _, kvPair := range it.req.GetTypeParams() {
		key, value := kvPair.GetKey(), kvPair.GetValue()
		typeParams[key] = value
		indexParams[key] = value
	}

	for _, kvPair := range it.req.GetIndexParams() {
		key, value := kvPair.GetKey(), kvPair.GetValue()
		// knowhere would report error if encountered the unknown key,
		// so skip this
		if key == common.MmapEnabledKey {
			continue
		}
		indexParams[key] = value
	}
	it.newTypeParams = typeParams
	it.newIndexParams = indexParams
	it.statistic.IndexParams = it.req.GetIndexParams()
	// ugly codes to get dimension
	if dimStr, ok := typeParams[common.DimKey]; ok {
		var err error
		it.statistic.Dim, err = strconv.ParseInt(dimStr, 10, 64)
		if err != nil {
			log.Ctx(ctx).Error("parse dimesion failed", zap.Error(err))
			// ignore error
		}
	}
	log.Ctx(ctx).Info("Successfully prepare indexBuildTask", zap.Int64("buildID", it.BuildID),
		zap.Int64("Collection", it.collectionID), zap.Int64("SegmentID", it.segmentID))
	return nil
}

func (at *analysisTask) LoadData(context.Context) error {

}
func (at *analysisTask) BuildIndex(context.Context) error {

}
func (at *analysisTask) SaveIndexFiles(context.Context) error {

}

func (at *analysisTask) OnEnqueue(ctx context.Context) error {
	at.queueDur = 0
	at.tr.RecordSpan()
	at.startTime = time.Now().UnixMicro()
	log.Ctx(ctx).Info("IndexNode analysisTask enqueued", zap.String("clusterID", at.req.GetClusterID()),
		zap.Int64("taskID", at.req.GetTaskID()))
	return nil
}

func (at *analysisTask) SetState(state commonpb.IndexState, failReason string) {
	at.node.storeAnalysisTaskState(at.req.GetClusterID(), at.req.GetTaskID(), state, failReason)
}

func (at *analysisTask) GetState() commonpb.IndexState {
	return at.node.loadAnalysisTaskState(at.req.GetClusterID(), at.req.GetTaskID())
}

func (at *analysisTask) Reset() {
	at.ident = ""
	at.ctx = nil
	at.cancel = nil
	at.req = nil
	at.tr = nil
	at.queueDur = 0
	at.node = nil
	at.startTime = 0
	at.endTime = 0
}
