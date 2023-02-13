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

package server

import (
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/cdc/core/model"
	"github.com/milvus-io/milvus/cdc/core/util"
	"github.com/milvus-io/milvus/cdc/core/writer"
	"go.uber.org/zap"
)

type WriteCallback struct {
	writer.DefaultWriteCallBack

	etcdCtl  util.KVApi
	rootPath string
	taskID   string
	log      *zap.Logger
}

func NewWriteCallback(etcdCtl util.KVApi, rootPath string, taskID string) *WriteCallback {
	return &WriteCallback{
		etcdCtl:  etcdCtl,
		rootPath: rootPath,
		taskID:   taskID,
		log:      log.With(zap.String("task_id", taskID)),
	}
}

func (w *WriteCallback) OnFail(data *model.CDCData, err error) {
	w.log.Warn("fail to write the msg", zap.String("data", util.Base64Encode(data)), zap.Error(err))
	writerFailCountVec.WithLabelValues(w.taskID, writeFailOnFail).Inc()
	_ = updateTaskFailedReason(w.etcdCtl, w.rootPath, w.taskID, err.Error())
}

func (w *WriteCallback) OnSuccess(collectionID int64, channelInfos map[string]writer.CallbackChannelInfo) {
	for channelName, info := range channelInfos {
		sub := util.SubByNow(info.Ts) // TODO should be the latest message time
		writerTimeDifferenceVec.WithLabelValues(w.taskID, strconv.FormatInt(collectionID, 10), channelName).Set(float64(sub))
	}
	// means it's drop collection message
	if len(channelInfos) > 1 {
		streamingCollectionCountVec.WithLabelValues(w.taskID, finishStatusLabel).Inc()
	}
}

func (w *WriteCallback) UpdateTaskCollectionPosition(collectionID int64, collectionName string, pChannelName string, position *commonpb.KeyDataPair) {
	if position == nil {
		return
	}
	err := updateTaskCollectionPosition(w.etcdCtl, w.rootPath, w.taskID,
		collectionID, collectionName, pChannelName, position)
	if err != nil {
		w.log.Warn("fail to update the collection position",
			zap.Int64("collection_id", collectionID),
			zap.String("vchannel_name", pChannelName),
			zap.String("position", util.Base64Encode(position)),
			zap.Error(err))
		writerFailCountVec.WithLabelValues(w.taskID, writeFailOnUpdatePosition).Inc()
	}
}
