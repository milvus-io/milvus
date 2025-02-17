/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package proxy

import (
	"context"
	"fmt"
	"strconv"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type importTask struct {
	baseTask
	Condition
	req       *internalpb.ImportRequest
	ctx       context.Context
	node      *Proxy
	dataCoord types.DataCoordClient

	msgID        UniqueID
	taskTS       Timestamp
	vchannels    []string
	pchannels    []string
	partitionIDs []int64
	collectionID UniqueID
	schema       *schemaInfo
	resp         *internalpb.ImportResponse
}

func (it *importTask) TraceCtx() context.Context {
	return it.ctx
}

func (it *importTask) ID() UniqueID {
	return it.msgID
}

func (it *importTask) SetID(uid UniqueID) {
	it.msgID = uid
}

func (it *importTask) Name() string {
	return "ImportTask"
}

func (it *importTask) Type() commonpb.MsgType {
	return commonpb.MsgType_Import
}

func (it *importTask) BeginTs() Timestamp {
	return it.taskTS
}

func (it *importTask) EndTs() Timestamp {
	return it.taskTS
}

func (it *importTask) SetTs(ts Timestamp) {
	it.taskTS = ts
}

func (it *importTask) OnEnqueue() error {
	return nil
}

func (it *importTask) PreExecute(ctx context.Context) error {
	req := it.req
	node := it.node
	collectionID, err := globalMetaCache.GetCollectionID(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}
	it.collectionID = collectionID
	schema, err := globalMetaCache.GetCollectionSchema(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}
	it.schema = schema
	channels, err := node.chMgr.getVChannels(collectionID)
	if err != nil {
		return err
	}
	it.vchannels = channels

	isBackup := importutilv2.IsBackup(req.GetOptions())
	isL0Import := importutilv2.IsL0Import(req.GetOptions())
	hasPartitionKey := typeutil.HasPartitionKey(schema.CollectionSchema)

	var partitionIDs []int64
	if isBackup {
		if req.GetPartitionName() == "" {
			return merr.WrapErrParameterInvalidMsg("partition not specified")
		}
		// Currently, Backup tool call import must with a partition name, each time restore a partition
		partitionID, err := globalMetaCache.GetPartitionID(ctx, req.GetDbName(), req.GetCollectionName(), req.GetPartitionName())
		if err != nil {
			return err
		}
		partitionIDs = []UniqueID{partitionID}
	} else if isL0Import {
		if req.GetPartitionName() == "" {
			partitionIDs = []UniqueID{common.AllPartitionsID}
		} else {
			partitionID, err := globalMetaCache.GetPartitionID(ctx, req.GetDbName(), req.GetCollectionName(), req.PartitionName)
			if err != nil {
				return err
			}
			partitionIDs = []UniqueID{partitionID}
		}
		// Currently, querynodes first load L0 segments and then load L1 segments.
		// Therefore, to ensure the deletes from L0 import take effect,
		// the collection needs to be in an unloaded state,
		// and then all L0 and L1 segments should be loaded at once.
		// We will remove this restriction after querynode supported to load L0 segments dynamically.
		loaded, err := isCollectionLoaded(ctx, node.queryCoord, collectionID)
		if err != nil {
			return err
		}
		if loaded {
			return merr.WrapErrImportFailed("for l0 import, collection cannot be loaded, please release it first")
		}
	} else {
		if hasPartitionKey {
			if req.GetPartitionName() != "" {
				return merr.WrapErrImportFailed("not allow to set partition name for collection with partition key")
			}
			partitions, err := globalMetaCache.GetPartitions(ctx, req.GetDbName(), req.GetCollectionName())
			if err != nil {
				return err
			}
			_, partitionIDs, err = typeutil.RearrangePartitionsForPartitionKey(partitions)
			if err != nil {
				return err
			}
		} else {
			if req.GetPartitionName() == "" {
				req.PartitionName = Params.CommonCfg.DefaultPartitionName.GetValue()
			}
			partitionID, err := globalMetaCache.GetPartitionID(ctx, req.GetDbName(), req.GetCollectionName(), req.PartitionName)
			if err != nil {
				return err
			}
			partitionIDs = []UniqueID{partitionID}
		}
	}

	req.Files = lo.Filter(req.GetFiles(), func(file *internalpb.ImportFile, _ int) bool {
		return len(file.GetPaths()) > 0
	})
	if len(req.Files) == 0 {
		return merr.WrapErrParameterInvalidMsg("import request is empty")
	}
	if len(req.Files) > Params.DataCoordCfg.MaxFilesPerImportReq.GetAsInt() {
		return merr.WrapErrImportFailed(fmt.Sprintf("The max number of import files should not exceed %d, but got %d",
			Params.DataCoordCfg.MaxFilesPerImportReq.GetAsInt(), len(req.Files)))
	}
	if !isBackup && !isL0Import {
		// check file type
		for _, file := range req.GetFiles() {
			_, err = importutilv2.GetFileType(file)
			if err != nil {
				return err
			}
		}
	}
	it.partitionIDs = partitionIDs
	return nil
}

func (it *importTask) setChannels() error {
	collID, err := globalMetaCache.GetCollectionID(it.ctx, it.req.GetDbName(), it.req.CollectionName)
	if err != nil {
		return err
	}
	channels, err := it.node.chMgr.getChannels(collID)
	if err != nil {
		return err
	}
	it.pchannels = channels
	return nil
}

func (it *importTask) getChannels() []pChan {
	return it.pchannels
}

func (it *importTask) Execute(ctx context.Context) error {
	jobID, err := it.node.rowIDAllocator.AllocOne()
	if err != nil {
		log.Ctx(ctx).Warn("alloc job id failed", zap.Error(err))
		return err
	}
	resourceKey := message.NewImportJobIDResourceKey(jobID)
	msg, err := message.NewImportMessageBuilderV1().
		WithHeader(&message.ImportMessageHeader{}).WithBody(
		&msgpb.ImportMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Import,
				Timestamp: it.BeginTs(),
			},
			DbName:         it.req.GetDbName(),
			CollectionName: it.req.GetCollectionName(),
			CollectionID:   it.collectionID,
			PartitionIDs:   it.partitionIDs,
			Options:        funcutil.KeyValuePair2Map(it.req.GetOptions()),
			Files:          GetImportFiles(it.req.GetFiles()),
			Schema:         it.schema.CollectionSchema,
			JobID:          jobID,
		}).
		WithBroadcast(it.vchannels, resourceKey).
		BuildBroadcast()
	if err != nil {
		log.Ctx(ctx).Warn("create import message failed", zap.Error(err))
		return err
	}
	resp, err := streaming.WAL().Broadcast().Append(ctx, msg)
	if err != nil {
		log.Ctx(ctx).Warn("broadcast import msg failed", zap.Error(err))
		return err
	}
	log.Ctx(ctx).Info(
		"broadcast import msg success",
		zap.Int64("jobID", jobID),
		zap.Uint64("broadcastID", resp.BroadcastID),
		zap.Any("appendResults", resp.AppendResults),
	)
	it.resp.JobID = strconv.FormatInt(jobID, 10)
	return nil
}

func GetImportFiles(internals []*internalpb.ImportFile) []*msgpb.ImportFile {
	return lo.Map(internals, func(internal *internalpb.ImportFile, _ int) *msgpb.ImportFile {
		return &msgpb.ImportFile{
			Id:    internal.GetId(),
			Paths: internal.GetPaths(),
		}
	})
}

func (it *importTask) PostExecute(ctx context.Context) error {
	return nil
}
