// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package indexcoord

import (
	"context"
	"errors"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/golang/protobuf/proto"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	miniokv "github.com/milvus-io/milvus/internal/kv/minio"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/tso"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	reqTimeoutInterval = time.Second * 10
	durationInterval   = time.Second * 10
	assignTaskInterval = time.Second * 3
	taskLimit          = 20
)

type IndexCoord struct {
	stateCode atomic.Value

	ID UniqueID

	loopCtx    context.Context
	loopCancel func()
	loopWg     sync.WaitGroup

	sched   *TaskScheduler
	session *sessionutil.Session

	eventChan <-chan *sessionutil.SessionEvent

	idAllocator *allocator.GlobalIDAllocator

	kv kv.BaseKV

	metaTable   *metaTable
	nodeManager *NodeManager

	nodeLock sync.RWMutex

	// Add callback functions at different stages
	startCallbacks []func()
	closeCallbacks []func()
}

type UniqueID = typeutil.UniqueID
type Timestamp = typeutil.Timestamp

func NewIndexCoord(ctx context.Context) (*IndexCoord, error) {
	rand.Seed(time.Now().UnixNano())
	ctx1, cancel := context.WithCancel(ctx)
	i := &IndexCoord{
		loopCtx:    ctx1,
		loopCancel: cancel,
	}
	i.UpdateStateCode(internalpb.StateCode_Abnormal)
	return i, nil
}

// Register register index service at etcd
func (i *IndexCoord) Register() error {
	i.session = sessionutil.NewSession(i.loopCtx, Params.MetaRootPath, Params.EtcdEndpoints)
	i.session.Init(typeutil.IndexCoordRole, Params.Address, true)
	return nil
}

func (i *IndexCoord) Init() error {
	log.Debug("IndexCoord", zap.Any("etcd endpoints", Params.EtcdEndpoints))

	connectEtcdFn := func() error {
		etcdClient, err := clientv3.New(clientv3.Config{Endpoints: Params.EtcdEndpoints})
		if err != nil {
			return err
		}
		etcdKV := etcdkv.NewEtcdKV(etcdClient, Params.MetaRootPath)
		metakv, err := NewMetaTable(etcdKV)
		if err != nil {
			return err
		}
		i.metaTable = metakv
		return err
	}
	log.Debug("IndexCoord try to connect etcd")
	err := retry.Do(i.loopCtx, connectEtcdFn, retry.Attempts(300))
	if err != nil {
		log.Debug("IndexCoord try to connect etcd failed", zap.Error(err))
		return err
	}
	log.Debug("IndexCoord try to connect etcd success")
	i.nodeManager = NewNodeManager()

	sessions, revision, err := i.session.GetSessions(typeutil.IndexNodeRole)
	log.Debug("IndexCoord", zap.Any("session number", len(sessions)), zap.Any("revision", revision))
	if err != nil {
		log.Debug("IndexCoord", zap.Any("Get IndexNode Sessions error", err))
	}
	for _, session := range sessions {
		if err = i.nodeManager.AddNode(session.ServerID, session.Address); err != nil {
			log.Debug("IndexCoord", zap.Any("ServerID", session.ServerID),
				zap.Any("Add IndexNode error", err))
		}
	}
	log.Debug("IndexCoord", zap.Any("IndexNode number", len(i.nodeManager.nodeClients)))
	i.eventChan = i.session.WatchServices(typeutil.IndexNodeRole, revision+1)
	nodeTasks := i.metaTable.GetNodeTaskStats()
	for nodeID, taskNum := range nodeTasks {
		i.nodeManager.pq.UpdatePriority(nodeID, taskNum)
	}

	//init idAllocator
	kvRootPath := Params.KvRootPath
	i.idAllocator = allocator.NewGlobalIDAllocator("idTimestamp", tsoutil.NewTSOKVBase(Params.EtcdEndpoints, kvRootPath, "index_gid"))
	if err := i.idAllocator.Initialize(); err != nil {
		log.Debug("IndexCoord idAllocator initialize failed", zap.Error(err))
		return err
	}

	i.ID, err = i.idAllocator.AllocOne()
	if err != nil {
		return err
	}

	option := &miniokv.Option{
		Address:           Params.MinIOAddress,
		AccessKeyID:       Params.MinIOAccessKeyID,
		SecretAccessKeyID: Params.MinIOSecretAccessKey,
		UseSSL:            Params.MinIOUseSSL,
		BucketName:        Params.MinioBucketName,
		CreateBucket:      true,
	}

	i.kv, err = miniokv.NewMinIOKV(i.loopCtx, option)
	if err != nil {
		log.Debug("IndexCoord new minio kv failed", zap.Error(err))
		return err
	}
	log.Debug("IndexCoord new minio kv success")

	i.sched, err = NewTaskScheduler(i.loopCtx, i.idAllocator, i.kv, i.metaTable)
	if err != nil {
		log.Debug("IndexCoord new task scheduler failed", zap.Error(err))
		return err
	}
	log.Debug("IndexCoord new task scheduler success")
	i.UpdateStateCode(internalpb.StateCode_Healthy)
	log.Debug("IndexCoord", zap.Any("State", i.stateCode.Load()))

	log.Debug("IndexCoord assign tasks server success", zap.Error(err))
	return nil
}

func (i *IndexCoord) Start() error {
	i.loopWg.Add(1)
	go i.tsLoop()

	i.loopWg.Add(1)
	go i.recycleUnusedIndexFiles()

	i.loopWg.Add(1)
	go i.assignTaskLoop()

	i.loopWg.Add(1)
	go i.watchNodeLoop()

	i.loopWg.Add(1)
	go i.watchMetaLoop()

	i.sched.Start()
	// Start callbacks
	for _, cb := range i.startCallbacks {
		cb()
	}
	log.Debug("IndexCoord start")

	return nil
}

func (i *IndexCoord) Stop() error {
	i.loopCancel()
	i.sched.Close()
	for _, cb := range i.closeCallbacks {
		cb()
	}
	return nil
}

func (i *IndexCoord) UpdateStateCode(code internalpb.StateCode) {
	i.stateCode.Store(code)
}

func (i *IndexCoord) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	log.Debug("get IndexCoord component states ...")
	stateInfo := &internalpb.ComponentInfo{
		NodeID:    i.ID,
		Role:      "IndexCoord",
		StateCode: i.stateCode.Load().(internalpb.StateCode),
	}

	ret := &internalpb.ComponentStates{
		State:              stateInfo,
		SubcomponentStates: nil, // todo add subcomponents states
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}
	log.Debug("IndexCoord GetComponentStates", zap.Any("IndexCoord component state", stateInfo))
	return ret, nil
}

func (i *IndexCoord) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	log.Debug("get IndexCoord time tick channel ...")
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: "",
	}, nil
}

func (i *IndexCoord) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	log.Debug("get IndexCoord statistics channel ...")
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Value: "",
	}, nil
}

func (i *IndexCoord) BuildIndex(ctx context.Context, req *indexpb.BuildIndexRequest) (*indexpb.BuildIndexResponse, error) {
	log.Debug("IndexCoord building index ...",
		zap.Int64("IndexBuildID", req.IndexBuildID),
		zap.String("IndexName = ", req.IndexName),
		zap.Int64("IndexID = ", req.IndexID),
		zap.Strings("DataPath = ", req.DataPaths),
		zap.Any("TypeParams", req.TypeParams),
		zap.Any("IndexParams", req.IndexParams))
	hasIndex, indexBuildID := i.metaTable.HasSameReq(req)
	if hasIndex {
		log.Debug("IndexCoord", zap.Int64("hasIndex true", indexBuildID), zap.Strings("data paths", req.DataPaths))
		return &indexpb.BuildIndexResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
				Reason:    "already have same index",
			},
			IndexBuildID: indexBuildID,
		}, nil
	}
	ret := &indexpb.BuildIndexResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		},
	}
	t := &IndexAddTask{
		BaseTask: BaseTask{
			ctx:   ctx,
			done:  make(chan error),
			table: i.metaTable,
		},
		req:         req,
		idAllocator: i.idAllocator,
	}

	var cancel func()
	t.ctx, cancel = context.WithTimeout(ctx, reqTimeoutInterval)
	defer cancel()

	fn := func() error {
		select {
		case <-ctx.Done():
			return errors.New("IndexAddQueue enqueue timeout")
		default:
			return i.sched.IndexAddQueue.Enqueue(t)
		}
	}

	err := fn()
	if err != nil {
		ret.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		ret.Status.Reason = err.Error()
		return ret, nil
	}
	log.Debug("IndexCoord BuildIndex Enqueue successfully", zap.Any("IndexBuildID", indexBuildID))

	err = t.WaitToFinish()
	if err != nil {
		ret.Status.ErrorCode = commonpb.ErrorCode_UnexpectedError
		ret.Status.Reason = err.Error()
		return ret, nil
	}
	ret.Status.ErrorCode = commonpb.ErrorCode_Success
	ret.IndexBuildID = t.indexBuildID
	return ret, nil
}

func (i *IndexCoord) GetIndexStates(ctx context.Context, req *indexpb.GetIndexStatesRequest) (*indexpb.GetIndexStatesResponse, error) {
	var (
		cntNone       = 0
		cntUnissued   = 0
		cntInprogress = 0
		cntFinished   = 0
		cntFailed     = 0
	)
	indexStates := i.metaTable.GetIndexStates(req.IndexBuildIDs)
	for _, state := range indexStates {
		switch state.State {
		case commonpb.IndexState_IndexStateNone:
			cntNone++
		case commonpb.IndexState_Unissued:
			cntUnissued++
		case commonpb.IndexState_InProgress:
			cntInprogress++
		case commonpb.IndexState_Finished:
			cntFinished++
		case commonpb.IndexState_Failed:
			cntFailed++
		}
	}
	log.Debug("IndexCoord get index states success",
		zap.Int("total", len(indexStates)), zap.Int("None", cntNone), zap.Int("Unissued", cntUnissued),
		zap.Int("InProgress", cntInprogress), zap.Int("Finished", cntFinished), zap.Int("Failed", cntFailed))

	ret := &indexpb.GetIndexStatesResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		States: indexStates,
	}
	return ret, nil
}

func (i *IndexCoord) DropIndex(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error) {
	log.Debug("IndexCoord DropIndex", zap.Any("IndexID", req.IndexID))

	ret := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}
	err := i.metaTable.MarkIndexAsDeleted(req.IndexID)
	if err != nil {
		ret.ErrorCode = commonpb.ErrorCode_UnexpectedError
		ret.Reason = err.Error()
		return ret, nil
	}

	defer func() {
		go func() {
			unissuedIndexBuildIDs := i.sched.IndexAddQueue.tryToRemoveUselessIndexAddTask(req.IndexID)
			for _, indexBuildID := range unissuedIndexBuildIDs {
				i.metaTable.DeleteIndex(indexBuildID)
			}
		}()
	}()

	log.Debug("IndexCoord DropIndex success", zap.Any("IndexID", req.IndexID))
	return ret, nil
}

func (i *IndexCoord) GetIndexFilePaths(ctx context.Context, req *indexpb.GetIndexFilePathsRequest) (*indexpb.GetIndexFilePathsResponse, error) {
	log.Debug("IndexCoord GetIndexFilePaths", zap.Int64s("IndexBuildIds", req.IndexBuildIDs))
	var indexPaths []*indexpb.IndexFilePathInfo = nil

	for _, indexID := range req.IndexBuildIDs {
		indexPathInfo, err := i.metaTable.GetIndexFilePathInfo(indexID)
		if err != nil {
			return nil, err
		}
		indexPaths = append(indexPaths, indexPathInfo)
	}
	log.Debug("IndexCoord GetIndexFilePaths success")

	ret := &indexpb.GetIndexFilePathsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		FilePaths: indexPaths,
	}
	log.Debug("IndexCoord GetIndexFilePaths ", zap.Any("FilePaths", ret.FilePaths))

	return ret, nil
}

func (i *IndexCoord) tsLoop() {
	tsoTicker := time.NewTicker(tso.UpdateTimestampStep)
	defer tsoTicker.Stop()
	ctx, cancel := context.WithCancel(i.loopCtx)
	defer cancel()
	defer i.loopWg.Done()
	for {
		select {
		case <-tsoTicker.C:
			if err := i.idAllocator.UpdateID(); err != nil {
				log.Debug("IndexCoord tsLoop UpdateID failed", zap.Error(err))
				return
			}
		case <-ctx.Done():
			// Server is closed and it should return nil.
			log.Debug("IndexCoord tsLoop is closed")
			return
		}
	}
}

func (i *IndexCoord) recycleUnusedIndexFiles() {
	ctx, cancel := context.WithCancel(i.loopCtx)

	defer cancel()
	defer i.loopWg.Done()

	timeTicker := time.NewTicker(durationInterval)
	log.Debug("IndexCoord start recycleUnusedIndexFiles loop")

	for {
		select {
		case <-ctx.Done():
			return
		case <-timeTicker.C:
			metas := i.metaTable.GetUnusedIndexFiles(taskLimit)
			for _, meta := range metas {
				if meta.indexMeta.MarkDeleted {
					unusedIndexFilePathPrefix := strconv.Itoa(int(meta.indexMeta.IndexBuildID))
					if err := i.kv.RemoveWithPrefix(unusedIndexFilePathPrefix); err != nil {
						log.Debug("IndexCoord recycleUnusedIndexFiles Remove index files failed",
							zap.Any("MarkDeleted", true), zap.Error(err))
					}
					i.metaTable.DeleteIndex(meta.indexMeta.IndexBuildID)
				} else {
					for j := 1; j < int(meta.indexMeta.Version); j++ {
						unusedIndexFilePathPrefix := strconv.Itoa(int(meta.indexMeta.IndexBuildID)) + "/" + strconv.Itoa(j)
						if err := i.kv.RemoveWithPrefix(unusedIndexFilePathPrefix); err != nil {
							log.Debug("IndexCoord recycleUnusedIndexFiles Remove index files failed",
								zap.Any("MarkDeleted", false), zap.Error(err))
						}
					}
					if err := i.metaTable.UpdateRecycleState(meta.indexMeta.IndexBuildID); err != nil {
						log.Debug("IndexCoord recycleUnusedIndexFiles UpdateRecycleState failed", zap.Error(err))
					}
				}
			}
		}
	}
}

func (i *IndexCoord) watchNodeLoop() {
	ctx, cancel := context.WithCancel(i.loopCtx)

	defer cancel()
	defer i.loopWg.Done()
	log.Debug("IndexCoord watchNodeLoop start")

	for {
		select {
		case <-ctx.Done():
			return
		case event := <-i.eventChan:
			log.Debug("IndexCoord watchNodeLoop event updated")
			switch event.EventType {
			case sessionutil.SessionAddEvent:
				serverID := event.Session.ServerID
				log.Debug("IndexCoord watchNodeLoop SessionAddEvent", zap.Any("serverID", serverID), zap.Any("address", event.Session.Address))
				err := i.nodeManager.AddNode(serverID, event.Session.Address)
				if err != nil {
					log.Debug("IndexCoord", zap.Any("Add IndexNode err", err))
				}
				log.Debug("IndexCoord", zap.Any("IndexNode number", len(i.nodeManager.nodeClients)))
			case sessionutil.SessionDelEvent:
				serverID := event.Session.ServerID
				log.Debug("IndexCoord watchNodeLoop SessionDelEvent", zap.Any("serverID", serverID))
				i.nodeManager.RemoveNode(serverID)
			}
		}
	}
}

func (i *IndexCoord) watchMetaLoop() {
	ctx, cancel := context.WithCancel(i.loopCtx)

	defer cancel()
	defer i.loopWg.Done()
	log.Debug("IndexCoord watchMetaLoop start")

	watchChan := i.metaTable.client.WatchWithPrefix("indexes")

	for {
		select {
		case <-ctx.Done():
			return
		case resp := <-watchChan:
			log.Debug("IndexCoord watchMetaLoop find meta updated.")
			for _, event := range resp.Events {
				eventRevision := event.Kv.Version
				indexMeta := &indexpb.IndexMeta{}
				err := proto.UnmarshalText(string(event.Kv.Value), indexMeta)
				indexBuildID := indexMeta.IndexBuildID
				log.Debug("IndexCoord watchMetaLoop", zap.Any("event.Key", event.Kv.Key),
					zap.Any("event.V", indexMeta), zap.Any("IndexBuildID", indexBuildID), zap.Error(err))
				switch event.Type {
				case mvccpb.PUT:
					//TODO: get indexBuildID fast
					reload := i.metaTable.LoadMetaFromETCD(indexBuildID, eventRevision)
					log.Debug("IndexCoord watchMetaLoop PUT", zap.Any("IndexBuildID", indexBuildID), zap.Any("reload", reload))
					if reload {
						i.nodeManager.pq.IncPriority(indexMeta.NodeID, -1)
					}
				case mvccpb.DELETE:
				}
			}
		}
	}
}

func (i *IndexCoord) assignTaskLoop() {
	ctx, cancel := context.WithCancel(i.loopCtx)

	defer cancel()
	defer i.loopWg.Done()

	timeTicker := time.NewTicker(assignTaskInterval)
	log.Debug("IndexCoord start assignTask loop")

	for {
		select {
		case <-ctx.Done():
			log.Debug("IndexCoord assignTaskLoop ctx Done")
			return
		case <-timeTicker.C:
			sessions, _, err := i.session.GetSessions(typeutil.IndexNodeRole)
			if err != nil {
				log.Debug("IndexCoord assignTaskLoop", zap.Any("GetSessions error", err))
			}
			if len(sessions) <= 0 {
				break
			}
			var serverIDs []int64
			for _, session := range sessions {
				serverIDs = append(serverIDs, session.ServerID)
			}
			metas := i.metaTable.GetUnassignedTasks(serverIDs)
			sort.Slice(metas, func(i, j int) bool {
				return metas[i].indexMeta.Version <= metas[j].indexMeta.Version
			})
			log.Debug("IndexCoord assignTaskLoop", zap.Any("Unassign tasks number", len(metas)))
			for index, meta := range metas {
				indexBuildID := meta.indexMeta.IndexBuildID
				if err = i.metaTable.UpdateVersion(indexBuildID); err != nil {
					log.Debug("IndexCoord assignmentTasksLoop metaTable.UpdateVersion failed", zap.Error(err))
				}
				nodeID, builderClient := i.nodeManager.PeekClient()
				if builderClient == nil {
					log.Debug("IndexCoord assignmentTasksLoop can not find available IndexNode")
					break
				}
				req := &indexpb.CreateIndexRequest{
					IndexBuildID: indexBuildID,
					IndexName:    meta.indexMeta.Req.IndexName,
					IndexID:      meta.indexMeta.Req.IndexID,
					Version:      meta.indexMeta.Version + 1,
					MetaPath:     "/indexes/" + strconv.FormatInt(indexBuildID, 10),
					DataPaths:    meta.indexMeta.Req.DataPaths,
					TypeParams:   meta.indexMeta.Req.TypeParams,
					IndexParams:  meta.indexMeta.Req.IndexParams,
				}
				resp, err := builderClient.CreateIndex(ctx, req)
				if err != nil {
					log.Debug("IndexCoord assignmentTasksLoop builderClient.CreateIndex failed", zap.Error(err))
					continue
				}
				if resp.ErrorCode != commonpb.ErrorCode_Success {
					log.Debug("IndexCoord assignmentTasksLoop builderClient.CreateIndex failed", zap.String("Reason", resp.Reason))
					continue
				}
				if err = i.metaTable.BuildIndex(indexBuildID, nodeID); err != nil {
					log.Debug("IndexCoord assignmentTasksLoop metaTable.BuildIndex failed", zap.Error(err))
				}
				i.nodeManager.pq.IncPriority(nodeID, 1)
				if index > taskLimit {
					break
				}
			}
		}
	}
}
