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

package indexnode

import (
	"context"
	"errors"
	"sync"

	"go.uber.org/zap"

	"github.com/golang/protobuf/proto"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// Mock is an alternative to IndexNode, it will return specific results based on specific parameters.
type Mock struct {
	Build   bool
	Failure bool
	Err     bool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	etcdKV *etcdkv.EtcdKV

	buildIndex chan *indexpb.CreateIndexRequest
}

// Init initializes the Mock of IndexNode. If the internal member `Err` is True, return an error.
func (inm *Mock) Init() error {
	if inm.Err {
		return errors.New("IndexNode init failed")
	}
	inm.ctx, inm.cancel = context.WithCancel(context.Background())
	inm.buildIndex = make(chan *indexpb.CreateIndexRequest)
	return nil
}

func (inm *Mock) buildIndexTask() {
	log.Debug("IndexNodeMock wait for building index")
	defer inm.wg.Done()
	for {
		select {
		case <-inm.ctx.Done():
			return
		case req := <-inm.buildIndex:
			if inm.Failure {
				saveIndexMeta := func() error {
					indexMeta := indexpb.IndexMeta{}

					_, values, versions, err := inm.etcdKV.LoadWithPrefix2(req.MetaPath)
					if err != nil {
						return err
					}
					err = proto.Unmarshal([]byte(values[0]), &indexMeta)
					if err != nil {
						return err
					}
					indexMeta.IndexFilePaths = []string{"IndexFilePath-1", "IndexFilePath-2"}
					indexMeta.State = commonpb.IndexState_Failed
					metaData, err := proto.Marshal(&indexMeta)
					if err != nil {
						return err
					}
					err = inm.etcdKV.CompareVersionAndSwap(req.MetaPath, versions[0],
						string(metaData))
					if err != nil {
						return err
					}
					return nil
				}
				err := retry.Do(context.Background(), saveIndexMeta, retry.Attempts(3))
				if err != nil {
					log.Error("IndexNode Mock saveIndexMeta error", zap.Error(err))
				}
			} else {
				saveIndexMeta := func() error {
					indexMeta := indexpb.IndexMeta{}
					_, values, versions, err := inm.etcdKV.LoadWithPrefix2(req.MetaPath)
					if err != nil {
						return err
					}
					err = proto.Unmarshal([]byte(values[0]), &indexMeta)
					if err != nil {
						return err
					}
					indexMeta.IndexFilePaths = []string{"IndexFilePath-1", "IndexFilePath-2"}
					indexMeta.State = commonpb.IndexState_Failed
					metaData, err := proto.Marshal(&indexMeta)
					if err != nil {
						return err
					}
					err = inm.etcdKV.CompareVersionAndSwap(req.MetaPath, versions[0],
						string(metaData))
					if err != nil {
						return err
					}

					indexMeta2 := indexpb.IndexMeta{}
					_, values2, versions2, err := inm.etcdKV.LoadWithPrefix2(req.MetaPath)
					if err != nil {
						return err
					}
					err = proto.Unmarshal([]byte(values2[0]), &indexMeta2)
					if err != nil {
						return err
					}
					indexMeta2.Version = indexMeta.Version + 1
					indexMeta2.IndexFilePaths = []string{"IndexFilePath-1", "IndexFilePath-2"}
					indexMeta2.State = commonpb.IndexState_Finished
					metaData2, err := proto.Marshal(&indexMeta2)
					if err != nil {
						return err
					}
					err = inm.etcdKV.CompareVersionAndSwap(req.MetaPath, versions2[0],
						string(metaData2))
					if err != nil {
						return err
					}
					return nil
				}
				err := retry.Do(context.Background(), saveIndexMeta, retry.Attempts(3))
				if err != nil {
					log.Error("IndexNode Mock saveIndexMeta error", zap.Error(err))
				}
			}
		}
	}
}

// Start starts the Mock of IndexNode. If the internal member `Err` is true, it will return an error.
func (inm *Mock) Start() error {
	if inm.Err {
		return errors.New("IndexNode start failed")
	}
	inm.wg.Add(1)
	go inm.buildIndexTask()
	return nil
}

// Stop stops the Mock of IndexNode. If the internal member `Err` is true, it will return an error.
func (inm *Mock) Stop() error {
	if inm.Err {
		return errors.New("IndexNode stop failed")
	}
	inm.cancel()
	inm.wg.Wait()
	inm.etcdKV.RemoveWithPrefix("session/" + typeutil.IndexNodeRole)
	return nil
}

// Register registers an IndexNode role in ETCD, if the internal member `Err` is true, it will return an error.
func (inm *Mock) Register() error {
	if inm.Err {
		return errors.New("IndexNode register failed")
	}
	Params.Init()
	inm.etcdKV, _ = etcdkv.NewEtcdKV(Params.EtcdEndpoints, Params.MetaRootPath)
	inm.etcdKV.RemoveWithPrefix("session/" + typeutil.IndexNodeRole)
	session := sessionutil.NewSession(context.Background(), Params.MetaRootPath, Params.EtcdEndpoints)
	session.Init(typeutil.IndexNodeRole, "localhost:21121", false)
	return nil
}

// GetComponentStates gets the component states of the mocked IndexNode, if the internal member `Err` is true, it will return an error,
// and the state is `StateCode_Abnormal`. Under normal circumstances the state is `StateCode_Healthy`.
func (inm *Mock) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	if inm.Err {
		return &internalpb.ComponentStates{
			State: &internalpb.ComponentInfo{
				StateCode: internalpb.StateCode_Abnormal,
			},
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
			},
		}, errors.New("IndexNode GetComponentStates Failed")
	}
	return &internalpb.ComponentStates{
		State: &internalpb.ComponentInfo{
			StateCode: internalpb.StateCode_Healthy,
		},
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}, nil
}

// GetStatisticsChannel gets the statistics channel of the mocked IndexNode, if the internal member `Err` is true, it will return an error.
func (inm *Mock) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	if inm.Err {
		return &milvuspb.StringResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
			},
		}, errors.New("IndexNode GetStatisticsChannel failed")
	}
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Value: "",
	}, nil
}

// GetTimeTickChannel gets the time tick channel of the mocked IndexNode, if the internal member `Err` is true, it will return an error.
func (inm *Mock) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	if inm.Err {
		return &milvuspb.StringResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
			},
		}, errors.New("IndexNode GetTimeTickChannel failed")
	}
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Value: "",
	}, nil
}

// CreateIndex receives a building index request, and return success, if the internal member `Build` is true,
// the indexing task will be executed. If the internal member `Err` is true, it will return an error.
// If the internal member `Failure` is true, the indexing task will be executed and the index state is Failed.
func (inm *Mock) CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest) (*commonpb.Status, error) {
	if inm.Build {
		inm.buildIndex <- req
	}

	if inm.Err {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		}, errors.New("IndexNode CreateIndex failed")
	}

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

// GetMetrics gets the metrics of mocked IndexNode, if the internal member `Failure` is true, it will return an error.
func (inm *Mock) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	if inm.Err {
		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    metricsinfo.MsgUnimplementedMetric,
			},
			Response: "",
		}, errors.New("IndexNode GetMetrics failed")
	}

	if inm.Failure {
		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    metricsinfo.MsgUnimplementedMetric,
			},
			Response: "",
		}, nil
	}

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Response:      "",
		ComponentName: "IndexNode",
	}, nil
}
