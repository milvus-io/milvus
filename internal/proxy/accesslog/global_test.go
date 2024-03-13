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

package accesslog

import (
	"context"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/accesslog/info"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	os.Exit(m.Run())
}

func TestAccessLogger_NotEnable(t *testing.T) {
	once = sync.Once{}
	var Params paramtable.ComponentParam

	Params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
	Params.Save(Params.ProxyCfg.AccessLog.Enable.Key, "false")

	InitAccessLogger(&Params)

	rpcInfo := &grpc.UnaryServerInfo{Server: nil, FullMethod: "testMethod"}
	accessInfo := info.NewGrpcAccessInfo(context.Background(), rpcInfo, nil)
	ok := _globalL.Write(accessInfo)
	assert.False(t, ok)
}

func TestAccessLogger_InitFailed(t *testing.T) {
	once = sync.Once{}
	var Params paramtable.ComponentParam

	Params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
	Params.Save(Params.ProxyCfg.AccessLog.Enable.Key, "true")
	Params.SaveGroup(map[string]string{Params.ProxyCfg.AccessLog.Formatter.KeyPrefix + "testf.invaild": "invalidConfig"})

	InitAccessLogger(&Params)
	rpcInfo := &grpc.UnaryServerInfo{Server: nil, FullMethod: "testMethod"}
	accessInfo := info.NewGrpcAccessInfo(context.Background(), rpcInfo, nil)
	ok := _globalL.Write(accessInfo)
	assert.False(t, ok)
}

func TestAccessLogger_Basic(t *testing.T) {
	once = sync.Once{}
	var Params paramtable.ComponentParam

	Params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
	testPath := "/tmp/accesstest"
	Params.Save(Params.ProxyCfg.AccessLog.Enable.Key, "true")
	Params.Save(Params.ProxyCfg.AccessLog.LocalPath.Key, testPath)
	defer os.RemoveAll(testPath)

	InitAccessLogger(&Params)

	ctx := peer.NewContext(
		context.Background(),
		&peer.Peer{
			Addr: &net.IPAddr{
				IP:   net.IPv4(0, 0, 0, 0),
				Zone: "test",
			},
		})
	ctx = metadata.AppendToOutgoingContext(ctx, info.ClientRequestIDKey, "test")

	req := &milvuspb.QueryRequest{
		DbName:         "test-db",
		CollectionName: "test-collection",
		PartitionNames: []string{"test-partition-1", "test-partition-2"},
	}

	resp := &milvuspb.BoolResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "",
		},
		Value: false,
	}

	rpcInfo := &grpc.UnaryServerInfo{Server: nil, FullMethod: "testMethod"}

	accessInfo := info.NewGrpcAccessInfo(ctx, rpcInfo, req)
	accessInfo.SetResult(resp, nil)

	ok := _globalL.Write(accessInfo)
	assert.True(t, ok)
}

func TestAccessLogger_Stdout(t *testing.T) {
	once = sync.Once{}
	var Params paramtable.ComponentParam

	Params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
	Params.Save(Params.ProxyCfg.AccessLog.Enable.Key, "true")
	Params.Save(Params.ProxyCfg.AccessLog.Filename.Key, "")

	InitAccessLogger(&Params)

	ctx := peer.NewContext(
		context.Background(),
		&peer.Peer{
			Addr: &net.IPAddr{
				IP:   net.IPv4(0, 0, 0, 0),
				Zone: "test",
			},
		})
	ctx = metadata.AppendToOutgoingContext(ctx, info.ClientRequestIDKey, "test")

	req := &milvuspb.QueryRequest{
		DbName:         "test-db",
		CollectionName: "test-collection",
		PartitionNames: []string{"test-partition-1", "test-partition-2"},
	}

	resp := &milvuspb.BoolResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "",
		},
		Value: false,
	}

	rpcInfo := &grpc.UnaryServerInfo{Server: nil, FullMethod: "testMethod"}

	accessInfo := info.NewGrpcAccessInfo(ctx, rpcInfo, req)
	accessInfo.SetResult(resp, nil)
	ok := _globalL.Write(accessInfo)
	assert.True(t, ok)
}

func TestAccessLogger_WithMinio(t *testing.T) {
	once = sync.Once{}
	var Params paramtable.ComponentParam

	Params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
	testPath := "/tmp/accesstest"
	Params.Save(Params.ProxyCfg.AccessLog.Enable.Key, "true")
	Params.Save(Params.ProxyCfg.AccessLog.Filename.Key, "test_access")
	Params.Save(Params.ProxyCfg.AccessLog.LocalPath.Key, testPath)
	Params.Save(Params.ProxyCfg.AccessLog.MinioEnable.Key, "true")
	Params.Save(Params.ProxyCfg.AccessLog.CacheSize.Key, "0")
	Params.Save(Params.ProxyCfg.AccessLog.RemotePath.Key, "access_log/")
	Params.Save(Params.ProxyCfg.AccessLog.MaxSize.Key, "1")
	defer os.RemoveAll(testPath)

	InitAccessLogger(&Params)
	writer, ok := _globalL.writer.(*RotateWriter)
	assert.True(t, ok)

	ctx := peer.NewContext(
		context.Background(),
		&peer.Peer{
			Addr: &net.IPAddr{
				IP:   net.IPv4(0, 0, 0, 0),
				Zone: "test",
			},
		})
	ctx = metadata.AppendToOutgoingContext(ctx, info.ClientRequestIDKey, "test")

	req := &milvuspb.QueryRequest{
		DbName:         "test-db",
		CollectionName: "test-collection",
		PartitionNames: []string{"test-partition-1", "test-partition-2"},
	}

	resp := &milvuspb.BoolResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "",
		},
		Value: false,
	}

	rpcInfo := &grpc.UnaryServerInfo{Server: nil, FullMethod: "testMethod"}

	accessInfo := info.NewGrpcAccessInfo(ctx, rpcInfo, req)
	accessInfo.SetResult(resp, nil)
	ok = _globalL.Write(accessInfo)
	assert.True(t, ok)

	err := writer.Rotate()
	assert.NoError(t, err)
	defer writer.handler.Clean()

	time.Sleep(time.Duration(1) * time.Second)
	logfiles, err := writer.handler.listAll()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(logfiles))
}
