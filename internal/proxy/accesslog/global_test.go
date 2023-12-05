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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestAccessLogger_NotEnable(t *testing.T) {
	var Params paramtable.ComponentParam

	Params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
	Params.Save(Params.ProxyCfg.AccessLog.Enable.Key, "false")

	err := initAccessLogger(&Params.ProxyCfg.AccessLog, &Params.MinioCfg)
	assert.NoError(t, err)

	rpcInfo := &grpc.UnaryServerInfo{Server: nil, FullMethod: "testMethod"}
	accessInfo := NewGrpcAccessInfo(context.Background(), rpcInfo, nil)
	ok := accessInfo.Write()
	assert.False(t, ok)
}

func TestAccessLogger_InitFailed(t *testing.T) {
	var Params paramtable.ComponentParam

	Params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
	Params.Save(Params.ProxyCfg.AccessLog.Enable.Key, "true")
	Params.SaveGroup(map[string]string{Params.ProxyCfg.AccessLog.Formatter.KeyPrefix + "testf.invaild": "invalidConfig"})

	err := initAccessLogger(&Params.ProxyCfg.AccessLog, &Params.MinioCfg)
	assert.Error(t, err)
}

func TestAccessLogger_Basic(t *testing.T) {
	var Params paramtable.ComponentParam

	Params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
	testPath := "/tmp/accesstest"
	Params.Save(Params.ProxyCfg.AccessLog.Enable.Key, "true")
	Params.Save(Params.ProxyCfg.AccessLog.LocalPath.Key, testPath)
	defer os.RemoveAll(testPath)

	initAccessLogger(&Params.ProxyCfg.AccessLog, &Params.MinioCfg)

	ctx := peer.NewContext(
		context.Background(),
		&peer.Peer{
			Addr: &net.IPAddr{
				IP:   net.IPv4(0, 0, 0, 0),
				Zone: "test",
			},
		})
	ctx = metadata.AppendToOutgoingContext(ctx, clientRequestIDKey, "test")

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

	accessInfo := NewGrpcAccessInfo(ctx, rpcInfo, req)

	accessInfo.SetResult(resp, nil)
	ok := accessInfo.Write()
	assert.True(t, ok)
}

func TestAccessLogger_Stdout(t *testing.T) {
	var Params paramtable.ComponentParam

	Params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
	Params.Save(Params.ProxyCfg.AccessLog.Enable.Key, "true")
	Params.Save(Params.ProxyCfg.AccessLog.Filename.Key, "")

	initAccessLogger(&Params.ProxyCfg.AccessLog, &Params.MinioCfg)

	ctx := peer.NewContext(
		context.Background(),
		&peer.Peer{
			Addr: &net.IPAddr{
				IP:   net.IPv4(0, 0, 0, 0),
				Zone: "test",
			},
		})
	ctx = metadata.AppendToOutgoingContext(ctx, clientRequestIDKey, "test")

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

	accessInfo := NewGrpcAccessInfo(ctx, rpcInfo, req)
	accessInfo.SetResult(resp, nil)
	ok := accessInfo.Write()
	assert.True(t, ok)
}

func TestAccessLogger_WithMinio(t *testing.T) {
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

	// test rotate before init
	err := Rotate()
	assert.NoError(t, err)

	initAccessLogger(&Params.ProxyCfg.AccessLog, &Params.MinioCfg)

	ctx := peer.NewContext(
		context.Background(),
		&peer.Peer{
			Addr: &net.IPAddr{
				IP:   net.IPv4(0, 0, 0, 0),
				Zone: "test",
			},
		})
	ctx = metadata.AppendToOutgoingContext(ctx, clientRequestIDKey, "test")

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

	accessInfo := NewGrpcAccessInfo(ctx, rpcInfo, req)
	accessInfo.SetResult(resp, nil)
	ok := accessInfo.Write()
	assert.True(t, ok)

	Rotate()
	defer _globalR.handler.Clean()

	time.Sleep(time.Duration(1) * time.Second)
	logfiles, err := _globalR.handler.listAll()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(logfiles))
}
