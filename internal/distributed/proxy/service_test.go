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

package grpcproxy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/federpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	grpcproxyclient "github.com/milvus-io/milvus/internal/distributed/proxy/client"
	"github.com/milvus-io/milvus/internal/distributed/proxy/httpserver"
	mhttp "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	milvusmock "github.com/milvus-io/milvus/internal/util/mock"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/netutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/uniquegenerator"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	code := m.Run()
	os.Exit(code)
}

// /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
type WaitOption struct {
	Duration      time.Duration `json:"duration"`
	Port          int           `json:"port"`
	TLSMode       int           `json:"tls_mode"`
	ClientPemPath string        `json:"client_pem_path"`
	ClientKeyPath string        `json:"client_key_path"`
	CaPath        string        `json:"ca_path"`
}

func (opt *WaitOption) String() string {
	s, err := json.Marshal(*opt)
	if err != nil {
		return fmt.Sprintf("error: %s", err)
	}
	return string(s)
}

func newWaitOption(duration time.Duration, Port int, tlsMode int, clientPemPath, clientKeyPath, clientCaPath string) *WaitOption {
	return &WaitOption{
		Duration:      duration,
		Port:          Port,
		TLSMode:       tlsMode,
		ClientPemPath: clientPemPath,
		ClientKeyPath: clientKeyPath,
		CaPath:        clientCaPath,
	}
}

func withCredential(clientPemPath, clientKeyPath, clientCaPath string) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(clientPemPath, clientKeyPath)
	if err != nil {
		return nil, err
	}
	certPool := x509.NewCertPool()
	ca, err := os.ReadFile(clientCaPath)
	if err != nil {
		return nil, err
	}
	if ok := certPool.AppendCertsFromPEM(ca); !ok {
		return nil, errors.New("failed to AppendCertsFromPEM")
	}
	creds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		ServerName:   "localhost",
		RootCAs:      certPool,
		MinVersion:   tls.VersionTLS13,
	})
	return creds, nil
}

// waitForGrpcReady block until service available or panic after times out.
func waitForGrpcReady(opt *WaitOption) {
	Params := &paramtable.Get().ProxyGrpcServerCfg
	ch := make(chan error, 1)

	go func() {
		// just used in UT to self-check service is available.
		address := "localhost:" + strconv.Itoa(opt.Port)
		var err error

		if opt.TLSMode == 1 || opt.TLSMode == 2 {
			var creds credentials.TransportCredentials
			if opt.TLSMode == 1 {
				creds, err = credentials.NewClientTLSFromFile(Params.ServerPemPath.GetValue(), "localhost")
			} else {
				creds, err = withCredential(opt.ClientPemPath, opt.ClientKeyPath, opt.CaPath)
			}
			if err != nil {
				ch <- err
				return
			}
			conn, err := grpc.Dial(address, grpc.WithBlock(), grpc.WithTransportCredentials(creds))
			ch <- err
			conn.Close()
			return
		}
		if conn, err := grpc.Dial(address, grpc.WithBlock(), grpc.WithTransportCredentials(insecure.NewCredentials())); true {
			ch <- err
			conn.Close()
		}
	}()

	timer := time.NewTimer(opt.Duration)

	select {
	case err := <-ch:
		if err != nil {
			mlog.Error(context.TODO(), "grpc service not ready",
				mlog.Err(err),
				mlog.Any("option", opt))
			panic(err)
		}
	case <-timer.C:
		mlog.Error(context.TODO(), "grpc service not ready",
			mlog.Any("option", opt))
		panic("grpc service not ready")
	}
}

// TODO: should tls-related configurations be hard code here?
var (
	waitDuration  = time.Second * 1
	clientPemPath = "../../../configs/cert/client.pem"
	clientKeyPath = "../../../configs/cert/client.key"
)

// waitForServerReady wait for internal grpc service and external service to be ready, according to the params.
func waitForServerReady() {
	Params := &paramtable.Get().ProxyGrpcServerCfg
	waitForGrpcReady(newWaitOption(waitDuration, Params.InternalPort.GetAsInt(), 0, "", "", ""))
	waitForGrpcReady(newWaitOption(waitDuration, Params.Port.GetAsInt(), Params.TLSMode.GetAsInt(), clientPemPath, clientKeyPath, Params.CaPemPath.GetValue()))
}

func runAndWaitForServerReady(server *Server) error {
	if err := server.Prepare(); err != nil {
		return err
	}
	err := server.Run()
	if err != nil {
		return err
	}
	waitForServerReady()
	return nil
}

// /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func Test_NewServer(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	server := getServer(t)
	assert.NotNil(t, server)
	mockProxy := server.proxy.(*mocks.MockProxy)

	t.Run("Run", func(t *testing.T) {
		mockProxy.EXPECT().Init().Return(nil)
		mockProxy.EXPECT().Start().Return(nil)
		mockProxy.EXPECT().Register().Return(nil)
		mockProxy.EXPECT().GetRateLimiter().Return(nil, nil)
		mockProxy.EXPECT().SetMixCoordClient(mock.Anything).Return()
		mockProxy.EXPECT().UpdateStateCode(mock.Anything).Return()
		mockProxy.EXPECT().SetAddress(mock.Anything).Return()
		err := runAndWaitForServerReady(server)
		assert.NoError(t, err)

		mockProxy.EXPECT().Stop().Return(nil)
		err = server.Stop()
		assert.NoError(t, err)
	})

	t.Run("GetComponentStates", func(t *testing.T) {
		_, err := server.GetComponentStates(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("GetStatisticsChannel", func(t *testing.T) {
		mockProxy.EXPECT().GetStatisticsChannel(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.GetStatisticsChannel(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("InvalidateCollectionMetaCache", func(t *testing.T) {
		mockProxy.EXPECT().InvalidateCollectionMetaCache(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.InvalidateCollectionMetaCache(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("InvalidateShardLeaderCache", func(t *testing.T) {
		mockProxy.EXPECT().InvalidateShardLeaderCache(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.InvalidateShardLeaderCache(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("CreateCollection", func(t *testing.T) {
		mockProxy.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.CreateCollection(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("DropCollection", func(t *testing.T) {
		mockProxy.EXPECT().DropCollection(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.DropCollection(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("TruncateCollection", func(t *testing.T) {
		mockProxy.EXPECT().TruncateCollection(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.TruncateCollection(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("HasCollection", func(t *testing.T) {
		mockProxy.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(nil, nil)
		mockProxy.EXPECT().HasCollection(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.HasCollection(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("LoadCollection", func(t *testing.T) {
		mockProxy.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(nil, nil)
		mockProxy.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.LoadCollection(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("ReleaseCollection", func(t *testing.T) {
		mockProxy.EXPECT().ReleaseCollection(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.ReleaseCollection(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("DescribeCollection", func(t *testing.T) {
		mockProxy.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.DescribeCollection(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("BatchDescribeCollection", func(t *testing.T) {
		mockProxy.EXPECT().BatchDescribeCollection(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.BatchDescribeCollection(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("GetCollectionStatistics", func(t *testing.T) {
		mockProxy.EXPECT().GetCollectionStatistics(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.GetCollectionStatistics(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("ShowCollections", func(t *testing.T) {
		mockProxy.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.ShowCollections(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("CreatePartition", func(t *testing.T) {
		mockProxy.EXPECT().CreatePartition(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.CreatePartition(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("DropPartition", func(t *testing.T) {
		mockProxy.EXPECT().DropPartition(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.DropPartition(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("HasPartition", func(t *testing.T) {
		mockProxy.EXPECT().HasPartition(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.HasPartition(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("LoadPartitions", func(t *testing.T) {
		mockProxy.EXPECT().LoadPartitions(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.LoadPartitions(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("ReleasePartitions", func(t *testing.T) {
		mockProxy.EXPECT().ReleasePartitions(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.ReleasePartitions(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("GetPartitionStatistics", func(t *testing.T) {
		mockProxy.EXPECT().GetPartitionStatistics(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.GetPartitionStatistics(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("ShowPartitions", func(t *testing.T) {
		mockProxy.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.ShowPartitions(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("GetLoadingProgress", func(t *testing.T) {
		mockProxy.EXPECT().GetLoadingProgress(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.GetLoadingProgress(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("CreateIndex", func(t *testing.T) {
		mockProxy.EXPECT().CreateIndex(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.CreateIndex(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("DropIndex", func(t *testing.T) {
		mockProxy.EXPECT().DropIndex(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.DropIndex(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("DescribeIndex", func(t *testing.T) {
		mockProxy.EXPECT().DescribeIndex(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.DescribeIndex(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("GetIndexStatistics", func(t *testing.T) {
		mockProxy.EXPECT().GetIndexStatistics(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.GetIndexStatistics(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("GetIndexBuildProgress", func(t *testing.T) {
		mockProxy.EXPECT().GetIndexBuildProgress(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.GetIndexBuildProgress(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("GetIndexState", func(t *testing.T) {
		mockProxy.EXPECT().GetIndexState(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.GetIndexState(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("Insert", func(t *testing.T) {
		mockProxy.EXPECT().Insert(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.Insert(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("Delete", func(t *testing.T) {
		mockProxy.EXPECT().Delete(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.Delete(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("Upsert", func(t *testing.T) {
		mockProxy.EXPECT().Upsert(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.Upsert(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("Search", func(t *testing.T) {
		mockProxy.EXPECT().Search(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.Search(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("Flush", func(t *testing.T) {
		mockProxy.EXPECT().Flush(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.Flush(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("Query", func(t *testing.T) {
		mockProxy.EXPECT().Query(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.Query(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("CalcDistance", func(t *testing.T) {
		mockProxy.EXPECT().CalcDistance(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.CalcDistance(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("GetDdChannel", func(t *testing.T) {
		mockProxy.EXPECT().GetDdChannel(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.GetDdChannel(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("GetPersistentSegmentInfo", func(t *testing.T) {
		mockProxy.EXPECT().GetPersistentSegmentInfo(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.GetPersistentSegmentInfo(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("GetQuerySegmentInfo", func(t *testing.T) {
		mockProxy.EXPECT().GetQuerySegmentInfo(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.GetQuerySegmentInfo(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("Dummy", func(t *testing.T) {
		mockProxy.EXPECT().Dummy(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.Dummy(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("RegisterLink", func(t *testing.T) {
		mockProxy.EXPECT().RegisterLink(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.RegisterLink(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("GetMetrics", func(t *testing.T) {
		mockProxy.EXPECT().GetMetrics(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.GetMetrics(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("LoadBalance", func(t *testing.T) {
		mockProxy.EXPECT().LoadBalance(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.LoadBalance(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("CreateAlias", func(t *testing.T) {
		mockProxy.EXPECT().CreateAlias(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.CreateAlias(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("DropAlias", func(t *testing.T) {
		mockProxy.EXPECT().DropAlias(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.DropAlias(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("AlterAlias", func(t *testing.T) {
		mockProxy.EXPECT().AlterAlias(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.AlterAlias(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("DescribeAlias", func(t *testing.T) {
		mockProxy.EXPECT().DescribeAlias(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.DescribeAlias(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("ListAliases", func(t *testing.T) {
		mockProxy.EXPECT().ListAliases(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.ListAliases(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("GetCompactionState", func(t *testing.T) {
		mockProxy.EXPECT().GetCompactionState(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.GetCompactionState(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("ManualCompaction", func(t *testing.T) {
		mockProxy.EXPECT().ManualCompaction(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.ManualCompaction(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("GetCompactionStateWithPlans", func(t *testing.T) {
		mockProxy.EXPECT().GetCompactionStateWithPlans(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.GetCompactionStateWithPlans(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("CreateCredential", func(t *testing.T) {
		mockProxy.EXPECT().CreateCredential(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.CreateCredential(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("UpdateCredential", func(t *testing.T) {
		mockProxy.EXPECT().UpdateCredential(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.UpdateCredential(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("DeleteCredential", func(t *testing.T) {
		mockProxy.EXPECT().DeleteCredential(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.DeleteCredential(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("ListCredUsers", func(t *testing.T) {
		mockProxy.EXPECT().ListCredUsers(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.ListCredUsers(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("InvalidateCredentialCache", func(t *testing.T) {
		mockProxy.EXPECT().InvalidateCredentialCache(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.InvalidateCredentialCache(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("UpdateCredentialCache", func(t *testing.T) {
		mockProxy.EXPECT().UpdateCredentialCache(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.UpdateCredentialCache(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("CreateRole", func(t *testing.T) {
		mockProxy.EXPECT().CreateRole(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.CreateRole(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("AlterRole", func(t *testing.T) {
		mockProxy.EXPECT().AlterRole(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.AlterRole(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("DropRole", func(t *testing.T) {
		mockProxy.EXPECT().DropRole(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.DropRole(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("OperateUserRole", func(t *testing.T) {
		mockProxy.EXPECT().OperateUserRole(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.OperateUserRole(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("SelectRole", func(t *testing.T) {
		mockProxy.EXPECT().SelectRole(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.SelectRole(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("SelectUser", func(t *testing.T) {
		mockProxy.EXPECT().SelectUser(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.SelectUser(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("OperatePrivilege", func(t *testing.T) {
		mockProxy.EXPECT().OperatePrivilege(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.OperatePrivilege(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("SelectGrant", func(t *testing.T) {
		mockProxy.EXPECT().SelectGrant(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.SelectGrant(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("RefreshPrivilegeInfoCache", func(t *testing.T) {
		mockProxy.EXPECT().RefreshPolicyInfoCache(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.RefreshPolicyInfoCache(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("CheckHealth", func(t *testing.T) {
		mockProxy.EXPECT().CheckHealth(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.CheckHealth(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("RenameCollection", func(t *testing.T) {
		mockProxy.EXPECT().RenameCollection(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.RenameCollection(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("CreateResourceGroup", func(t *testing.T) {
		mockProxy.EXPECT().CreateResourceGroup(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.CreateResourceGroup(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("DropResourceGroup", func(t *testing.T) {
		mockProxy.EXPECT().DropResourceGroup(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.DropResourceGroup(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("TransferNode", func(t *testing.T) {
		mockProxy.EXPECT().TransferNode(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.TransferNode(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("TransferReplica", func(t *testing.T) {
		mockProxy.EXPECT().TransferReplica(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.TransferReplica(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("ListResourceGroups", func(t *testing.T) {
		mockProxy.EXPECT().ListResourceGroups(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.ListResourceGroups(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("DescribeResourceGroup", func(t *testing.T) {
		mockProxy.EXPECT().DescribeResourceGroup(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.DescribeResourceGroup(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("FlushAll", func(t *testing.T) {
		mockProxy.EXPECT().FlushAll(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.FlushAll(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("GetFlushAllState", func(t *testing.T) {
		mockProxy.EXPECT().GetFlushAllState(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.GetFlushAllState(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("CreateDatabase", func(t *testing.T) {
		mockProxy.EXPECT().CreateDatabase(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.CreateDatabase(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("DropDatabase", func(t *testing.T) {
		mockProxy.EXPECT().DropDatabase(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.DropDatabase(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("ListDatabase", func(t *testing.T) {
		mockProxy.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.ListDatabases(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("AlterDatabase", func(t *testing.T) {
		mockProxy.EXPECT().AlterDatabase(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.AlterDatabase(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("DescribeDatabase", func(t *testing.T) {
		mockProxy.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.DescribeDatabase(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("AllocTimestamp", func(t *testing.T) {
		mockProxy.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).Return(nil, nil)
		_, err := server.AllocTimestamp(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("RunAnalyzer", func(t *testing.T) {
		mockProxy.EXPECT().RunAnalyzer(mock.Anything, mock.Anything).Return(&milvuspb.RunAnalyzerResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		}, nil)
		_, err := server.RunAnalyzer(ctx, &milvuspb.RunAnalyzerRequest{})
		assert.NoError(t, err)
	})

	t.Run("Run with different config", func(t *testing.T) {
		mockProxy.EXPECT().Init().Return(nil)
		mockProxy.EXPECT().Start().Return(nil)
		mockProxy.EXPECT().Register().Return(nil)
		mockProxy.EXPECT().GetRateLimiter().Return(nil, nil)
		mockProxy.EXPECT().SetMixCoordClient(mock.Anything).Return()
		mockProxy.EXPECT().UpdateStateCode(mock.Anything).Return()
		mockProxy.EXPECT().SetAddress(mock.Anything).Return()
		// Update config and start server again to test with different config set.
		// This works as config will be initialized only once
		paramtable.Get().Save(proxy.Params.ProxyCfg.GinLogging.Key, "false")
		err := runAndWaitForServerReady(server)
		assert.NoError(t, err)

		mockProxy.EXPECT().Stop().Return(nil)
		err = server.Stop()
		assert.NoError(t, err)
	})
}

func TestServer_GetReplicateConfiguration(t *testing.T) {
	ctx := context.Background()
	req := &milvuspb.GetReplicateConfigurationRequest{}
	expectedResp := &milvuspb.GetReplicateConfigurationResponse{
		Status:        &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		Configuration: &commonpb.ReplicateConfiguration{},
	}
	mockProxy := mocks.NewMockProxy(t)
	mockProxy.EXPECT().GetReplicateConfiguration(mock.Anything, req).Return(expectedResp, nil)

	server := &Server{proxy: mockProxy}
	resp, err := server.GetReplicateConfiguration(ctx, req)

	assert.NoError(t, err)
	assert.Same(t, expectedResp, resp)
}

func TestServer_Check(t *testing.T) {
	ctx := context.Background()
	server := getServer(t)
	mockProxy := server.proxy.(*mocks.MockProxy)

	req := &grpc_health_v1.HealthCheckRequest{Service: ""}
	ret, err := server.Check(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, ret.Status)

	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(nil, errors.New("mock grpc unexpected error"))

	ret, err = server.Check(ctx, req)
	assert.Error(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, ret.Status)

	componentInfo := &milvuspb.ComponentInfo{
		NodeID:    0,
		Role:      "proxy",
		StateCode: commonpb.StateCode_Abnormal,
	}
	status := &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}
	componentState := &milvuspb.ComponentStates{
		State:  componentInfo,
		Status: status,
	}

	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(componentState, nil)
	ret, err = server.Check(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, ret.Status)

	status.ErrorCode = commonpb.ErrorCode_Success
	ret, err = server.Check(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, ret.Status)

	componentInfo.StateCode = commonpb.StateCode_Initializing
	ret, err = server.Check(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, ret.Status)

	componentInfo.StateCode = commonpb.StateCode_Healthy
	ret, err = server.Check(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, ret.Status)
}

func TestServer_Watch(t *testing.T) {
	server := getServer(t)
	mockProxy := server.proxy.(*mocks.MockProxy)

	watchServer := milvusmock.NewGrpcHealthWatchServer()
	resultChan := watchServer.Chan()
	req := &grpc_health_v1.HealthCheckRequest{Service: ""}
	// var ret *grpc_health_v1.HealthCheckResponse
	err := server.Watch(req, watchServer)
	ret := <-resultChan

	assert.NoError(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, ret.Status)

	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(nil, errors.New("mock grpc unexpected error"))

	err = server.Watch(req, watchServer)
	ret = <-resultChan
	assert.NoError(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, ret.Status)

	componentInfo := &milvuspb.ComponentInfo{
		NodeID:    0,
		Role:      "proxy",
		StateCode: commonpb.StateCode_Abnormal,
	}
	status := &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}
	componentState := &milvuspb.ComponentStates{
		State:  componentInfo,
		Status: status,
	}
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(componentState, nil)

	err = server.Watch(req, watchServer)
	ret = <-resultChan
	assert.NoError(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, ret.Status)

	status.ErrorCode = commonpb.ErrorCode_Success
	err = server.Watch(req, watchServer)
	ret = <-resultChan
	assert.NoError(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, ret.Status)

	componentInfo.StateCode = commonpb.StateCode_Initializing
	err = server.Watch(req, watchServer)
	ret = <-resultChan
	assert.NoError(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, ret.Status)

	componentInfo.StateCode = commonpb.StateCode_Healthy
	err = server.Watch(req, watchServer)
	ret = <-resultChan
	assert.NoError(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, ret.Status)
}

func Test_NewServer_HTTPServer_Enabled(t *testing.T) {
	server := getServer(t)

	mockProxy := server.proxy.(*mocks.MockProxy)
	mockProxy.EXPECT().Stop().Return(nil)
	mockProxy.EXPECT().Init().Return(nil)
	mockProxy.EXPECT().Start().Return(nil)
	mockProxy.EXPECT().Register().Return(nil)
	mockProxy.EXPECT().GetRateLimiter().Return(nil, nil)
	mockProxy.EXPECT().SetMixCoordClient(mock.Anything).Return()
	mockProxy.EXPECT().UpdateStateCode(mock.Anything).Return()
	mockProxy.EXPECT().SetAddress(mock.Anything).Return()

	paramtable.Get().Save(proxy.Params.HTTPCfg.Enabled.Key, "true")
	err := runAndWaitForServerReady(server)
	assert.NoError(t, err)
	err = server.Stop()
	assert.NoError(t, err)

	defer func() {
		e := recover()
		if e == nil {
			t.Fatalf("test should have panicked but did not")
		}
	}()
	// if disable works path not registered, so it shall not panic
	server.registerHTTPServer()
}

func Test_NewServer_HTTPServer_TimeoutDefaults(t *testing.T) {
	server := getServer(t)
	startProxyHTTPServerForTest(t, server)

	assert.Equal(t, 5*time.Second, server.httpServer.ReadHeaderTimeout)
	assert.Equal(t, time.Duration(0), server.httpServer.ReadTimeout)
	assert.Equal(t, time.Duration(0), server.httpServer.WriteTimeout)
	assert.Equal(t, 300*time.Second, server.httpServer.IdleTimeout)
	assert.Equal(t, 16<<20, server.httpServer.MaxHeaderBytes)
}

func Test_NewServer_HTTPServer_TimeoutConfigOverrides(t *testing.T) {
	params := paramtable.Get()
	params.Save("proxy.http.readHeaderTimeout", "7s")
	params.Save("proxy.http.readTimeout", "8s")
	params.Save("proxy.http.writeTimeout", "9s")
	params.Save("proxy.http.idleTimeout", "10s")
	params.Save("proxy.http.maxHeaderBytes", "2048")
	t.Cleanup(func() {
		params.Reset("proxy.http.readHeaderTimeout")
		params.Reset("proxy.http.readTimeout")
		params.Reset("proxy.http.writeTimeout")
		params.Reset("proxy.http.idleTimeout")
		params.Reset("proxy.http.maxHeaderBytes")
	})

	server := getServer(t)
	startProxyHTTPServerForTest(t, server)

	assert.Equal(t, 7*time.Second, server.httpServer.ReadHeaderTimeout)
	assert.Equal(t, 8*time.Second, server.httpServer.ReadTimeout)
	assert.Equal(t, 9*time.Second, server.httpServer.WriteTimeout)
	assert.Equal(t, 10*time.Second, server.httpServer.IdleTimeout)
	assert.Equal(t, 2048, server.httpServer.MaxHeaderBytes)
}

func startProxyHTTPServerForTest(t *testing.T, server *Server) {
	t.Helper()

	listener, err := netutil.NewListener()
	assert.NoError(t, err)
	server.listenerManager = &listenerManager{httpListener: listener}

	errChan := make(chan error, 2)
	server.wg.Add(1)
	go server.startHTTPServer(errChan)
	assert.NoError(t, <-errChan)
	t.Cleanup(func() {
		assert.NoError(t, server.httpServer.Close())
		server.wg.Wait()
	})
}

func getServer(t *testing.T) *Server {
	ctx := context.Background()
	server, err := NewServer(ctx, nil)
	assert.NotNil(t, server)
	assert.NoError(t, err)

	mockProxy := mocks.NewMockProxy(t)
	mockProxy.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(&milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			NodeID:    int64(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
			Role:      "MockProxy",
			StateCode: commonpb.StateCode_Healthy,
			ExtraInfo: nil,
		},
		SubcomponentStates: nil,
		Status:             &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
	}, nil).Maybe()
	server.proxy = mockProxy

	mockMC := mocks.NewMockMixCoordClient(t)
	mockMC.EXPECT().GetComponentStates(mock.Anything, mock.Anything, mock.Anything).Return(&milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			NodeID:    int64(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()),
			Role:      "MockRootCoord",
			StateCode: commonpb.StateCode_Healthy,
			ExtraInfo: nil,
		},
		SubcomponentStates: nil,
		Status:             &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
	}, nil).Maybe()
	server.mixCoordClient = mockMC
	return server
}

func expectProxyLifecycle(t *testing.T, server *Server) {
	t.Helper()
	mockProxy := server.proxy.(*mocks.MockProxy)
	mockProxy.EXPECT().Stop().Return(nil)
	mockProxy.EXPECT().Init().Return(nil)
	mockProxy.EXPECT().Start().Return(nil)
	mockProxy.EXPECT().Register().Return(nil)
	mockProxy.EXPECT().GetRateLimiter().Return(nil, nil)
	mockProxy.EXPECT().SetMixCoordClient(mock.Anything).Return()
	mockProxy.EXPECT().UpdateStateCode(mock.Anything).Return()
	mockProxy.EXPECT().SetAddress(mock.Anything).Return()
}

func getAvailablePortExcept(excluded ...int) int {
	for {
		port := funcutil.GetAvailablePort()
		duplicated := false
		for _, excludedPort := range excluded {
			if port == excludedPort {
				duplicated = true
				break
			}
		}
		if !duplicated {
			return port
		}
	}
}

func configureHTTP2ProxyParams(t *testing.T, tlsMode int, portShare bool) (int, int) {
	t.Helper()
	Params := &paramtable.Get().ProxyGrpcServerCfg
	resetKeys := []string{
		Params.TLSMode.Key,
		Params.Port.Key,
		Params.InternalPort.Key,
		Params.ServerPemPath.Key,
		Params.ServerKeyPath.Key,
		Params.CaPemPath.Key,
		proxy.Params.HTTPCfg.Enabled.Key,
		proxy.Params.HTTPCfg.Port.Key,
	}
	for _, key := range resetKeys {
		key := key
		t.Cleanup(func() { paramtable.Get().Reset(key) })
	}

	grpcPort := getAvailablePortExcept()
	internalPort := getAvailablePortExcept(grpcPort)
	httpPort := grpcPort
	httpPortValue := ""
	if !portShare {
		httpPort = getAvailablePortExcept(grpcPort, internalPort)
		httpPortValue = strconv.Itoa(httpPort)
	}

	paramtable.Get().Save(Params.TLSMode.Key, strconv.Itoa(tlsMode))
	paramtable.Get().Save(Params.Port.Key, strconv.Itoa(grpcPort))
	paramtable.Get().Save(Params.InternalPort.Key, strconv.Itoa(internalPort))
	paramtable.Get().Save(Params.ServerPemPath.Key, "../../../configs/cert/server.pem")
	paramtable.Get().Save(Params.ServerKeyPath.Key, "../../../configs/cert/server.key")
	paramtable.Get().Save(Params.CaPemPath.Key, "../../../configs/cert/ca.pem")
	paramtable.Get().Save(proxy.Params.HTTPCfg.Enabled.Key, "true")
	paramtable.Get().Save(proxy.Params.HTTPCfg.Port.Key, httpPortValue)
	return grpcPort, httpPort
}

func startHTTP2ProxyServer(t *testing.T, tlsMode int, portShare bool) (*Server, int, int) {
	t.Helper()
	server := getServer(t)
	expectProxyLifecycle(t, server)
	grpcPort, httpPort := configureHTTP2ProxyParams(t, tlsMode, portShare)
	t.Cleanup(func() { require.NoError(t, server.Stop()) })
	require.NoError(t, runAndWaitForServerReady(server))
	return server, grpcPort, httpPort
}

func newH2CClient(t *testing.T) *http.Client {
	t.Helper()
	transport := &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, network, addr)
		},
	}
	client := &http.Client{Transport: transport}
	t.Cleanup(client.CloseIdleConnections)
	return client
}

func newTLSHTTP2Client(t *testing.T, tlsMode int) *http.Client {
	t.Helper()
	certPool := x509.NewCertPool()
	ca, err := os.ReadFile("../../../configs/cert/ca.pem")
	require.NoError(t, err)
	require.True(t, certPool.AppendCertsFromPEM(ca))
	tlsConf := &tls.Config{
		RootCAs:    certPool,
		ServerName: "localhost",
		NextProtos: []string{http2.NextProtoTLS},
		MinVersion: tls.VersionTLS12,
	}
	if tlsMode == 2 {
		cert, err := tls.LoadX509KeyPair(clientPemPath, clientKeyPath)
		require.NoError(t, err)
		tlsConf.Certificates = []tls.Certificate{cert}
	}
	client := &http.Client{Transport: &http2.Transport{TLSClientConfig: tlsConf}}
	t.Cleanup(client.CloseIdleConnections)
	return client
}

func assertHTTP2Response(t *testing.T, client *http.Client, url string) {
	t.Helper()
	resp, err := client.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	_, err = io.Copy(io.Discard, resp.Body)
	require.NoError(t, err)
	assert.Equal(t, "HTTP/2.0", resp.Proto)
}

func Test_NewServer_TLS_TwoWay(t *testing.T) {
	server := getServer(t)
	Params := &paramtable.Get().ProxyGrpcServerCfg

	mockProxy := server.proxy.(*mocks.MockProxy)
	mockProxy.EXPECT().Stop().Return(nil)
	mockProxy.EXPECT().Init().Return(nil)
	mockProxy.EXPECT().Start().Return(nil)
	mockProxy.EXPECT().Register().Return(nil)
	mockProxy.EXPECT().GetRateLimiter().Return(nil, nil)
	mockProxy.EXPECT().SetMixCoordClient(mock.Anything).Return()
	mockProxy.EXPECT().UpdateStateCode(mock.Anything).Return()
	mockProxy.EXPECT().SetAddress(mock.Anything).Return()

	paramtable.Get().Save(Params.TLSMode.Key, "2")
	paramtable.Get().Save(Params.ServerPemPath.Key, "../../../configs/cert/server.pem")
	paramtable.Get().Save(Params.ServerKeyPath.Key, "../../../configs/cert/server.key")
	paramtable.Get().Save(Params.CaPemPath.Key, "../../../configs/cert/ca.pem")
	paramtable.Get().Save(proxy.Params.HTTPCfg.Enabled.Key, "false")

	err := runAndWaitForServerReady(server)
	assert.NoError(t, err)
	assert.NotNil(t, server.grpcExternalServer)
	err = server.Stop()
	assert.NoError(t, err)
}

func Test_NewServer_TLS_OneWay(t *testing.T) {
	server := getServer(t)
	Params := &paramtable.Get().ProxyGrpcServerCfg

	mockProxy := server.proxy.(*mocks.MockProxy)
	mockProxy.EXPECT().Stop().Return(nil)
	mockProxy.EXPECT().Init().Return(nil)
	mockProxy.EXPECT().Start().Return(nil)
	mockProxy.EXPECT().Register().Return(nil)
	mockProxy.EXPECT().GetRateLimiter().Return(nil, nil)
	mockProxy.EXPECT().SetMixCoordClient(mock.Anything).Return()
	mockProxy.EXPECT().UpdateStateCode(mock.Anything).Return()
	mockProxy.EXPECT().SetAddress(mock.Anything).Return()

	paramtable.Get().Save(Params.TLSMode.Key, "1")
	paramtable.Get().Save(Params.ServerPemPath.Key, "../../../configs/cert/server.pem")
	paramtable.Get().Save(Params.ServerKeyPath.Key, "../../../configs/cert/server.key")
	paramtable.Get().Save(proxy.Params.HTTPCfg.Enabled.Key, "false")

	err := runAndWaitForServerReady(server)
	assert.NoError(t, err)
	assert.NotNil(t, server.grpcExternalServer)
	err = server.Stop()
	assert.NoError(t, err)
}

func Test_NewServer_TLS_FileNotExisted(t *testing.T) {
	server := getServer(t)
	Params := &paramtable.Get().ProxyGrpcServerCfg

	mockProxy := server.proxy.(*mocks.MockProxy)
	mockProxy.EXPECT().Stop().Return(nil)
	mockProxy.EXPECT().GetRateLimiter().Return(nil, nil)
	mockProxy.EXPECT().SetAddress(mock.Anything).Return()

	paramtable.Get().Save(Params.TLSMode.Key, "1")
	paramtable.Get().Save(Params.ServerPemPath.Key, "../not/existed/server.pem")
	paramtable.Get().Save(Params.ServerKeyPath.Key, "../../../configs/cert/server.key")
	paramtable.Get().Save(proxy.Params.HTTPCfg.Enabled.Key, "false")
	err := runAndWaitForServerReady(server)
	assert.Error(t, err)
	server.Stop()

	paramtable.Get().Save(Params.TLSMode.Key, "2")
	paramtable.Get().Save(Params.ServerPemPath.Key, "../not/existed/server.pem")
	paramtable.Get().Save(Params.CaPemPath.Key, "../../../configs/cert/ca.pem")
	err = runAndWaitForServerReady(server)
	assert.Error(t, err)
	server.Stop()

	paramtable.Get().Save(Params.ServerPemPath.Key, "../../../configs/cert/server.pem")
	paramtable.Get().Save(Params.CaPemPath.Key, "../not/existed/ca.pem")
	err = runAndWaitForServerReady(server)
	assert.Error(t, err)
	server.Stop()

	paramtable.Get().Save(Params.CaPemPath.Key, "service.go")
	err = runAndWaitForServerReady(server)
	assert.Error(t, err)
	server.Stop()
}

// freeTCPPort grabs a random unused TCP port. The TLS HTTP-server tests
// below hardcoded port 8080 originally, which collides with anything else
// already bound to that port on the dev machine (e.g. the TEI text-embedding
// container in our compose stack).
func freeTCPPort(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	assert.NoError(t, ln.Close())
	return strconv.Itoa(port)
}

func Test_NewHTTPServer_TLS_TwoWay(t *testing.T) {
	server := getServer(t)

	mockProxy := server.proxy.(*mocks.MockProxy)
	mockProxy.EXPECT().Stop().Return(nil)
	mockProxy.EXPECT().Init().Return(nil)
	mockProxy.EXPECT().Start().Return(nil)
	mockProxy.EXPECT().Register().Return(nil)
	mockProxy.EXPECT().GetRateLimiter().Return(nil, nil)
	mockProxy.EXPECT().SetMixCoordClient(mock.Anything).Return()
	mockProxy.EXPECT().UpdateStateCode(mock.Anything).Return()
	mockProxy.EXPECT().SetAddress(mock.Anything).Return()

	Params := &paramtable.Get().ProxyGrpcServerCfg

	paramtable.Get().Save(Params.TLSMode.Key, "2")
	paramtable.Get().Save(Params.ServerPemPath.Key, "../../../configs/cert/server.pem")
	paramtable.Get().Save(Params.ServerKeyPath.Key, "../../../configs/cert/server.key")
	paramtable.Get().Save(Params.CaPemPath.Key, "../../../configs/cert/ca.pem")
	paramtable.Get().Save(proxy.Params.HTTPCfg.Enabled.Key, "true")
	paramtable.Get().Save(proxy.Params.HTTPCfg.Port.Key, freeTCPPort(t))

	err := runAndWaitForServerReady(server)
	assert.Nil(t, err)
	assert.NotNil(t, server.grpcExternalServer)
	err = server.Stop()
	assert.Nil(t, err)

	paramtable.Get().Save(proxy.Params.HTTPCfg.Port.Key, "19529")
	err = runAndWaitForServerReady(server)
	assert.NotNil(t, err)
	err = server.Stop()
	assert.Nil(t, err)
}

func Test_NewHTTPServer_TLS_OneWay(t *testing.T) {
	server := getServer(t)

	mockProxy := server.proxy.(*mocks.MockProxy)
	mockProxy.EXPECT().Stop().Return(nil)
	mockProxy.EXPECT().Init().Return(nil)
	mockProxy.EXPECT().Start().Return(nil)
	mockProxy.EXPECT().Register().Return(nil)
	mockProxy.EXPECT().GetRateLimiter().Return(nil, nil)
	mockProxy.EXPECT().SetMixCoordClient(mock.Anything).Return()
	mockProxy.EXPECT().UpdateStateCode(mock.Anything).Return()
	mockProxy.EXPECT().SetAddress(mock.Anything).Return()

	Params := &paramtable.Get().ProxyGrpcServerCfg

	paramtable.Get().Save(Params.TLSMode.Key, "1")
	paramtable.Get().Save(Params.ServerPemPath.Key, "../../../configs/cert/server.pem")
	paramtable.Get().Save(Params.ServerKeyPath.Key, "../../../configs/cert/server.key")
	paramtable.Get().Save(proxy.Params.HTTPCfg.Enabled.Key, "true")
	paramtable.Get().Save(proxy.Params.HTTPCfg.Port.Key, freeTCPPort(t))

	err := runAndWaitForServerReady(server)
	fmt.Printf("err: %v\n", err)
	assert.Nil(t, err)
	assert.NotNil(t, server.grpcExternalServer)
	err = server.Stop()
	assert.Nil(t, err)

	paramtable.Get().Save(proxy.Params.HTTPCfg.Port.Key, "19529")
	fmt.Printf("err: %v\n", err)
	err = runAndWaitForServerReady(server)
	assert.NotNil(t, err)
	server.Stop()
}

func Test_NewHTTPServer_H2C(t *testing.T) {
	_, _, httpPort := startHTTP2ProxyServer(t, 0, false)
	assertHTTP2Response(t, newH2CClient(t), fmt.Sprintf("http://localhost:%d/not-found", httpPort))
}

func Test_NewHTTPServer_TLS_HTTP2(t *testing.T) {
	for _, tlsMode := range []int{1, 2} {
		tlsMode := tlsMode
		t.Run(fmt.Sprintf("tls_mode_%d", tlsMode), func(t *testing.T) {
			_, _, httpPort := startHTTP2ProxyServer(t, tlsMode, false)
			assertHTTP2Response(t, newTLSHTTP2Client(t, tlsMode), fmt.Sprintf("https://localhost:%d/not-found", httpPort))
		})
	}
}

func Test_NewHTTPServer_PortShare_H2C(t *testing.T) {
	enableCustomInterceptor = false
	t.Cleanup(func() { enableCustomInterceptor = true })
	_, grpcPort, _ := startHTTP2ProxyServer(t, 0, true)
	assertHTTP2Response(t, newH2CClient(t), fmt.Sprintf("http://localhost:%d/not-found", grpcPort))

	ctx, cancel := context.WithTimeout(context.Background(), waitDuration)
	defer cancel()
	conn, err := grpc.DialContext(ctx, fmt.Sprintf("localhost:%d", grpcPort), grpc.WithBlock(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	resp, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.GetStatus())
}

func Test_NewHTTPServer_TLS_FileNotExisted(t *testing.T) {
	server := getServer(t)

	mockProxy := server.proxy.(*mocks.MockProxy)
	mockProxy.EXPECT().Stop().Return(nil)
	mockProxy.EXPECT().SetAddress(mock.Anything).Return().Maybe()
	Params := &paramtable.Get().ProxyGrpcServerCfg

	paramtable.Get().Save(Params.TLSMode.Key, "1")
	paramtable.Get().Save(Params.ServerPemPath.Key, "../not/existed/server.pem")
	paramtable.Get().Save(Params.ServerKeyPath.Key, "../../../configs/cert/server.key")
	paramtable.Get().Save(proxy.Params.HTTPCfg.Enabled.Key, "true")
	paramtable.Get().Save(proxy.Params.HTTPCfg.Port.Key, freeTCPPort(t))
	err := runAndWaitForServerReady(server)
	assert.NotNil(t, err)
	server.Stop()

	paramtable.Get().Save(Params.TLSMode.Key, "2")
	paramtable.Get().Save(Params.ServerPemPath.Key, "../not/existed/server.pem")
	paramtable.Get().Save(Params.CaPemPath.Key, "../../../configs/cert/ca.pem")
	err = runAndWaitForServerReady(server)
	assert.NotNil(t, err)
	server.Stop()

	paramtable.Get().Save(Params.ServerPemPath.Key, "../../../configs/cert/server.pem")
	paramtable.Get().Save(Params.CaPemPath.Key, "../not/existed/ca.pem")
	err = runAndWaitForServerReady(server)
	assert.NotNil(t, err)
	server.Stop()

	paramtable.Get().Save(Params.CaPemPath.Key, "service.go")
	err = runAndWaitForServerReady(server)
	assert.NotNil(t, err)
	server.Stop()
}

func Test_NewServer_GetVersion(t *testing.T) {
	req := &milvuspb.GetVersionRequest{}
	t.Run("test get version failed", func(t *testing.T) {
		server := getServer(t)
		resp, err := server.GetVersion(context.TODO(), req)
		assert.Empty(t, resp.GetVersion())
		assert.NoError(t, err)
	})

	t.Run("test get version failed", func(t *testing.T) {
		server := getServer(t)
		err := os.Setenv(metricsinfo.GitBuildTagsEnvKey, "v1")
		assert.NoError(t, err)
		resp, err := server.GetVersion(context.TODO(), req)
		assert.Equal(t, "v1", resp.GetVersion())
		assert.NoError(t, err)
	})
}

func TestNotImplementedAPIs(t *testing.T) {
	server := getServer(t)

	t.Run("ListIndexedSegment", func(t *testing.T) {
		assert.NotPanics(t, func() {
			resp, err := server.ListIndexedSegment(context.TODO(), &federpb.ListIndexedSegmentRequest{})
			assert.NoError(t, err)
			assert.NotNil(t, resp)
			assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
		})
	})

	t.Run("DescribeSegmentIndexData", func(t *testing.T) {
		assert.NotPanics(t, func() {
			resp, err := server.DescribeSegmentIndexData(context.TODO(), &federpb.DescribeSegmentIndexDataRequest{})
			assert.NoError(t, err)
			assert.NotNil(t, resp)
			assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
		})
	})
}

func TestSnapshotAPIs(t *testing.T) {
	ctx := context.Background()
	server := getServer(t)
	mockProxy := server.proxy.(*mocks.MockProxy)

	t.Run("GetRestoreSnapshotState", func(t *testing.T) {
		req := &milvuspb.GetRestoreSnapshotStateRequest{}
		mockResp := &milvuspb.GetRestoreSnapshotStateResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		}
		mockProxy.EXPECT().GetRestoreSnapshotState(mock.Anything, req).Return(mockResp, nil)
		resp, err := server.GetRestoreSnapshotState(ctx, req)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, mockResp, resp)
	})

	t.Run("ListRestoreSnapshotJobs", func(t *testing.T) {
		req := &milvuspb.ListRestoreSnapshotJobsRequest{}
		mockResp := &milvuspb.ListRestoreSnapshotJobsResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		}
		mockProxy.EXPECT().ListRestoreSnapshotJobs(mock.Anything, req).Return(mockResp, nil)
		resp, err := server.ListRestoreSnapshotJobs(ctx, req)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, mockResp, resp)
	})

	t.Run("BatchUpdateManifest", func(t *testing.T) {
		req := &milvuspb.BatchUpdateManifestRequest{
			CollectionName: "test_collection",
			Items: []*milvuspb.BatchUpdateManifestItem{
				{SegmentId: 1, ManifestVersion: 10},
			},
		}
		mockResp := &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
		mockProxy.EXPECT().BatchUpdateManifest(mock.Anything, req).Return(mockResp, nil)
		resp, err := server.BatchUpdateManifest(ctx, req)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, mockResp, resp)
	})
}

func TestHttpAuthenticate(t *testing.T) {
	paramtable.Get().Save(proxy.Params.CommonCfg.AuthorizationEnabled.Key, "true")
	defer paramtable.Get().Reset(proxy.Params.CommonCfg.AuthorizationEnabled.Key)
	ctx, _ := gin.CreateTestContext(nil)
	ctx.Request = httptest.NewRequest("GET", "/test", nil)
	{
		assert.Panics(t, func() {
			ctx.Request.Header.Set("Authorization", "Bearer 123456")
			authenticate(ctx)
		})
	}

	{
		hookutil.SetMockAPIHook("foo", nil)
		defer hookutil.SetMockAPIHook("", nil)
		ctx.Request.Header.Set("Authorization", "Bearer 123456")
		authenticate(ctx)
		ctxName, _ := ctx.Get(httpserver.ContextUsername)
		assert.Equal(t, "foo", ctxName)
	}
}

// runAdminAuthMiddleware drives a console request through the whole assembled
// chain rather than the middleware alone: a middleware exercised on its own
// cannot show that something ahead of it answered first, which is the failure
// this design is arranged to prevent.
func runAdminAuthMiddleware(t *testing.T, setAuth func(*http.Request)) *httptest.ResponseRecorder {
	t.Helper()
	return requestMetricsPort(t, http.MethodGet, mhttp.ClusterConfigsPath, setAuth)
}

func TestAdminAuthMiddlewareIsInertWhileDisabled(t *testing.T) {
	paramtable.Get().Reset(proxy.Params.CommonCfg.AdminAuthEnabled.Key)
	require.False(t, proxy.Params.CommonCfg.AuthorizationEnabled.GetAsBool())

	// A verifier that fails the test if consulted: asserting only on the status
	// would pass with the middleware deleted outright, since with both flags
	// off every branch is a no-op anyway.
	mhttp.RegisterManagementVerifier(mhttp.VerifierSlotProxy, func(context.Context, string, string) error {
		t.Error("no credential may be checked while the gate is off")
		return nil
	})
	t.Cleanup(func() { mhttp.RegisterManagementVerifier(mhttp.VerifierSlotProxy, nil) })

	// Assert the request reached a handler, not just that nothing wrote a
	// status: an untouched recorder reports 200 all on its own. A cross-site
	// header must be ignored too — the refusal is part of the gate, not
	// something the flag-off posture does.
	w := requestMetricsPort(t, http.MethodGet, mhttp.ClusterConfigsPath, func(req *http.Request) {
		req.SetBasicAuth("alice", "whatever")
		req.Header.Set("Sec-Fetch-Site", "cross-site")
	})
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "ok", w.Body.String())
}

func TestAdminAuthMiddlewarePreservesManagementStoreFailure(t *testing.T) {
	paramtable.Get().Save(proxy.Params.CommonCfg.AdminAuthEnabled.Key, "true")
	t.Cleanup(func() {
		paramtable.Get().Reset(proxy.Params.CommonCfg.AdminAuthEnabled.Key)
		mhttp.RegisterManagementVerifier(mhttp.VerifierSlotProxy, nil)
	})

	const internalCause = "credential backend sentinel must stay private"
	mhttp.RegisterManagementVerifier(mhttp.VerifierSlotProxy, func(context.Context, string, string) error {
		return errors.New(internalCause)
	})

	w := runAdminAuthMiddleware(t, func(req *http.Request) {
		req.SetBasicAuth(util.UserRoot, "correct-password")
	})

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.NotContains(t, w.Body.String(), internalCause)
	assert.Contains(t, w.Body.String(), "cannot verify credentials on this node")
	assert.Empty(t, w.Header().Get("WWW-Authenticate"))
}

func TestAdminAuthMiddlewareRejectsNonRootBeforePasswordVerification(t *testing.T) {
	paramtable.Get().Save(proxy.Params.CommonCfg.AdminAuthEnabled.Key, "true")
	verifierCalls := 0
	mhttp.RegisterManagementVerifier(mhttp.VerifierSlotProxy, func(context.Context, string, string) error {
		verifierCalls++
		return nil
	})
	t.Cleanup(func() {
		paramtable.Get().Reset(proxy.Params.CommonCfg.AdminAuthEnabled.Key)
		mhttp.RegisterManagementVerifier(mhttp.VerifierSlotProxy, nil)
	})

	for _, password := range []string{"wrong-password", "otherwise-valid-password"} {
		w := runAdminAuthMiddleware(t, func(req *http.Request) {
			req.SetBasicAuth("alice", password)
		})
		assert.Equal(t, http.StatusForbidden, w.Code)
		assert.Contains(t, w.Body.String(), "only root user")
		assert.Empty(t, w.Header().Get("WWW-Authenticate"))
	}
	assert.Zero(t, verifierCalls, "a non-root username must not reach the credential store")
}

func TestAdminAuthMiddlewareRequiresCredentials(t *testing.T) {
	paramtable.Get().Save(proxy.Params.CommonCfg.AdminAuthEnabled.Key, "true")
	t.Cleanup(func() { paramtable.Get().Reset(proxy.Params.CommonCfg.AdminAuthEnabled.Key) })

	w := runAdminAuthMiddleware(t, nil)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
	// These routes are what the web console fetches, and it has no login form:
	// without the challenge the browser swallows the 401 and every panel shows
	// an error the operator has no way to clear.
	assert.Contains(t, w.Header().Get("WWW-Authenticate"), "Basic realm=")
}

// metricsPortEngine builds the metrics-port gin tree through the same function
// registerHTTPServer uses, so these tests exercise the handler chain that
// actually runs. Reassembling it by hand instead would leave every test green
// if the production assembly changed shape -- and the assembly is exactly what
// makes the gate apply, since RouterGroup.Group snapshots the parent's handler
// slice.
//
// consoleAPIPaths mirrors the console half of Proxy.RegisterRestRouter, so a
// test can drive newMetricsPortEngine without a real *proxy.Proxy. It is a
// sample, not the source of truth: what actually keeps the console surface
// gated is that every route it registers carries the /_ prefix the middleware
// classifies on, which TestRegisterRestRouterOnlyPublishesConsolePaths pins in
// the package that owns those registrations.
var consoleAPIPaths = []string{
	mhttp.ClusterInfoPath, mhttp.ClusterConfigsPath, mhttp.ClusterClientsPath,
	mhttp.ClusterDependenciesPath, mhttp.HookConfigsPath, mhttp.SlowQueryPath,
	mhttp.QCTargetPath, mhttp.QCDistPath, mhttp.QCReplicaPath, mhttp.QCResourceGroupPath,
	mhttp.QCAllTasksPath, mhttp.QCSegmentsPath,
	mhttp.QNSegmentsPath, mhttp.QNChannelsPath,
	mhttp.DCDistPath, mhttp.DCCompactionTasksPath, mhttp.DCImportTasksPath,
	mhttp.DCBuildIndexTasksPath, mhttp.IndexListPath, mhttp.DCSegmentsPath,
	mhttp.DNSyncTasksPath, mhttp.DNSegmentsPath, mhttp.DNChannelsPath,
	mhttp.DatabaseListPath, mhttp.DatabaseDescPath,
	mhttp.CollectionListPath, mhttp.CollectionDescPath,
}

// telemetryAPIPaths are the console routes that also carry their own copy of
// the check, and the only parameterised ones on this port.
var telemetryAPIPaths = []string{
	mhttp.TelemetryClientsPath,
	mhttp.TelemetryClientsPath + "/:clientId",
	mhttp.TelemetryClientsPath + "/:clientId/config",
	mhttp.TelemetryClientHistoryPath,
	mhttp.TelemetryCommandsPath,
	mhttp.TelemetryCommandsPath + "/:commandId",
}

// consoleProxy is a ProxyComponent that also registers the console routes, so
// newMetricsPortEngine takes the same branch it takes in production. Without
// it the assertion is on a concrete *proxy.Proxy that no test can build, and
// the entire console surface stays uncovered.
type consoleProxy struct {
	types.ProxyComponent
}

func (consoleProxy) RegisterRestRouter(router gin.IRouter) {
	ok := func(c *gin.Context) { c.String(http.StatusOK, "ok") }
	for _, path := range consoleAPIPaths {
		router.GET(path, ok)
	}
	telemetryAuth := proxy.TelemetryAuthMiddleware()
	for _, path := range telemetryAPIPaths {
		router.GET(path, telemetryAuth, ok)
	}
}

func metricsPortEngine(t *testing.T) *gin.Engine {
	t.Helper()
	previousMode := gin.Mode()
	gin.SetMode(gin.TestMode)
	t.Cleanup(func() { gin.SetMode(previousMode) })

	mockProxy := mocks.NewMockProxy(t)
	mockProxy.EXPECT().DropCollection(mock.Anything, mock.Anything).
		Return(merr.Success(), nil).Maybe()
	return newMetricsPortEngine(gin.New(), consoleProxy{mockProxy})
}

func requestMetricsPort(t *testing.T, method, path string, setAuth func(*http.Request)) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	req := httptest.NewRequest(method, apiPathPrefix+path, nil)
	if setAuth != nil {
		setAuth(req)
	}
	metricsPortEngine(t).ServeHTTP(w, req)
	return w
}

func runDataPlaneAuthMiddleware(t *testing.T, setAuth func(*http.Request)) *httptest.ResponseRecorder {
	t.Helper()
	return requestMetricsPort(t, http.MethodDelete, "/collection", setAuth)
}

// The console has no login form; its credentials come from the browser, which
// only prompts when a 401 carries WWW-Authenticate. Nothing in the chain may
// answer the console's 401 before the middleware that sets that header — which
// is exactly what a second, stacked credential check would do.
//
// The data plane must NOT send it. The challenge is what makes a browser hold
// root's credential for a path, and holding it for a route that drops
// collections is the thing the cross-site check then has to defend.
func TestChallengeOnConsoleRoutesOnly(t *testing.T) {
	paramtable.Get().Save(proxy.Params.CommonCfg.AdminAuthEnabled.Key, "true")
	t.Cleanup(func() { paramtable.Get().Reset(proxy.Params.CommonCfg.AdminAuthEnabled.Key) })

	w := requestMetricsPort(t, http.MethodGet, mhttp.ClusterConfigsPath, nil)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Contains(t, w.Header().Get("WWW-Authenticate"), "Basic realm=",
		"without this the browser swallows the 401 and the console cannot sign in")

	w = runDataPlaneAuthMiddleware(t, nil)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Empty(t, w.Header().Get("WWW-Authenticate"),
		"a browser must not be taught to hold root's credential for the data plane")
}

// /api/v1/health predates /healthz and is still wired into load balancers.
// A flag named after the management plane must not take it down.
func TestHealthStaysOpenWithGateOn(t *testing.T) {
	paramtable.Get().Save(proxy.Params.CommonCfg.AdminAuthEnabled.Key, "true")
	t.Cleanup(func() { paramtable.Get().Reset(proxy.Params.CommonCfg.AdminAuthEnabled.Key) })

	w := requestMetricsPort(t, http.MethodGet, "/health", nil)
	assert.Equal(t, http.StatusOK, w.Code)

	// The exemption has to stay narrow: a neighboring data-plane route must
	// not inherit it.
	w = runDataPlaneAuthMiddleware(t, nil)
	assert.Equal(t, http.StatusUnauthorized, w.Code,
		"only /api/v1/health is exempt, not everything near it")
}

// ... and must not take the opposite trade either. Exempting /api/v1/health
// from the new gate must not also drop the authentication authorizationEnabled
// already required there: a hardening flag that loosens an existing check is
// worse than one that does nothing.
func TestHealthKeepsExistingAuthWhenGateOn(t *testing.T) {
	paramtable.Get().Save(proxy.Params.CommonCfg.AuthorizationEnabled.Key, "true")
	paramtable.Get().Save(proxy.Params.CommonCfg.AdminAuthEnabled.Key, "true")
	t.Cleanup(func() {
		paramtable.Get().Reset(proxy.Params.CommonCfg.AuthorizationEnabled.Key)
		paramtable.Get().Reset(proxy.Params.CommonCfg.AdminAuthEnabled.Key)
	})

	w := requestMetricsPort(t, http.MethodGet, "/health", nil)
	assert.Equal(t, http.StatusUnauthorized, w.Code,
		"enabling the gate must not remove authorizationEnabled's coverage of /api/v1/health")
}

// The metrics port also carries the legacy REST data plane, where DELETE
// /api/v1/collection drops a collection. Gating only the operator surface would
// leave an operator who enabled adminAuthEnabled — and then exposed the port,
// which is what the flag is for — one unauthenticated request from data loss.
//
// With no data-plane authentication configured it requires root, verified from
// the cached hash: accepting a caller-chosen username would mean one credential
// lookup per name that the proxy's cache cannot absorb, and with
// authorizationEnabled off there is no non-root caller to lock out.
func TestDataPlaneRequiresRootWhenAuthorizationDisabled(t *testing.T) {
	paramtable.Get().Reset(proxy.Params.CommonCfg.AdminAuthEnabled.Key)
	require.False(t, proxy.Params.CommonCfg.AuthorizationEnabled.GetAsBool(),
		"this test is about the default posture: data-plane authorization off")

	w := runDataPlaneAuthMiddleware(t, nil)
	assert.Equal(t, http.StatusOK, w.Code, "unchanged while both gates are off")

	paramtable.Get().Save(proxy.Params.CommonCfg.AdminAuthEnabled.Key, "true")
	verifierCalls := 0
	mhttp.RegisterManagementVerifier(mhttp.VerifierSlotProxy, func(context.Context, string, string) error {
		verifierCalls++
		return nil
	})
	t.Cleanup(func() {
		paramtable.Get().Reset(proxy.Params.CommonCfg.AdminAuthEnabled.Key)
		mhttp.RegisterManagementVerifier(mhttp.VerifierSlotProxy, nil)
	})

	w = runDataPlaneAuthMiddleware(t, nil)
	assert.Equal(t, http.StatusUnauthorized, w.Code,
		"the metrics port must not carry an anonymous drop-collection route once the gate is on")

	w = runDataPlaneAuthMiddleware(t, func(req *http.Request) {
		req.SetBasicAuth("alice", "any-password")
	})
	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Zero(t, verifierCalls,
		"a caller-chosen username must not reach the credential store")

	w = runDataPlaneAuthMiddleware(t, func(req *http.Request) {
		req.SetBasicAuth(util.UserRoot, "any-password")
	})
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, 1, verifierCalls)
}

// Where data-plane authentication IS configured, the gate must not narrow it:
// an RBAC user or an API key that works on the main service port has to keep
// working here, because there is nowhere else for it to go.
func TestDataPlaneKeepsNonRootCallersWhenAuthorizationEnabled(t *testing.T) {
	paramtable.Get().Save(proxy.Params.CommonCfg.AuthorizationEnabled.Key, "true")
	paramtable.Get().Save(proxy.Params.CommonCfg.AdminAuthEnabled.Key, "true")
	hookutil.SetMockAPIHook("alice", nil)
	t.Cleanup(func() {
		paramtable.Get().Reset(proxy.Params.CommonCfg.AuthorizationEnabled.Key)
		paramtable.Get().Reset(proxy.Params.CommonCfg.AdminAuthEnabled.Key)
		hookutil.SetMockAPIHook("", nil)
	})

	w := runDataPlaneAuthMiddleware(t, func(req *http.Request) {
		req.Header.Set("Authorization", "Bearer some-api-key")
	})
	assert.Equal(t, http.StatusOK, w.Code,
		"enabling the operator gate must not lock non-root callers out of the data plane")
}

// The console's challenge is what teaches a browser to hold root's credential
// for this origin, and the browser replays it at the data-plane routes on the
// same origin just as readily: a cross-site form post to /api/v1/collection is
// a top-level navigation, which carries cached credentials and needs no
// preflight. Every gated branch has to refuse it, not just the console one.
func TestMetricsPortRefusesCrossSiteOnEveryGatedBranch(t *testing.T) {
	paramtable.Get().Save(proxy.Params.CommonCfg.AdminAuthEnabled.Key, "true")
	mhttp.RegisterManagementVerifier(mhttp.VerifierSlotProxy, func(context.Context, string, string) error {
		return nil
	})
	t.Cleanup(func() {
		paramtable.Get().Reset(proxy.Params.CommonCfg.AdminAuthEnabled.Key)
		mhttp.RegisterManagementVerifier(mhttp.VerifierSlotProxy, nil)
	})

	crossSite := func(req *http.Request) {
		req.SetBasicAuth(util.UserRoot, "any-password")
		req.Header.Set("Sec-Fetch-Site", "cross-site")
	}
	for _, tc := range []struct{ name, method, path string }{
		{"data plane", http.MethodDelete, "/collection"},
		{"console API", http.MethodGet, mhttp.ClusterConfigsPath},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := requestMetricsPort(t, tc.method, tc.path, crossSite)
			assert.Equal(t, http.StatusForbidden, w.Code)
			assert.Contains(t, w.Body.String(), "cross-site")
		})
	}

	// The probe stays open, cross-site or not: a load balancer health check is
	// not something a browser replays credentials at.
	w := requestMetricsPort(t, http.MethodGet, "/health", crossSite)
	assert.Equal(t, http.StatusOK, w.Code)
}

// The Register guard in internal/http cannot see this gin tree, so nothing else
// stops a route being mounted on the engine instead of the group — where no
// auth middleware would apply to it. Every route the real assembly produces
// must sit under the prefix the middleware is installed on, and every one of
// them must actually answer 401 to an anonymous caller once the gate is on.
func TestEveryMetricsPortRouteIsGated(t *testing.T) {
	paramtable.Get().Save(proxy.Params.CommonCfg.AdminAuthEnabled.Key, "true")
	t.Cleanup(func() { paramtable.Get().Reset(proxy.Params.CommonCfg.AdminAuthEnabled.Key) })

	engine := metricsPortEngine(t)
	routes := engine.Routes()
	require.NotEmpty(t, routes)

	// The exemption set is asserted, not consulted: using it as the test's own
	// judge would mean widening it silently widens the test too.
	assert.Equal(t, map[string]struct{}{apiPathPrefix + "/health": {}}, openMetricsPortPaths)

	gated := 0
	for _, route := range routes {
		assert.True(t, strings.HasPrefix(route.Path, apiPathPrefix+"/"),
			"%s %s is served on the metrics port outside the authenticated group",
			route.Method, route.Path)
		if route.Path == apiPathPrefix+"/health" {
			continue
		}
		// Parameterised routes need a concrete value to match; ":clientId"
		// would otherwise 404 and quietly prove nothing.
		concrete := paramSegment.ReplaceAllString(route.Path, "/x")
		w := httptest.NewRecorder()
		engine.ServeHTTP(w, httptest.NewRequest(route.Method, concrete, nil))
		assert.Equal(t, http.StatusUnauthorized, w.Code,
			"%s %s answered an anonymous caller while the gate is on", route.Method, concrete)
		gated++
	}
	assert.Greater(t, gated, len(consoleAPIPaths)+len(telemetryAPIPaths),
		"the console and telemetry surfaces must be among the routes checked")
}

var paramSegment = regexp.MustCompile(`/:[^/]+`)

func Test_Service_GracefulStop(t *testing.T) {
	var count int32

	server := getServer(t)
	assert.NotNil(t, server)

	mockProxy := server.proxy.(*mocks.MockProxy)
	mockProxy.ExpectedCalls = nil
	mockProxy.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Run(func(_a0 context.Context, _a1 *milvuspb.GetComponentStatesRequest) {
		fmt.Println("rpc start")
		time.Sleep(3 * time.Second)
		atomic.AddInt32(&count, 1)
		fmt.Println("rpc done")
	}).Return(&milvuspb.ComponentStates{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}}, nil)

	mockProxy.EXPECT().Init().Return(nil)
	mockProxy.EXPECT().Start().Return(nil)
	mockProxy.EXPECT().Stop().Return(nil)
	mockProxy.EXPECT().Register().Return(nil)
	mockProxy.EXPECT().GetRateLimiter().Return(nil, nil)
	mockProxy.EXPECT().SetMixCoordClient(mock.Anything).Return()
	mockProxy.EXPECT().UpdateStateCode(mock.Anything).Return()
	mockProxy.EXPECT().SetAddress(mock.Anything).Return()

	Params := &paramtable.Get().ProxyGrpcServerCfg

	paramtable.Get().Save(Params.TLSMode.Key, "0")
	paramtable.Get().Save(Params.Port.Key, fmt.Sprintf("%d", funcutil.GetAvailablePort()))
	paramtable.Get().Save(Params.InternalPort.Key, fmt.Sprintf("%d", funcutil.GetAvailablePort()))
	paramtable.Get().Save(Params.ServerPemPath.Key, "../../../configs/cert/server.pem")
	paramtable.Get().Save(Params.ServerKeyPath.Key, "../../../configs/cert/server.key")
	paramtable.Get().Save(proxy.Params.HTTPCfg.Enabled.Key, "true")
	paramtable.Get().Save(proxy.Params.HTTPCfg.Port.Key, "")

	ctx := context.Background()
	enableCustomInterceptor = false
	enableRegisterProxyServer = true
	defer func() {
		enableCustomInterceptor = true
		enableRegisterProxyServer = false
	}()

	err := server.Prepare()
	assert.Nil(t, err)
	err = server.Run()
	assert.Nil(t, err)

	proxyClient, err := grpcproxyclient.NewClient(ctx, fmt.Sprintf("localhost:%s", Params.Port.GetValue()), 0)
	assert.Nil(t, err)

	group := &errgroup.Group{}
	for i := 0; i < 3; i++ {
		group.Go(func() error {
			_, err := proxyClient.GetComponentStates(context.TODO(), &milvuspb.GetComponentStatesRequest{})
			return err
		})
	}

	// waiting for all requests have been launched
	time.Sleep(1 * time.Second)

	server.Stop()

	err = group.Wait()
	assert.Nil(t, err)
	assert.Equal(t, count, int32(3))
}

func TestServer_RefreshExternalCollection(t *testing.T) {
	mockProxy := mocks.NewMockProxy(t)
	server := &Server{proxy: mockProxy}

	ctx := context.Background()
	req := &milvuspb.RefreshExternalCollectionRequest{
		CollectionName: "test_external",
	}

	m := mockey.Mock(mockey.GetMethod(mockProxy, "RefreshExternalCollection")).Return(&milvuspb.RefreshExternalCollectionResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		JobId:  12345,
	}, nil).Build()
	defer m.UnPatch()

	resp, err := server.RefreshExternalCollection(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, int64(12345), resp.GetJobId())
	assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
}

func TestServer_GetRefreshExternalCollectionProgress(t *testing.T) {
	mockProxy := mocks.NewMockProxy(t)
	server := &Server{proxy: mockProxy}

	ctx := context.Background()
	req := &milvuspb.GetRefreshExternalCollectionProgressRequest{
		JobId: 12345,
	}

	m := mockey.Mock(mockey.GetMethod(mockProxy, "GetRefreshExternalCollectionProgress")).Return(&milvuspb.GetRefreshExternalCollectionProgressResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
	}, nil).Build()
	defer m.UnPatch()

	resp, err := server.GetRefreshExternalCollectionProgress(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
}

func TestServer_ListRefreshExternalCollectionJobs(t *testing.T) {
	mockProxy := mocks.NewMockProxy(t)
	server := &Server{proxy: mockProxy}

	ctx := context.Background()
	req := &milvuspb.ListRefreshExternalCollectionJobsRequest{}

	m := mockey.Mock(mockey.GetMethod(mockProxy, "ListRefreshExternalCollectionJobs")).Return(&milvuspb.ListRefreshExternalCollectionJobsResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
	}, nil).Build()
	defer m.UnPatch()

	resp, err := server.ListRefreshExternalCollectionJobs(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
}
