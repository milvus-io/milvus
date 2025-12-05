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
	"net/http/httptest"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/federpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	grpcproxyclient "github.com/milvus-io/milvus/internal/distributed/proxy/client"
	"github.com/milvus-io/milvus/internal/distributed/proxy/httpserver"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	milvusmock "github.com/milvus-io/milvus/internal/util/mock"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/uniquegenerator"
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
			log.Error("grpc service not ready",
				zap.Error(err),
				zap.Any("option", opt))
			panic(err)
		}
	case <-timer.C:
		log.Error("grpc service not ready",
			zap.Any("option", opt))
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

	t.Run("Run", func(t *testing.T) {
		mockey.Mock((*proxy.Proxy).Init).Return(nil).Build()
		mockey.Mock((*proxy.Proxy).Start).Return(nil).Build()
		mockey.Mock((*proxy.Proxy).Register).Return(nil).Build()
		mockey.Mock((*proxy.Proxy).GetRateLimiter).Return(nil, nil).Build()
		mockey.Mock((*proxy.Proxy).SetMixCoordClient).Return().Build()
		mockey.Mock((*proxy.Proxy).UpdateStateCode).Return().Build()
		mockey.Mock((*proxy.Proxy).SetAddress).Return().Build()
		mockey.Mock((*proxy.Proxy).Stop).Return(nil).Build()
		defer mockey.UnPatchAll()
		err := runAndWaitForServerReady(server)
		assert.NoError(t, err)

		err = server.Stop()
		assert.NoError(t, err)
	})

	t.Run("GetComponentStates", func(t *testing.T) {
		_, err := server.GetComponentStates(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("GetStatisticsChannel", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).GetStatisticsChannel).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.GetStatisticsChannel(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("InvalidateCollectionMetaCache", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).InvalidateCollectionMetaCache).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.InvalidateCollectionMetaCache(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("InvalidateShardLeaderCache", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).InvalidateShardLeaderCache).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.InvalidateShardLeaderCache(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("CreateCollection", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).CreateCollection).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.CreateCollection(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("DropCollection", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).DropCollection).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.DropCollection(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("HasCollection", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).HasCollection).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.HasCollection(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("LoadCollection", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).LoadCollection).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.LoadCollection(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("ReleaseCollection", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).ReleaseCollection).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.ReleaseCollection(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("DescribeCollection", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).DescribeCollection).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.DescribeCollection(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("BatchDescribeCollection", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).BatchDescribeCollection).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.BatchDescribeCollection(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("GetCollectionStatistics", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).GetCollectionStatistics).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.GetCollectionStatistics(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("ShowCollections", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).ShowCollections).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.ShowCollections(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("CreatePartition", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).CreatePartition).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.CreatePartition(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("DropPartition", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).DropPartition).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.DropPartition(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("HasPartition", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).HasPartition).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.HasPartition(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("LoadPartitions", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).LoadPartitions).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.LoadPartitions(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("ReleasePartitions", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).ReleasePartitions).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.ReleasePartitions(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("GetPartitionStatistics", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).GetPartitionStatistics).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.GetPartitionStatistics(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("ShowPartitions", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).ShowPartitions).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.ShowPartitions(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("GetLoadingProgress", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).GetLoadingProgress).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.GetLoadingProgress(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("CreateIndex", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).CreateIndex).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.CreateIndex(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("DropIndex", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).DropIndex).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.DropIndex(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("DescribeIndex", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).DescribeIndex).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.DescribeIndex(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("GetIndexStatistics", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).GetIndexStatistics).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.GetIndexStatistics(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("GetIndexBuildProgress", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).GetIndexBuildProgress).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.GetIndexBuildProgress(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("GetIndexState", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).GetIndexState).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.GetIndexState(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("Insert", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).Insert).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.Insert(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("Delete", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).Delete).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.Delete(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("Upsert", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).Upsert).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.Upsert(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("Search", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).Search).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.Search(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("Flush", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).Flush).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.Flush(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("Query", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).Query).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.Query(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("CalcDistance", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).CalcDistance).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.CalcDistance(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("GetDdChannel", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).GetDdChannel).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.GetDdChannel(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("GetPersistentSegmentInfo", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).GetPersistentSegmentInfo).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.GetPersistentSegmentInfo(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("GetQuerySegmentInfo", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).GetQuerySegmentInfo).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.GetQuerySegmentInfo(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("Dummy", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).Dummy).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.Dummy(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("RegisterLink", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).RegisterLink).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.RegisterLink(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("GetMetrics", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).GetMetrics).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.GetMetrics(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("LoadBalance", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).LoadBalance).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.LoadBalance(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("CreateAlias", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).CreateAlias).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.CreateAlias(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("DropAlias", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).DropAlias).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.DropAlias(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("AlterAlias", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).AlterAlias).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.AlterAlias(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("DescribeAlias", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).DescribeAlias).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.DescribeAlias(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("ListAliases", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).ListAliases).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.ListAliases(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("GetCompactionState", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).GetCompactionState).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.GetCompactionState(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("ManualCompaction", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).ManualCompaction).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.ManualCompaction(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("GetCompactionStateWithPlans", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).GetCompactionStateWithPlans).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.GetCompactionStateWithPlans(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("CreateCredential", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).CreateCredential).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.CreateCredential(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("UpdateCredential", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).UpdateCredential).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.UpdateCredential(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("DeleteCredential", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).DeleteCredential).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.DeleteCredential(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("ListCredUsers", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).ListCredUsers).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.ListCredUsers(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("InvalidateCredentialCache", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).InvalidateCredentialCache).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.InvalidateCredentialCache(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("UpdateCredentialCache", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).UpdateCredentialCache).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.UpdateCredentialCache(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("CreateRole", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).CreateRole).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.CreateRole(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("DropRole", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).DropRole).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.DropRole(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("OperateUserRole", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).OperateUserRole).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.OperateUserRole(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("SelectRole", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).SelectRole).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.SelectRole(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("SelectUser", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).SelectUser).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.SelectUser(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("OperatePrivilege", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).OperatePrivilege).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.OperatePrivilege(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("SelectGrant", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).SelectGrant).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.SelectGrant(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("RefreshPrivilegeInfoCache", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).RefreshPolicyInfoCache).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.RefreshPolicyInfoCache(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("CheckHealth", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).CheckHealth).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.CheckHealth(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("RenameCollection", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).RenameCollection).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.RenameCollection(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("CreateResourceGroup", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).CreateResourceGroup).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.CreateResourceGroup(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("DropResourceGroup", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).DropResourceGroup).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.DropResourceGroup(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("TransferNode", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).TransferNode).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.TransferNode(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("TransferReplica", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).TransferReplica).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.TransferReplica(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("ListResourceGroups", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).ListResourceGroups).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.ListResourceGroups(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("DescribeResourceGroup", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).DescribeResourceGroup).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.DescribeResourceGroup(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("FlushAll", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).FlushAll).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.FlushAll(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("GetFlushAllState", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).GetFlushAllState).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.GetFlushAllState(ctx, nil)
		assert.NoError(t, err)
	})

	t.Run("CreateDatabase", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).CreateDatabase).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.CreateDatabase(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("DropDatabase", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).DropDatabase).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.DropDatabase(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("ListDatabase", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).ListDatabases).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.ListDatabases(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("AlterDatabase", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).AlterDatabase).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.AlterDatabase(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("DescribeDatabase", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).DescribeDatabase).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.DescribeDatabase(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("AllocTimestamp", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).AllocTimestamp).Return(nil, nil).Build()
		defer m.UnPatch()
		_, err := server.AllocTimestamp(ctx, nil)
		assert.Nil(t, err)
	})

	t.Run("RunAnalyzer", func(t *testing.T) {
		m := mockey.Mock((*proxy.Proxy).RunAnalyzer).Return(&milvuspb.RunAnalyzerResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		}, nil).Build()
		defer m.UnPatch()
		_, err := server.RunAnalyzer(ctx, &milvuspb.RunAnalyzerRequest{})
		assert.NoError(t, err)
	})

	t.Run("Run with different config", func(t *testing.T) {
		mockey.Mock((*proxy.Proxy).Init).Return(nil).Build()
		mockey.Mock((*proxy.Proxy).Start).Return(nil).Build()
		mockey.Mock((*proxy.Proxy).Register).Return(nil).Build()
		mockey.Mock((*proxy.Proxy).GetRateLimiter).Return(nil, nil).Build()
		mockey.Mock((*proxy.Proxy).SetMixCoordClient).Return().Build()
		mockey.Mock((*proxy.Proxy).UpdateStateCode).Return().Build()
		mockey.Mock((*proxy.Proxy).SetAddress).Return().Build()
		mockey.Mock((*proxy.Proxy).Stop).Return(nil).Build()
		defer mockey.UnPatchAll()
		// Update config and start server again to test with different config set.
		// This works as config will be initialized only once
		paramtable.Get().Save(proxy.Params.ProxyCfg.GinLogging.Key, "false")
		err := runAndWaitForServerReady(server)
		assert.NoError(t, err)

		err = server.Stop()
		assert.NoError(t, err)
	})
}

func TestServer_Check(t *testing.T) {
	ctx := context.Background()
	server := getServer(t)

	// Mock proxy.GetComponentStates to return Healthy state for the first test case
	mockGetStates := mockey.Mock((*proxy.Proxy).GetComponentStates).Return(&milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			StateCode: commonpb.StateCode_Healthy,
		},
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
	}, nil).Build()

	req := &grpc_health_v1.HealthCheckRequest{Service: ""}
	ret, err := server.Check(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, ret.Status)
	mockGetStates.UnPatch()

	// Test error case
	mockey.UnPatchAll()
	m := mockey.Mock((*proxy.Proxy).GetComponentStates).Return(nil, errors.New("mock grpc unexpected error")).Build()

	ret, err = server.Check(ctx, req)
	assert.Error(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, ret.Status)
	m.UnPatch()

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

	m = mockey.Mock((*proxy.Proxy).GetComponentStates).Return(componentState, nil).Build()
	defer m.UnPatch()

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

	// Mock proxy.GetComponentStates to return Healthy state for the first test case
	mockGetStates := mockey.Mock((*proxy.Proxy).GetComponentStates).Return(&milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			StateCode: commonpb.StateCode_Healthy,
		},
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
	}, nil).Build()

	watchServer := milvusmock.NewGrpcHealthWatchServer()
	resultChan := watchServer.Chan()
	req := &grpc_health_v1.HealthCheckRequest{Service: ""}
	// var ret *grpc_health_v1.HealthCheckResponse
	err := server.Watch(req, watchServer)
	ret := <-resultChan

	assert.NoError(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, ret.Status)
	mockGetStates.UnPatch()

	// Test error case
	mockey.UnPatchAll()
	m := mockey.Mock((*proxy.Proxy).GetComponentStates).Return(nil, errors.New("mock grpc unexpected error")).Build()

	err = server.Watch(req, watchServer)
	ret = <-resultChan
	assert.NoError(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, ret.Status)
	m.UnPatch()

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
	m = mockey.Mock((*proxy.Proxy).GetComponentStates).Return(componentState, nil).Build()
	defer m.UnPatch()

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

	mockey.Mock((*proxy.Proxy).Stop).Return(nil).Build()
	mockey.Mock((*proxy.Proxy).Init).Return(nil).Build()
	mockey.Mock((*proxy.Proxy).Start).Return(nil).Build()
	mockey.Mock((*proxy.Proxy).Register).Return(nil).Build()
	mockey.Mock((*proxy.Proxy).GetRateLimiter).Return(nil, nil).Build()
	mockey.Mock((*proxy.Proxy).SetMixCoordClient).Return().Build()
	mockey.Mock((*proxy.Proxy).UpdateStateCode).Return().Build()
	mockey.Mock((*proxy.Proxy).SetAddress).Return().Build()
	defer mockey.UnPatchAll()

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

func getServer(t *testing.T) *Server {
	ctx := context.Background()
	server, err := NewServer(ctx, nil)
	assert.NotNil(t, server)
	assert.NoError(t, err)

	mockProxy := &proxy.Proxy{}
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

func Test_NewServer_TLS_TwoWay(t *testing.T) {
	server := getServer(t)
	Params := &paramtable.Get().ProxyGrpcServerCfg

	mockey.Mock((*proxy.Proxy).Stop).Return(nil).Build()
	mockey.Mock((*proxy.Proxy).Init).Return(nil).Build()
	mockey.Mock((*proxy.Proxy).Start).Return(nil).Build()
	mockey.Mock((*proxy.Proxy).Register).Return(nil).Build()
	mockey.Mock((*proxy.Proxy).GetRateLimiter).Return(nil, nil).Build()
	mockey.Mock((*proxy.Proxy).SetMixCoordClient).Return().Build()
	mockey.Mock((*proxy.Proxy).UpdateStateCode).Return().Build()
	mockey.Mock((*proxy.Proxy).SetAddress).Return().Build()
	defer mockey.UnPatchAll()

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

	mockey.Mock((*proxy.Proxy).Stop).Return(nil).Build()
	mockey.Mock((*proxy.Proxy).Init).Return(nil).Build()
	mockey.Mock((*proxy.Proxy).Start).Return(nil).Build()
	mockey.Mock((*proxy.Proxy).Register).Return(nil).Build()
	mockey.Mock((*proxy.Proxy).GetRateLimiter).Return(nil, nil).Build()
	mockey.Mock((*proxy.Proxy).SetMixCoordClient).Return().Build()
	mockey.Mock((*proxy.Proxy).UpdateStateCode).Return().Build()
	mockey.Mock((*proxy.Proxy).SetAddress).Return().Build()
	defer mockey.UnPatchAll()

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

	mockey.Mock((*proxy.Proxy).Stop).Return(nil).Build()
	mockey.Mock((*proxy.Proxy).GetRateLimiter).Return(nil, nil).Build()
	mockey.Mock((*proxy.Proxy).SetAddress).Return().Build()
	defer mockey.UnPatchAll()

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

func Test_NewHTTPServer_TLS_TwoWay(t *testing.T) {
	server := getServer(t)

	mockey.Mock((*proxy.Proxy).Stop).Return(nil).Build()
	mockey.Mock((*proxy.Proxy).Init).Return(nil).Build()
	mockey.Mock((*proxy.Proxy).Start).Return(nil).Build()
	mockey.Mock((*proxy.Proxy).Register).Return(nil).Build()
	mockey.Mock((*proxy.Proxy).GetRateLimiter).Return(nil, nil).Build()
	mockey.Mock((*proxy.Proxy).SetMixCoordClient).Return().Build()
	mockey.Mock((*proxy.Proxy).UpdateStateCode).Return().Build()
	mockey.Mock((*proxy.Proxy).SetAddress).Return().Build()
	defer mockey.UnPatchAll()

	Params := &paramtable.Get().ProxyGrpcServerCfg

	paramtable.Get().Save(Params.TLSMode.Key, "2")
	paramtable.Get().Save(Params.ServerPemPath.Key, "../../../configs/cert/server.pem")
	paramtable.Get().Save(Params.ServerKeyPath.Key, "../../../configs/cert/server.key")
	paramtable.Get().Save(Params.CaPemPath.Key, "../../../configs/cert/ca.pem")
	paramtable.Get().Save(proxy.Params.HTTPCfg.Enabled.Key, "true")
	paramtable.Get().Save(proxy.Params.HTTPCfg.Port.Key, "8080")

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

	mockey.Mock((*proxy.Proxy).Stop).Return(nil).Build()
	mockey.Mock((*proxy.Proxy).Init).Return(nil).Build()
	mockey.Mock((*proxy.Proxy).Start).Return(nil).Build()
	mockey.Mock((*proxy.Proxy).Register).Return(nil).Build()
	mockey.Mock((*proxy.Proxy).GetRateLimiter).Return(nil, nil).Build()
	mockey.Mock((*proxy.Proxy).SetMixCoordClient).Return().Build()
	mockey.Mock((*proxy.Proxy).UpdateStateCode).Return().Build()
	mockey.Mock((*proxy.Proxy).SetAddress).Return().Build()
	defer mockey.UnPatchAll()

	Params := &paramtable.Get().ProxyGrpcServerCfg

	paramtable.Get().Save(Params.TLSMode.Key, "1")
	paramtable.Get().Save(Params.ServerPemPath.Key, "../../../configs/cert/server.pem")
	paramtable.Get().Save(Params.ServerKeyPath.Key, "../../../configs/cert/server.key")
	paramtable.Get().Save(proxy.Params.HTTPCfg.Enabled.Key, "true")
	paramtable.Get().Save(proxy.Params.HTTPCfg.Port.Key, "8080")

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

func Test_NewHTTPServer_TLS_FileNotExisted(t *testing.T) {
	server := getServer(t)

	mockey.Mock((*proxy.Proxy).Stop).Return(nil).Build()
	mockey.Mock((*proxy.Proxy).SetAddress).Return().Build()
	defer mockey.UnPatchAll()

	Params := &paramtable.Get().ProxyGrpcServerCfg

	paramtable.Get().Save(Params.TLSMode.Key, "1")
	paramtable.Get().Save(Params.ServerPemPath.Key, "../not/existed/server.pem")
	paramtable.Get().Save(Params.ServerKeyPath.Key, "../../../configs/cert/server.key")
	paramtable.Get().Save(proxy.Params.HTTPCfg.Enabled.Key, "true")
	paramtable.Get().Save(proxy.Params.HTTPCfg.Port.Key, "8080")
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

func Test_Service_GracefulStop(t *testing.T) {
	var count int32

	server := getServer(t)
	assert.NotNil(t, server)

	mockey.UnPatchAll()
	mockey.Mock((*proxy.Proxy).GetComponentStates).To(func(_ *proxy.Proxy, _ context.Context, _ *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
		fmt.Println("rpc start")
		time.Sleep(3 * time.Second)
		atomic.AddInt32(&count, 1)
		fmt.Println("rpc done")
		return &milvuspb.ComponentStates{Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}}, nil
	}).Build()

	mockey.Mock((*proxy.Proxy).Init).Return(nil).Build()
	mockey.Mock((*proxy.Proxy).Start).Return(nil).Build()
	mockey.Mock((*proxy.Proxy).Stop).Return(nil).Build()
	mockey.Mock((*proxy.Proxy).Register).Return(nil).Build()
	mockey.Mock((*proxy.Proxy).GetRateLimiter).Return(nil, nil).Build()
	mockey.Mock((*proxy.Proxy).SetMixCoordClient).Return().Build()
	mockey.Mock((*proxy.Proxy).UpdateStateCode).Return().Build()
	mockey.Mock((*proxy.Proxy).SetAddress).Return().Build()
	defer mockey.UnPatchAll()

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
