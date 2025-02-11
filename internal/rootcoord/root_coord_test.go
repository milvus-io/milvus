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

package rootcoord

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/internal/util/dependency"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/internal/util/healthcheck"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tikv"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	rand.Seed(time.Now().UnixNano())
	parameters := []string{"tikv", "etcd"}
	var code int
	for _, v := range parameters {
		paramtable.Get().Save(paramtable.Get().MetaStoreCfg.MetaStoreType.Key, v)
		code = m.Run()
	}
	os.Exit(code)
}

func TestRootCoord_CreateDatabase(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())
		ctx := context.Background()
		resp, err := c.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_DropDatabase(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.DropDatabase(ctx, &milvuspb.DropDatabaseRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.DropDatabase(ctx, &milvuspb.DropDatabaseRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.DropDatabase(ctx, &milvuspb.DropDatabaseRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())
		ctx := context.Background()
		resp, err := c.DropDatabase(ctx, &milvuspb.DropDatabaseRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_ListDatabases(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())
		ctx := context.Background()
		resp, err := c.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestRootCoord_AlterDatabase(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.AlterDatabase(ctx, &rootcoordpb.AlterDatabaseRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.AlterDatabase(ctx, &rootcoordpb.AlterDatabaseRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.AlterDatabase(ctx, &rootcoordpb.AlterDatabaseRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())
		ctx := context.Background()
		resp, err := c.AlterDatabase(ctx, &rootcoordpb.AlterDatabaseRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_CreateCollection(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())

		ctx := context.Background()
		resp, err := c.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_DropCollection(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.DropCollection(ctx, &milvuspb.DropCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.DropCollection(ctx, &milvuspb.DropCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.DropCollection(ctx, &milvuspb.DropCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())

		ctx := context.Background()
		resp, err := c.DropCollection(ctx, &milvuspb.DropCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_CreatePartition(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())

		ctx := context.Background()
		resp, err := c.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_DropPartition(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.DropPartition(ctx, &milvuspb.DropPartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.DropPartition(ctx, &milvuspb.DropPartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.DropPartition(ctx, &milvuspb.DropPartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())

		ctx := context.Background()
		resp, err := c.DropPartition(ctx, &milvuspb.DropPartitionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_CreateAlias(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.CreateAlias(ctx, &milvuspb.CreateAliasRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.CreateAlias(ctx, &milvuspb.CreateAliasRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())
		ctx := context.Background()
		resp, err := c.CreateAlias(ctx, &milvuspb.CreateAliasRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())
		ctx := context.Background()
		resp, err := c.CreateAlias(ctx, &milvuspb.CreateAliasRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_DropAlias(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.DropAlias(ctx, &milvuspb.DropAliasRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.DropAlias(ctx, &milvuspb.DropAliasRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())
		ctx := context.Background()
		resp, err := c.DropAlias(ctx, &milvuspb.DropAliasRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())
		ctx := context.Background()
		resp, err := c.DropAlias(ctx, &milvuspb.DropAliasRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_AlterAlias(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.AlterAlias(ctx, &milvuspb.AlterAliasRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.AlterAlias(ctx, &milvuspb.AlterAliasRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())
		ctx := context.Background()
		resp, err := c.AlterAlias(ctx, &milvuspb.AlterAliasRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())
		ctx := context.Background()
		resp, err := c.AlterAlias(ctx, &milvuspb.AlterAliasRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_DescribeAlias(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.DescribeAlias(ctx, &milvuspb.DescribeAliasRequest{Alias: "test"})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler(),
			withInvalidMeta())
		ctx := context.Background()
		resp, err := c.DescribeAlias(ctx, &milvuspb.DescribeAliasRequest{Alias: "test"})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler(),
			withInvalidMeta())
		ctx := context.Background()
		resp, err := c.DescribeAlias(ctx, &milvuspb.DescribeAliasRequest{Alias: "test"})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("input alias is empty", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())
		meta := newMockMetaTable()
		meta.DescribeAliasFunc = func(ctx context.Context, dbName, alias string, ts Timestamp) (string, error) {
			return "", nil
		}
		c.meta = meta
		ctx := context.Background()
		resp, err := c.DescribeAlias(ctx, &milvuspb.DescribeAliasRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
		assert.Equal(t, int32(1101), resp.GetStatus().GetCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())
		meta := newMockMetaTable()
		meta.DescribeAliasFunc = func(ctx context.Context, dbName, alias string, ts Timestamp) (string, error) {
			return "", nil
		}
		c.meta = meta
		ctx := context.Background()
		resp, err := c.DescribeAlias(ctx, &milvuspb.DescribeAliasRequest{Alias: "test"})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestRootCoord_ListAliases(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.ListAliases(ctx, &milvuspb.ListAliasesRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler(),
			withInvalidMeta())
		ctx := context.Background()
		resp, err := c.ListAliases(ctx, &milvuspb.ListAliasesRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler(),
			withInvalidMeta())
		ctx := context.Background()
		resp, err := c.ListAliases(ctx, &milvuspb.ListAliasesRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())
		meta := newMockMetaTable()
		meta.ListAliasesFunc = func(ctx context.Context, dbName, collectionName string, ts Timestamp) ([]string, error) {
			return nil, nil
		}
		c.meta = meta
		ctx := context.Background()
		resp, err := c.ListAliases(ctx, &milvuspb.ListAliasesRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestRootCoord_DescribeCollection(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		resp, err = c.DescribeCollectionInternal(ctx, &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		resp, err = c.DescribeCollectionInternal(ctx, &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		resp, err = c.DescribeCollectionInternal(ctx, &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())

		ctx := context.Background()
		resp, err := c.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		resp, err = c.DescribeCollectionInternal(ctx, &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestRootCoord_HasCollection(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.HasCollection(ctx, &milvuspb.HasCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.HasCollection(ctx, &milvuspb.HasCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.HasCollection(ctx, &milvuspb.HasCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())

		ctx := context.Background()
		resp, err := c.HasCollection(ctx, &milvuspb.HasCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestRootCoord_ShowCollections(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())

		ctx := context.Background()
		resp, err := c.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestRootCoord_ShowCollectionIDs(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.ShowCollectionIDs(ctx, &rootcoordpb.ShowCollectionIDsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("test failed", func(t *testing.T) {
		c := newTestCore(withHealthyCode())
		meta := mockrootcoord.NewIMetaTable(t)
		c.meta = meta

		ctx := context.Background()

		// specify db names
		meta.EXPECT().GetDatabaseByName(mock.Anything, mock.Anything, typeutil.MaxTimestamp).Return(nil, fmt.Errorf("mock err"))
		resp, err := c.ShowCollectionIDs(ctx, &rootcoordpb.ShowCollectionIDsRequest{
			DbNames:          []string{"db1"},
			AllowUnavailable: true,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())

		// not specify db names
		meta.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(nil, fmt.Errorf("mock err"))
		resp, err = c.ShowCollectionIDs(ctx, &rootcoordpb.ShowCollectionIDsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())

		// list collections failed
		meta.ExpectedCalls = nil
		meta.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(
			[]*model.Database{model.NewDatabase(rand.Int63(), "db1", etcdpb.DatabaseState_DatabaseCreated, nil)}, nil)
		meta.EXPECT().ListCollections(mock.Anything, mock.Anything, typeutil.MaxTimestamp, false).Return(nil, fmt.Errorf("mock err"))
		resp, err = c.ShowCollectionIDs(ctx, &rootcoordpb.ShowCollectionIDsRequest{
			AllowUnavailable: true,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode())
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().ListCollections(mock.Anything, mock.Anything, typeutil.MaxTimestamp, false).Return([]*model.Collection{}, nil)
		c.meta = meta

		ctx := context.Background()

		// specify db names
		meta.EXPECT().GetDatabaseByName(mock.Anything, mock.Anything, typeutil.MaxTimestamp).Return(
			model.NewDatabase(rand.Int63(), "db1", etcdpb.DatabaseState_DatabaseCreated, nil), nil)
		resp, err := c.ShowCollectionIDs(ctx, &rootcoordpb.ShowCollectionIDsRequest{
			DbNames:          []string{"db1"},
			AllowUnavailable: true,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())

		// not specify db names
		meta.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(
			[]*model.Database{model.NewDatabase(rand.Int63(), "db1", etcdpb.DatabaseState_DatabaseCreated, nil)}, nil)
		resp, err = c.ShowCollectionIDs(ctx, &rootcoordpb.ShowCollectionIDsRequest{
			AllowUnavailable: true,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestRootCoord_HasPartition(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.HasPartition(ctx, &milvuspb.HasPartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.HasPartition(ctx, &milvuspb.HasPartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.HasPartition(ctx, &milvuspb.HasPartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())

		ctx := context.Background()
		resp, err := c.HasPartition(ctx, &milvuspb.HasPartitionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestRootCoord_ShowPartitions(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		c := newTestCore(withAbnormalCode())
		ctx := context.Background()
		resp, err := c.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		resp, err = c.ShowPartitionsInternal(ctx, &milvuspb.ShowPartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to add task", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		resp, err = c.ShowPartitionsInternal(ctx, &milvuspb.ShowPartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to execute", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())
		ctx := context.Background()
		resp, err := c.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		resp, err = c.ShowPartitionsInternal(ctx, &milvuspb.ShowPartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case, everything is ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())
		ctx := context.Background()
		resp, err := c.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		resp, err = c.ShowPartitionsInternal(ctx, &milvuspb.ShowPartitionsRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestRootCoord_AllocTimestamp(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.AllocTimestamp(ctx, &rootcoordpb.AllocTimestampRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to allocate ts", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withInvalidTsoAllocator())
		resp, err := c.AllocTimestamp(ctx, &rootcoordpb.AllocTimestampRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case", func(t *testing.T) {
		alloc := newMockTsoAllocator()
		count := uint32(10)
		ts := Timestamp(100)
		alloc.GenerateTSOF = func(count uint32) (uint64, error) {
			// end ts
			return ts, nil
		}
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withTsoAllocator(alloc))
		resp, err := c.AllocTimestamp(ctx, &rootcoordpb.AllocTimestampRequest{Count: count})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		// begin ts
		assert.Equal(t, ts-uint64(count)+1, resp.GetTimestamp())
		assert.Equal(t, count, resp.GetCount())
	})
}

func TestRootCoord_AllocID(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.AllocID(ctx, &rootcoordpb.AllocIDRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to allocate id", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withInvalidIDAllocator())
		resp, err := c.AllocID(ctx, &rootcoordpb.AllocIDRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case", func(t *testing.T) {
		alloc := newMockIDAllocator()
		id := UniqueID(100)
		alloc.AllocF = func(count uint32) (UniqueID, UniqueID, error) {
			return id, id + int64(count), nil
		}
		count := uint32(10)
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withIDAllocator(alloc))
		resp, err := c.AllocID(ctx, &rootcoordpb.AllocIDRequest{Count: count})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.Equal(t, id, resp.GetID())
		assert.Equal(t, count, resp.GetCount())
	})
}

func TestRootCoord_UpdateChannelTimeTick(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.UpdateChannelTimeTick(ctx, &internalpb.ChannelTimeTickMsg{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("invalid msg type", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withHealthyCode())
		resp, err := c.UpdateChannelTimeTick(ctx, &internalpb.ChannelTimeTickMsg{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection}})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("invalid msg", func(t *testing.T) {
		defer cleanTestEnv()

		ticker := newRocksMqTtSynchronizer()

		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withTtSynchronizer(ticker))

		// the length of channel names & timestamps mismatch.
		resp, err := c.UpdateChannelTimeTick(ctx, &internalpb.ChannelTimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_TimeTick,
			},
			ChannelNames: []string{funcutil.GenRandomStr()},
			Timestamps:   []uint64{},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("normal case", func(t *testing.T) {
		defer cleanTestEnv()

		source := int64(20220824)
		ts := Timestamp(100)
		defaultTs := Timestamp(101)

		ticker := newRocksMqTtSynchronizer()
		ticker.addSession(&sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: source}})

		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withTtSynchronizer(ticker))

		resp, err := c.UpdateChannelTimeTick(ctx, &internalpb.ChannelTimeTickMsg{
			Base: &commonpb.MsgBase{
				SourceID: source,
				MsgType:  commonpb.MsgType_TimeTick,
			},
			ChannelNames:     []string{funcutil.GenRandomStr()},
			Timestamps:       []uint64{ts},
			DefaultTimestamp: defaultTs,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_InvalidateCollectionMetaCache(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("failed to invalidate cache", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withInvalidProxyManager())
		resp, err := c.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("normal case", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withValidProxyManager())
		resp, err := c.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_RenameCollection(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("add task failed", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("execute task failed", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("run ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())

		ctx := context.Background()
		resp, err := c.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_ListPolicy(t *testing.T) {
	t.Run("expand privilege groups", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		c := newTestCore(withHealthyCode(), withMeta(meta))
		ctx := context.Background()

		meta.EXPECT().ListPolicy(util.DefaultTenant).Return([]*milvuspb.GrantEntity{
			{
				ObjectName: "*",
				Object: &milvuspb.ObjectEntity{
					Name: "Global",
				},
				Role:    &milvuspb.RoleEntity{Name: "role"},
				Grantor: &milvuspb.GrantorEntity{Privilege: &milvuspb.PrivilegeEntity{Name: "CollectionAdmin"}},
			},
		}, nil)

		meta.EXPECT().ListPrivilegeGroups().Return([]*milvuspb.PrivilegeGroupInfo{}, nil)

		meta.EXPECT().ListUserRole(util.DefaultTenant).Return([]string{}, nil)

		resp, err := c.ListPolicy(ctx, &internalpb.ListPolicyRequest{})
		assert.Equal(t, len(Params.RbacConfig.GetDefaultPrivilegeGroup("CollectionAdmin").Privileges), len(resp.PolicyInfos))
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})
}

func TestRootCoord_ShowConfigurations(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.ShowConfigurations(ctx, &internalpb.ShowConfigurationsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case", func(t *testing.T) {
		paramtable.Init()

		pattern := "rootcoord.Port"
		req := &internalpb.ShowConfigurationsRequest{
			Base: &commonpb.MsgBase{
				MsgID: rand.Int63(),
			},
			Pattern: pattern,
		}

		ctx := context.Background()
		c := newTestCore(withHealthyCode())
		resp, err := c.ShowConfigurations(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		assert.Equal(t, 1, len(resp.GetConfiguations()))
		assert.Equal(t, "rootcoord.port", resp.GetConfiguations()[0].Key)
	})
}

func TestRootCoord_GetMetrics(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.GetMetrics(ctx, &milvuspb.GetMetricsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("failed to parse metric type", func(t *testing.T) {
		req := &milvuspb.GetMetricsRequest{
			Request: "invalid request",
		}
		ctx := context.Background()
		c := newTestCore(withHealthyCode())
		resp, err := c.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("unsupported metric type", func(t *testing.T) {
		// unsupported metric type
		unsupportedMetricType := "unsupported"
		req, err := metricsinfo.ConstructRequestByMetricType(unsupportedMetricType)
		assert.NoError(t, err)
		ctx := context.Background()
		c := newTestCore(withHealthyCode())
		resp, err := c.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("normal case", func(t *testing.T) {
		systemInfoMetricType := metricsinfo.SystemInfoMetrics
		req, err := metricsinfo.ConstructRequestByMetricType(systemInfoMetricType)
		assert.NoError(t, err)
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withMetricsCacheManager())
		resp, err := c.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("get system info metrics from cache", func(t *testing.T) {
		systemInfoMetricType := metricsinfo.SystemInfoMetrics
		req, err := metricsinfo.ConstructRequestByMetricType(systemInfoMetricType)
		assert.NoError(t, err)
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withMetricsCacheManager())
		c.metricsCacheManager.UpdateSystemInfoMetrics(&milvuspb.GetMetricsResponse{
			Status:        merr.Success(),
			Response:      "cached response",
			ComponentName: "cached component",
		})
		resp, err := c.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("get system info metrics, cache miss", func(t *testing.T) {
		systemInfoMetricType := metricsinfo.SystemInfoMetrics
		req, err := metricsinfo.ConstructRequestByMetricType(systemInfoMetricType)
		assert.NoError(t, err)
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withMetricsCacheManager())
		c.metricsCacheManager.InvalidateSystemInfoMetrics()
		resp, err := c.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("get system info metrics", func(t *testing.T) {
		systemInfoMetricType := metricsinfo.SystemInfoMetrics
		req, err := metricsinfo.ConstructRequestByMetricType(systemInfoMetricType)
		assert.NoError(t, err)
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withMetricsCacheManager())
		resp, err := c.getSystemInfoMetrics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}

func TestCore_Rbac(t *testing.T) {
	ctx := context.Background()
	c := &Core{
		ctx: ctx,
	}

	// not healthy.
	c.UpdateStateCode(commonpb.StateCode_Abnormal)

	{
		resp, err := c.CreateCredential(ctx, &internalpb.CredentialInfo{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.ErrorCode)
	}

	{
		resp, err := c.DeleteCredential(ctx, &milvuspb.DeleteCredentialRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.ErrorCode)
	}

	{
		resp, err := c.UpdateCredential(ctx, &internalpb.CredentialInfo{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.ErrorCode)
	}

	{
		resp, err := c.GetCredential(ctx, &rootcoordpb.GetCredentialRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.GetStatus().GetErrorCode())
	}

	{
		resp, err := c.ListCredUsers(ctx, &milvuspb.ListCredUsersRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_NotReadyServe, resp.GetStatus().GetErrorCode())
	}

	{
		resp, err := c.CreateRole(ctx, &milvuspb.CreateRoleRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	}

	{
		resp, err := c.DropRole(ctx, &milvuspb.DropRoleRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	}

	{
		resp, err := c.OperateUserRole(ctx, &milvuspb.OperateUserRoleRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	}

	{
		resp, err := c.SelectRole(ctx, &milvuspb.SelectRoleRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	}

	{
		resp, err := c.SelectUser(ctx, &milvuspb.SelectUserRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	}

	{
		resp, err := c.OperatePrivilege(ctx, &milvuspb.OperatePrivilegeRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	}

	{
		resp, err := c.SelectGrant(ctx, &milvuspb.SelectGrantRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	}

	{
		resp, err := c.ListPolicy(ctx, &internalpb.ListPolicyRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	}
}

func TestCore_sendMinDdlTsAsTt(t *testing.T) {
	ticker := newRocksMqTtSynchronizer()
	ddlManager := newMockDdlTsLockManager()
	ddlManager.GetMinDdlTsFunc = func() Timestamp {
		return 100
	}
	sched := newMockScheduler()
	sched.GetMinDdlTsFunc = func() Timestamp {
		return 100
	}
	c := newTestCore(
		withTtSynchronizer(ticker),
		withDdlTsLockManager(ddlManager),
		withScheduler(sched))

	c.UpdateStateCode(commonpb.StateCode_Healthy)
	c.session.ServerID = TestRootCoordID

	_ = paramtable.Get().Save(paramtable.Get().CommonCfg.TTMsgEnabled.Key, "false")
	c.sendMinDdlTsAsTt() // disable ts msg
	_ = paramtable.Get().Save(paramtable.Get().CommonCfg.TTMsgEnabled.Key, "true")

	c.sendMinDdlTsAsTt() // no session.
	ticker.addSession(&sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: TestRootCoordID}})
	c.sendMinDdlTsAsTt()
	sched.GetMinDdlTsFunc = func() Timestamp {
		return typeutil.ZeroTimestamp
	}
	c.sendMinDdlTsAsTt() // zero ts
	sched.GetMinDdlTsFunc = func() Timestamp {
		return typeutil.MaxTimestamp
	}
	ddlManager.GetMinDdlTsFunc = func() Timestamp {
		return typeutil.MaxTimestamp
	}
	c.sendMinDdlTsAsTt()
}

func TestCore_startTimeTickLoop(t *testing.T) {
	ticker := newRocksMqTtSynchronizer()
	ticker.addSession(&sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: TestRootCoordID}})
	ddlManager := newMockDdlTsLockManager()
	ddlManager.GetMinDdlTsFunc = func() Timestamp {
		return 100
	}
	sched := newMockScheduler()
	sched.GetMinDdlTsFunc = func() Timestamp {
		return 100
	}
	c := newTestCore(
		withTtSynchronizer(ticker),
		withDdlTsLockManager(ddlManager),
		withScheduler(sched))
	ctx, cancel := context.WithCancel(context.Background())
	c.ctx = ctx
	paramtable.Get().Save(Params.ProxyCfg.TimeTickInterval.Key, "1")
	c.wg.Add(1)
	c.UpdateStateCode(commonpb.StateCode_Initializing)
	go c.startTimeTickLoop()

	time.Sleep(time.Millisecond * 4)
	cancel()
	c.wg.Wait()
}

// make sure the main functions work well when EnableActiveStandby=true
func TestRootcoord_EnableActiveStandby(t *testing.T) {
	randVal := rand.Int()
	paramtable.Init()
	Params.Save("etcd.rootPath", fmt.Sprintf("/%d", randVal))
	// Need to reset global etcd to follow new path
	kvfactory.CloseEtcdClient()
	paramtable.Get().Save(Params.RootCoordCfg.EnableActiveStandby.Key, "true")
	defer paramtable.Get().Reset(Params.RootCoordCfg.EnableActiveStandby.Key)
	paramtable.Get().Save(Params.CommonCfg.RootCoordTimeTick.Key, fmt.Sprintf("rootcoord-time-tick-%d", randVal))
	defer paramtable.Get().Reset(Params.CommonCfg.RootCoordTimeTick.Key)
	paramtable.Get().Save(Params.CommonCfg.RootCoordStatistics.Key, fmt.Sprintf("rootcoord-statistics-%d", randVal))
	defer paramtable.Get().Reset(Params.CommonCfg.RootCoordStatistics.Key)
	paramtable.Get().Save(Params.CommonCfg.RootCoordDml.Key, fmt.Sprintf("rootcoord-dml-test-%d", randVal))
	defer paramtable.Get().Reset(Params.CommonCfg.RootCoordDml.Key)

	ctx := context.Background()
	coreFactory := dependency.NewDefaultFactory(true)
	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	assert.NoError(t, err)
	defer etcdCli.Close()
	core, err := NewCore(ctx, coreFactory)
	core.etcdCli = etcdCli
	assert.NoError(t, err)
	core.SetTiKVClient(tikv.SetupLocalTxn())

	err = core.Init()
	assert.NoError(t, err)
	assert.Equal(t, commonpb.StateCode_StandBy, core.GetStateCode())
	core.session.TriggerKill = false
	err = core.Register()
	assert.NoError(t, err)
	err = core.Start()
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		return core.GetStateCode() == commonpb.StateCode_Healthy
	}, time.Second*5, time.Millisecond*200)
	resp, err := core.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DescribeCollection,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  paramtable.GetNodeID(),
		},
		CollectionName: "unexist",
	})
	assert.NoError(t, err)
	assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	err = core.Stop()
	assert.NoError(t, err)
}

// make sure the main functions work well when EnableActiveStandby=false
func TestRootcoord_DisableActiveStandby(t *testing.T) {
	randVal := rand.Int()
	paramtable.Init()
	Params.Save("etcd.rootPath", fmt.Sprintf("/%d", randVal))
	// Need to reset global etcd to follow new path
	kvfactory.CloseEtcdClient()

	paramtable.Get().Save(Params.RootCoordCfg.EnableActiveStandby.Key, "false")
	paramtable.Get().Save(Params.CommonCfg.RootCoordTimeTick.Key, fmt.Sprintf("rootcoord-time-tick-%d", randVal))
	paramtable.Get().Save(Params.CommonCfg.RootCoordStatistics.Key, fmt.Sprintf("rootcoord-statistics-%d", randVal))
	paramtable.Get().Save(Params.CommonCfg.RootCoordDml.Key, fmt.Sprintf("rootcoord-dml-test-%d", randVal))

	ctx := context.Background()
	coreFactory := dependency.NewDefaultFactory(true)
	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	assert.NoError(t, err)
	defer etcdCli.Close()
	core, err := NewCore(ctx, coreFactory)
	core.etcdCli = etcdCli
	assert.NoError(t, err)
	core.SetTiKVClient(tikv.SetupLocalTxn())

	err = core.Init()
	assert.NoError(t, err)
	assert.Equal(t, commonpb.StateCode_Initializing, core.GetStateCode())
	err = core.Start()
	assert.NoError(t, err)
	core.session.TriggerKill = false
	err = core.Register()
	assert.NoError(t, err)
	assert.Equal(t, commonpb.StateCode_Healthy, core.GetStateCode())
	resp, err := core.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DescribeCollection,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  paramtable.GetNodeID(),
		},
		CollectionName: "unexist",
	})
	assert.NoError(t, err)
	assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	err = core.Stop()
	assert.NoError(t, err)
}

func TestRootCoord_AlterCollection(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("add task failed", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())

		ctx := context.Background()
		resp, err := c.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("execute task failed", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())

		ctx := context.Background()
		resp, err := c.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})

	t.Run("run ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())

		ctx := context.Background()
		resp, err := c.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
	})
}

func TestRootCoord_CheckHealth(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
		assert.NoError(t, err)
		assert.Equal(t, false, resp.IsHealthy)
		assert.NotEmpty(t, resp.Reasons)
	})

	t.Run("proxy health check fail with invalid proxy", func(t *testing.T) {
		c := newTestCore(withHealthyCode(), withInvalidProxyManager())
		c.healthChecker = healthcheck.NewChecker(40*time.Millisecond, c.healthCheckFn)
		c.healthChecker.Start()
		defer c.healthChecker.Close()

		time.Sleep(50 * time.Millisecond)

		ctx := context.Background()
		resp, err := c.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
		assert.NoError(t, err)
		assert.Equal(t, false, resp.IsHealthy)
		assert.NotEmpty(t, resp.Reasons)
	})

	t.Run("ok", func(t *testing.T) {
		c := newTestCore(withHealthyCode(), withValidProxyManager())
		c.healthChecker = healthcheck.NewChecker(40*time.Millisecond, c.healthCheckFn)
		c.healthChecker.Start()
		defer c.healthChecker.Close()

		time.Sleep(50 * time.Millisecond)

		ctx := context.Background()
		resp, err := c.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
		assert.NoError(t, err)
		assert.Equal(t, true, resp.IsHealthy)
		assert.Empty(t, resp.Reasons)
	})
}

func TestRootCoord_DescribeDatabase(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withAbnormalCode())
		resp, err := c.DescribeDatabase(ctx, &rootcoordpb.DescribeDatabaseRequest{})
		assert.NoError(t, err)
		assert.Error(t, merr.CheckRPCCall(resp.GetStatus(), nil))
	})

	t.Run("add task failed", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withInvalidScheduler())
		resp, err := c.DescribeDatabase(ctx, &rootcoordpb.DescribeDatabaseRequest{})
		assert.NoError(t, err)
		assert.Error(t, merr.CheckRPCCall(resp.GetStatus(), nil))
	})

	t.Run("execute task failed", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withTaskFailScheduler())
		resp, err := c.DescribeDatabase(ctx, &rootcoordpb.DescribeDatabaseRequest{})
		assert.NoError(t, err)
		assert.Error(t, merr.CheckRPCCall(resp.GetStatus(), nil))
	})

	t.Run("run ok", func(t *testing.T) {
		ctx := context.Background()
		c := newTestCore(withHealthyCode(),
			withValidScheduler())
		resp, err := c.DescribeDatabase(ctx, &rootcoordpb.DescribeDatabaseRequest{})
		assert.NoError(t, err)
		assert.NoError(t, merr.CheckRPCCall(resp.GetStatus(), nil))
	})
}

func TestRootCoord_RBACError(t *testing.T) {
	ctx := context.Background()
	c := newTestCore(withHealthyCode(), withInvalidMeta())
	t.Run("create credential failed", func(t *testing.T) {
		resp, err := c.CreateCredential(ctx, &internalpb.CredentialInfo{Username: "foo", EncryptedPassword: "bar"})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})
	t.Run("get credential failed", func(t *testing.T) {
		resp, err := c.GetCredential(ctx, &rootcoordpb.GetCredentialRequest{Username: "foo"})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
	t.Run("update credential failed", func(t *testing.T) {
		resp, err := c.UpdateCredential(ctx, &internalpb.CredentialInfo{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})
	t.Run("delete credential failed", func(t *testing.T) {
		resp, err := c.DeleteCredential(ctx, &milvuspb.DeleteCredentialRequest{Username: "foo"})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})
	t.Run("list credential failed", func(t *testing.T) {
		resp, err := c.ListCredUsers(ctx, &milvuspb.ListCredUsersRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
	t.Run("create role failed", func(t *testing.T) {
		resp, err := c.CreateRole(ctx, &milvuspb.CreateRoleRequest{Entity: &milvuspb.RoleEntity{Name: "foo"}})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})
	t.Run("drop role failed", func(t *testing.T) {
		resp, err := c.DropRole(ctx, &milvuspb.DropRoleRequest{RoleName: "foo"})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})
	t.Run("operate user role failed", func(t *testing.T) {
		mockMeta := c.meta.(*mockMetaTable)
		mockMeta.SelectRoleFunc = func(tenant string, entity *milvuspb.RoleEntity, includeUserInfo bool) ([]*milvuspb.RoleResult, error) {
			return nil, nil
		}
		mockMeta.SelectUserFunc = func(tenant string, entity *milvuspb.UserEntity, includeRoleInfo bool) ([]*milvuspb.UserResult, error) {
			return nil, nil
		}
		resp, err := c.OperateUserRole(ctx, &milvuspb.OperateUserRoleRequest{RoleName: "foo", Username: "bar", Type: milvuspb.OperateUserRoleType_AddUserToRole})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
		mockMeta.SelectRoleFunc = func(tenant string, entity *milvuspb.RoleEntity, includeUserInfo bool) ([]*milvuspb.RoleResult, error) {
			return nil, errors.New("mock error")
		}
		mockMeta.SelectUserFunc = func(tenant string, entity *milvuspb.UserEntity, includeRoleInfo bool) ([]*milvuspb.UserResult, error) {
			return nil, errors.New("mock error")
		}
	})
	t.Run("select role failed", func(t *testing.T) {
		{
			resp, err := c.SelectRole(ctx, &milvuspb.SelectRoleRequest{Role: &milvuspb.RoleEntity{Name: "foo"}})
			assert.NoError(t, err)
			assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		}
		{
			resp, err := c.SelectRole(ctx, &milvuspb.SelectRoleRequest{})
			assert.NoError(t, err)
			assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		}
	})
	t.Run("select user failed", func(t *testing.T) {
		{
			resp, err := c.SelectUser(ctx, &milvuspb.SelectUserRequest{User: &milvuspb.UserEntity{Name: "foo"}})
			assert.NoError(t, err)
			assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		}
		{
			resp, err := c.SelectUser(ctx, &milvuspb.SelectUserRequest{})
			assert.NoError(t, err)
			assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		}
	})
	t.Run("operate privilege failed", func(t *testing.T) {
		{
			resp, err := c.OperatePrivilege(ctx, &milvuspb.OperatePrivilegeRequest{Type: milvuspb.OperatePrivilegeType(100)})
			assert.NoError(t, err)
			assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
		}
		{
			resp, err := c.OperatePrivilege(ctx, &milvuspb.OperatePrivilegeRequest{Type: milvuspb.OperatePrivilegeType_Grant})
			assert.NoError(t, err)
			assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
		}
		{
			resp, err := c.OperatePrivilege(ctx, &milvuspb.OperatePrivilegeRequest{Entity: &milvuspb.GrantEntity{Object: &milvuspb.ObjectEntity{Name: "CollectionErr"}}})
			assert.NoError(t, err)
			assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
		}
		{
			resp, err := c.OperatePrivilege(ctx, &milvuspb.OperatePrivilegeRequest{Entity: &milvuspb.GrantEntity{Object: &milvuspb.ObjectEntity{Name: "Collection"}, Role: &milvuspb.RoleEntity{Name: "foo"}}})
			assert.NoError(t, err)
			assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
		}

		mockMeta := c.meta.(*mockMetaTable)
		mockMeta.SelectRoleFunc = func(tenant string, entity *milvuspb.RoleEntity, includeUserInfo bool) ([]*milvuspb.RoleResult, error) {
			return nil, nil
		}
		mockMeta.ListPrivilegeGroupsFunc = func() ([]*milvuspb.PrivilegeGroupInfo, error) {
			return nil, nil
		}
		{
			resp, err := c.OperatePrivilege(ctx, &milvuspb.OperatePrivilegeRequest{Entity: &milvuspb.GrantEntity{
				Role:       &milvuspb.RoleEntity{Name: "foo"},
				Object:     &milvuspb.ObjectEntity{Name: "Collection"},
				ObjectName: "col1",
				Grantor: &milvuspb.GrantorEntity{
					User:      &milvuspb.UserEntity{Name: "root"},
					Privilege: &milvuspb.PrivilegeEntity{Name: "Insert"},
				},
			}, Type: milvuspb.OperatePrivilegeType_Grant})
			assert.NoError(t, err)
			assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
		}
		mockMeta.IsCustomPrivilegeGroupFunc = func(groupName string) (bool, error) {
			return false, nil
		}
		mockMeta.SelectUserFunc = func(tenant string, entity *milvuspb.UserEntity, includeRoleInfo bool) ([]*milvuspb.UserResult, error) {
			return nil, nil
		}
		resp, err := c.OperatePrivilege(ctx, &milvuspb.OperatePrivilegeRequest{Entity: &milvuspb.GrantEntity{
			Role:       &milvuspb.RoleEntity{Name: "foo"},
			Object:     &milvuspb.ObjectEntity{Name: "Collection"},
			ObjectName: "col1",
			Grantor: &milvuspb.GrantorEntity{
				User:      &milvuspb.UserEntity{Name: "root"},
				Privilege: &milvuspb.PrivilegeEntity{Name: "Insert"},
			},
		}, Type: milvuspb.OperatePrivilegeType_Grant})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
		mockMeta.SelectRoleFunc = func(tenant string, entity *milvuspb.RoleEntity, includeUserInfo bool) ([]*milvuspb.RoleResult, error) {
			return nil, errors.New("mock error")
		}
		mockMeta.SelectUserFunc = func(tenant string, entity *milvuspb.UserEntity, includeRoleInfo bool) ([]*milvuspb.UserResult, error) {
			return nil, errors.New("mock error")
		}
	})

	t.Run("operate privilege group failed", func(t *testing.T) {
		mockMeta := c.meta.(*mockMetaTable)
		mockMeta.ListPrivilegeGroupsFunc = func() ([]*milvuspb.PrivilegeGroupInfo, error) {
			return nil, errors.New("mock error")
		}
		mockMeta.CreatePrivilegeGroupFunc = func(groupName string) error {
			return errors.New("mock error")
		}
		mockMeta.GetPrivilegeGroupRolesFunc = func(groupName string) ([]*milvuspb.RoleEntity, error) {
			return nil, errors.New("mock error")
		}
		{
			resp, err := c.OperatePrivilegeGroup(ctx, &milvuspb.OperatePrivilegeGroupRequest{})
			assert.NoError(t, err)
			assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
		}
		{
			resp, err := c.ListPrivilegeGroups(ctx, &milvuspb.ListPrivilegeGroupsRequest{})
			assert.NoError(t, err)
			assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		}
		{
			resp, err := c.OperatePrivilegeGroup(ctx, &milvuspb.OperatePrivilegeGroupRequest{})
			assert.NoError(t, err)
			assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
		}
		{
			resp, err := c.CreatePrivilegeGroup(ctx, &milvuspb.CreatePrivilegeGroupRequest{})
			assert.NoError(t, err)
			assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetErrorCode())
		}
	})

	t.Run("select grant failed", func(t *testing.T) {
		{
			resp, err := c.SelectGrant(ctx, &milvuspb.SelectGrantRequest{})
			assert.NoError(t, err)
			assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		}
		{
			resp, err := c.SelectGrant(ctx, &milvuspb.SelectGrantRequest{Entity: &milvuspb.GrantEntity{Role: &milvuspb.RoleEntity{Name: "foo"}}})
			assert.NoError(t, err)
			assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		}
		mockMeta := c.meta.(*mockMetaTable)
		mockMeta.SelectRoleFunc = func(tenant string, entity *milvuspb.RoleEntity, includeUserInfo bool) ([]*milvuspb.RoleResult, error) {
			return nil, nil
		}
		{
			resp, err := c.SelectGrant(ctx, &milvuspb.SelectGrantRequest{Entity: &milvuspb.GrantEntity{Role: &milvuspb.RoleEntity{Name: "foo"}, Object: &milvuspb.ObjectEntity{Name: "CollectionFoo"}}})
			assert.NoError(t, err)
			assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		}
		{
			resp, err := c.SelectGrant(ctx, &milvuspb.SelectGrantRequest{Entity: &milvuspb.GrantEntity{Role: &milvuspb.RoleEntity{Name: "foo"}, Object: &milvuspb.ObjectEntity{Name: "Collection"}}})
			assert.NoError(t, err)
			assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		}
		mockMeta.SelectRoleFunc = func(tenant string, entity *milvuspb.RoleEntity, includeUserInfo bool) ([]*milvuspb.RoleResult, error) {
			return nil, errors.New("mock error")
		}
	})

	t.Run("select grant success", func(t *testing.T) {
		mockMeta := c.meta.(*mockMetaTable)
		mockMeta.SelectRoleFunc = func(tenant string, entity *milvuspb.RoleEntity, includeUserInfo bool) ([]*milvuspb.RoleResult, error) {
			return []*milvuspb.RoleResult{
				{
					Role: &milvuspb.RoleEntity{Name: "foo"},
				},
			}, nil
		}
		mockMeta.SelectGrantFunc = func(tenant string, entity *milvuspb.GrantEntity) ([]*milvuspb.GrantEntity, error) {
			return []*milvuspb.GrantEntity{
				{
					Role: &milvuspb.RoleEntity{Name: "foo"},
				},
			}, merr.ErrIoKeyNotFound
		}

		{
			resp, err := c.SelectGrant(ctx, &milvuspb.SelectGrantRequest{Entity: &milvuspb.GrantEntity{Role: &milvuspb.RoleEntity{Name: "foo"}, Object: &milvuspb.ObjectEntity{Name: "Collection"}, ObjectName: "fir"}})
			assert.NoError(t, err)
			assert.Equal(t, 1, len(resp.GetEntities()))
			assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		}

		mockMeta.SelectRoleFunc = func(tenant string, entity *milvuspb.RoleEntity, includeUserInfo bool) ([]*milvuspb.RoleResult, error) {
			return nil, errors.New("mock error")
		}

		mockMeta.SelectGrantFunc = func(tenant string, entity *milvuspb.GrantEntity) ([]*milvuspb.GrantEntity, error) {
			return nil, errors.New("mock error")
		}
	})

	t.Run("list policy failed", func(t *testing.T) {
		resp, err := c.ListPolicy(ctx, &internalpb.ListPolicyRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())

		mockMeta := c.meta.(*mockMetaTable)
		mockMeta.ListPolicyFunc = func(tenant string) ([]*milvuspb.GrantEntity, error) {
			return []*milvuspb.GrantEntity{{
				ObjectName: "*",
				Object: &milvuspb.ObjectEntity{
					Name: "Global",
				},
				Role:    &milvuspb.RoleEntity{Name: "role"},
				Grantor: &milvuspb.GrantorEntity{Privilege: &milvuspb.PrivilegeEntity{Name: "CustomGroup"}},
			}}, nil
		}
		resp, err = c.ListPolicy(ctx, &internalpb.ListPolicyRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		mockMeta.ListPrivilegeGroupsFunc = func() ([]*milvuspb.PrivilegeGroupInfo, error) {
			return []*milvuspb.PrivilegeGroupInfo{
				{
					GroupName:  "CollectionAdmin",
					Privileges: []*milvuspb.PrivilegeEntity{{Name: "CreateCollection"}},
				},
			}, nil
		}
		resp, err = c.ListPolicy(ctx, &internalpb.ListPolicyRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		mockMeta.IsCustomPrivilegeGroupFunc = func(groupName string) (bool, error) {
			return true, nil
		}
		resp, err = c.ListPolicy(ctx, &internalpb.ListPolicyRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		mockMeta.ListPolicyFunc = func(tenant string) ([]*milvuspb.GrantEntity, error) {
			return []*milvuspb.GrantEntity{}, errors.New("mock error")
		}
		mockMeta.ListPrivilegeGroupsFunc = func() ([]*milvuspb.PrivilegeGroupInfo, error) {
			return []*milvuspb.PrivilegeGroupInfo{}, errors.New("mock error")
		}
		mockMeta.IsCustomPrivilegeGroupFunc = func(groupName string) (bool, error) {
			return false, errors.New("mock error")
		}
	})
}

func TestRootCoord_BuiltinRoles(t *testing.T) {
	roleDbAdmin := "db_admin"
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().RoleCfg.Enabled.Key, "true")
	paramtable.Get().Save(paramtable.Get().RoleCfg.Roles.Key, `{"`+roleDbAdmin+`": {"privileges": [{"object_type": "Global", "object_name": "*", "privilege": "CreateCollection", "db_name": "*"}]}}`)
	t.Run("init builtin roles success", func(t *testing.T) {
		c := newTestCore(withHealthyCode(), withInvalidMeta())
		mockMeta := c.meta.(*mockMetaTable)
		mockMeta.CreateRoleFunc = func(tenant string, entity *milvuspb.RoleEntity) error {
			return nil
		}
		mockMeta.OperatePrivilegeFunc = func(tenant string, entity *milvuspb.GrantEntity, operateType milvuspb.OperatePrivilegeType) error {
			return nil
		}
		mockMeta.ListPrivilegeGroupsFunc = func() ([]*milvuspb.PrivilegeGroupInfo, error) {
			return nil, nil
		}
		err := c.initBuiltinRoles()
		assert.Equal(t, nil, err)
		assert.True(t, util.IsBuiltinRole(roleDbAdmin))
		assert.False(t, util.IsBuiltinRole(util.RoleAdmin))
		resp, err := c.DropRole(context.Background(), &milvuspb.DropRoleRequest{RoleName: roleDbAdmin})
		assert.Equal(t, nil, err)
		assert.Equal(t, int32(1401), resp.Code) // merr.ErrPrivilegeNotPermitted
	})
	t.Run("init builtin roles fail to create role", func(t *testing.T) {
		c := newTestCore(withHealthyCode(), withInvalidMeta())
		mockMeta := c.meta.(*mockMetaTable)
		mockMeta.CreateRoleFunc = func(tenant string, entity *milvuspb.RoleEntity) error {
			return merr.ErrPrivilegeNotPermitted
		}
		err := c.initBuiltinRoles()
		assert.Error(t, err)
	})
	t.Run("init builtin roles fail to operate privileg", func(t *testing.T) {
		c := newTestCore(withHealthyCode(), withInvalidMeta())
		mockMeta := c.meta.(*mockMetaTable)
		mockMeta.CreateRoleFunc = func(tenant string, entity *milvuspb.RoleEntity) error {
			return nil
		}
		mockMeta.OperatePrivilegeFunc = func(tenant string, entity *milvuspb.GrantEntity, operateType milvuspb.OperatePrivilegeType) error {
			return merr.ErrPrivilegeNotPermitted
		}
		err := c.initBuiltinRoles()
		assert.Error(t, err)
	})
}

func TestCore_Stop(t *testing.T) {
	t.Run("abnormal stop before component is ready", func(t *testing.T) {
		c := &Core{}
		err := c.Stop()
		assert.NoError(t, err)
		code := c.GetStateCode()
		assert.Equal(t, commonpb.StateCode_Abnormal, code)
	})

	t.Run("normal case", func(t *testing.T) {
		c := newTestCore(withHealthyCode(),
			withValidScheduler())
		c.ctx, c.cancel = context.WithCancel(context.Background())
		err := c.Stop()
		assert.NoError(t, err)
		code := c.GetStateCode()
		assert.Equal(t, commonpb.StateCode_Abnormal, code)
	})
}

func TestCore_InitRBAC(t *testing.T) {
	paramtable.Init()
	t.Run("init default role and public role privilege", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		c := newTestCore(withHealthyCode(), withMeta(meta))
		meta.EXPECT().CreateRole(mock.Anything, mock.Anything).Return(nil).Twice()
		meta.EXPECT().OperatePrivilege(mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(3)

		Params.Save(Params.RoleCfg.Enabled.Key, "false")
		Params.Save(Params.CommonCfg.EnablePublicPrivilege.Key, "true")

		defer func() {
			Params.Reset(Params.RoleCfg.Enabled.Key)
			Params.Reset(Params.CommonCfg.EnablePublicPrivilege.Key)
		}()

		err := c.initRbac()
		assert.NoError(t, err)
	})

	t.Run("not init public role privilege and init default privilege", func(t *testing.T) {
		builtinRoles := `{"db_admin": {"privileges": [{"object_type": "Global", "object_name": "*", "privilege": "CreateCollection", "db_name": "*"}]}}`
		meta := mockrootcoord.NewIMetaTable(t)
		c := newTestCore(withHealthyCode(), withMeta(meta))
		meta.EXPECT().CreateRole(mock.Anything, mock.Anything).Return(nil).Times(3)
		meta.EXPECT().OperatePrivilege(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		Params.Save(Params.RoleCfg.Enabled.Key, "true")
		Params.Save(Params.RoleCfg.Roles.Key, builtinRoles)
		Params.Save(Params.CommonCfg.EnablePublicPrivilege.Key, "false")

		defer func() {
			Params.Reset(Params.RoleCfg.Enabled.Key)
			Params.Reset(Params.RoleCfg.Roles.Key)
			Params.Reset(Params.CommonCfg.EnablePublicPrivilege.Key)
		}()

		err := c.initRbac()
		assert.NoError(t, err)
	})

	t.Run("compact proxy setting", func(t *testing.T) {
		builtinRoles := `{"db_admin": {"privileges": [{"object_type": "Global", "object_name": "*", "privilege": "CreateCollection", "db_name": "*"}]}}`
		meta := mockrootcoord.NewIMetaTable(t)
		c := newTestCore(withHealthyCode(), withMeta(meta))
		meta.EXPECT().CreateRole(mock.Anything, mock.Anything).Return(nil).Times(3)
		meta.EXPECT().OperatePrivilege(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		Params.Save(Params.RoleCfg.Enabled.Key, "true")
		Params.Save(Params.RoleCfg.Roles.Key, builtinRoles)
		Params.Save("proxy.enablePublicPrivilege", "false")

		defer func() {
			Params.Reset(Params.RoleCfg.Enabled.Key)
			Params.Reset(Params.RoleCfg.Roles.Key)
			Params.Reset("proxy.enablePublicPrivilege")
		}()

		err := c.initRbac()
		assert.NoError(t, err)
	})
}

func TestCore_BackupRBAC(t *testing.T) {
	meta := mockrootcoord.NewIMetaTable(t)
	c := newTestCore(withHealthyCode(), withMeta(meta))

	meta.EXPECT().BackupRBAC(mock.Anything, mock.Anything).Return(&milvuspb.RBACMeta{}, nil)
	resp, err := c.BackupRBAC(context.Background(), &milvuspb.BackupRBACMetaRequest{})
	assert.NoError(t, err)
	assert.True(t, merr.Ok(resp.GetStatus()))

	meta.ExpectedCalls = nil
	meta.EXPECT().BackupRBAC(mock.Anything, mock.Anything).Return(nil, errors.New("mock error"))
	resp, err = c.BackupRBAC(context.Background(), &milvuspb.BackupRBACMetaRequest{})
	assert.NoError(t, err)
	assert.False(t, merr.Ok(resp.GetStatus()))
}

func TestCore_RestoreRBAC(t *testing.T) {
	meta := mockrootcoord.NewIMetaTable(t)
	c := newTestCore(withHealthyCode(), withMeta(meta))
	mockProxyClientManager := proxyutil.NewMockProxyClientManager(t)
	mockProxyClientManager.EXPECT().RefreshPolicyInfoCache(mock.Anything, mock.Anything).Return(nil)
	c.proxyClientManager = mockProxyClientManager

	meta.EXPECT().RestoreRBAC(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	resp, err := c.RestoreRBAC(context.Background(), &milvuspb.RestoreRBACMetaRequest{})
	assert.NoError(t, err)
	assert.True(t, merr.Ok(resp))

	meta.ExpectedCalls = nil
	meta.EXPECT().RestoreRBAC(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("mock error"))
	resp, err = c.RestoreRBAC(context.Background(), &milvuspb.RestoreRBACMetaRequest{})
	assert.NoError(t, err)
	assert.False(t, merr.Ok(resp))
}

func TestCore_getMetastorePrivilegeName(t *testing.T) {
	meta := mockrootcoord.NewIMetaTable(t)
	c := newTestCore(withHealthyCode(), withMeta(meta))

	priv, err := c.getMetastorePrivilegeName(context.Background(), util.AnyWord)
	assert.NoError(t, err)
	assert.Equal(t, priv, util.AnyWord)

	meta.EXPECT().IsCustomPrivilegeGroup("unknown").Return(false, nil)
	_, err = c.getMetastorePrivilegeName(context.Background(), "unknown")
	assert.Equal(t, err.Error(), "not found the privilege name [unknown] from metastore")
}

func TestCore_expandPrivilegeGroup(t *testing.T) {
	meta := mockrootcoord.NewIMetaTable(t)
	c := newTestCore(withHealthyCode(), withMeta(meta))

	grants := []*milvuspb.GrantEntity{
		{
			ObjectName: "*",
			Object: &milvuspb.ObjectEntity{
				Name: "Global",
			},
			Role:    &milvuspb.RoleEntity{Name: "role"},
			Grantor: &milvuspb.GrantorEntity{Privilege: &milvuspb.PrivilegeEntity{Name: "*"}},
		},
	}
	groups := map[string][]*milvuspb.PrivilegeEntity{}
	expandGrants, err := c.expandPrivilegeGroups(grants, groups)
	assert.NoError(t, err)
	assert.Equal(t, len(expandGrants), len(grants))
	assert.Equal(t, expandGrants[0].Grantor.Privilege.Name, grants[0].Grantor.Privilege.Name)
}

type RootCoordSuite struct {
	suite.Suite
}

func (s *RootCoordSuite) TestRestore() {
	meta := mockrootcoord.NewIMetaTable(s.T())
	gc := mockrootcoord.NewGarbageCollector(s.T())

	finishCh := make(chan struct{}, 4)
	gc.EXPECT().ReDropPartition(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Once().
		Run(func(args mock.Arguments) {
			finishCh <- struct{}{}
		})
	gc.EXPECT().RemoveCreatingPartition(mock.Anything, mock.Anything, mock.Anything).Once().
		Run(func(args mock.Arguments) {
			finishCh <- struct{}{}
		})
	gc.EXPECT().ReDropCollection(mock.Anything, mock.Anything).Once().
		Run(func(args mock.Arguments) {
			finishCh <- struct{}{}
		})
	gc.EXPECT().RemoveCreatingCollection(mock.Anything).Once().
		Run(func(args mock.Arguments) {
			finishCh <- struct{}{}
		})

	meta.EXPECT().ListDatabases(mock.Anything, mock.Anything).
		Return([]*model.Database{
			{Name: "available_colls_db"},
			{Name: "not_available_colls_db"},
		}, nil)

	meta.EXPECT().ListCollections(mock.Anything, "available_colls_db", mock.Anything, false).
		Return([]*model.Collection{
			{
				DBID:                 1,
				State:                etcdpb.CollectionState_CollectionCreated, // available collection
				PhysicalChannelNames: []string{"ch1"},
				Partitions: []*model.Partition{
					{State: etcdpb.PartitionState_PartitionDropping},
					{State: etcdpb.PartitionState_PartitionCreating},
					{State: etcdpb.PartitionState_PartitionDropped}, // ignored
				},
			},
		}, nil)
	meta.EXPECT().ListCollections(mock.Anything, "not_available_colls_db", mock.Anything, false).
		Return([]*model.Collection{
			{
				DBID:                 1,
				State:                etcdpb.CollectionState_CollectionDropping, // not available collection
				PhysicalChannelNames: []string{"ch1"},
				Partitions: []*model.Partition{
					{State: etcdpb.PartitionState_PartitionDropping},
					{State: etcdpb.PartitionState_PartitionCreating},
					{State: etcdpb.PartitionState_PartitionDropped},
				},
			},
			{
				DBID:                 1,
				State:                etcdpb.CollectionState_CollectionCreating, // not available collection
				PhysicalChannelNames: []string{"ch1"},
				Partitions: []*model.Partition{
					{State: etcdpb.PartitionState_PartitionDropping},
					{State: etcdpb.PartitionState_PartitionCreating},
					{State: etcdpb.PartitionState_PartitionDropped},
				},
			},
			{
				DBID:                 1,
				State:                etcdpb.CollectionState_CollectionDropped, // ignored
				PhysicalChannelNames: []string{"ch1"},
				Partitions: []*model.Partition{
					{State: etcdpb.PartitionState_PartitionDropping},
					{State: etcdpb.PartitionState_PartitionCreating},
					{State: etcdpb.PartitionState_PartitionDropped},
				},
			},
		}, nil)

	// ticker := newTickerWithMockNormalStream()
	tsoAllocator := newMockTsoAllocator()
	tsoAllocator.GenerateTSOF = func(count uint32) (uint64, error) {
		return 100, nil
	}
	core := newTestCore(
		withGarbageCollector(gc),
		// withTtSynchronizer(ticker),
		withTsoAllocator(tsoAllocator),
		// withValidProxyManager(),
		withMeta(meta))
	core.restore(context.Background())

	for i := 0; i < 4; i++ {
		<-finishCh
	}
}

func TestRootCoordSuite(t *testing.T) {
	suite.Run(t, new(RootCoordSuite))
}
