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

package meta

import (
	"sort"
	"testing"

	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/stretchr/testify/suite"
)

type StoreTestSuite struct {
	suite.Suite

	kv    kv.MetaKv
	store metaStore
}

func (suite *StoreTestSuite) SetupSuite() {
	Params.Init()
}

func (suite *StoreTestSuite) SetupTest() {
	config := GenerateEtcdConfig()
	cli, err := etcd.GetEtcdClient(
		config.UseEmbedEtcd.GetAsBool(),
		config.EtcdUseSSL.GetAsBool(),
		config.Endpoints.GetAsStrings(),
		config.EtcdTLSCert.GetValue(),
		config.EtcdTLSKey.GetValue(),
		config.EtcdTLSCACert.GetValue(),
		config.EtcdTLSMinVersion.GetValue())
	suite.Require().NoError(err)
	suite.kv = etcdkv.NewEtcdKV(cli, config.MetaRootPath.GetValue())
	suite.store = NewMetaStore(suite.kv)
}

func (suite *StoreTestSuite) TearDownTest() {
	if suite.kv != nil {
		suite.kv.Close()
	}
}

func (suite *StoreTestSuite) TestResourceGroup() {
	suite.store.SaveResourceGroup("rg1", &querypb.ResourceGroup{
		Name:     "rg1",
		Capacity: 3,
		Nodes:    []int64{1, 2, 3},
	})
	suite.store.SaveResourceGroup("rg2", &querypb.ResourceGroup{
		Name:     "rg2",
		Capacity: 3,
		Nodes:    []int64{4, 5},
	})

	suite.store.SaveResourceGroup("rg3", &querypb.ResourceGroup{
		Name:     "rg3",
		Capacity: 0,
		Nodes:    []int64{},
	})

	suite.store.RemoveResourceGroup("rg3")

	groups, err := suite.store.GetResourceGroups()
	suite.NoError(err)
	suite.Len(groups, 2)

	sort.Slice(groups, func(i, j int) bool {
		return groups[i].GetName() < groups[j].GetName()
	})

	suite.Equal("rg1", groups[0].GetName())
	suite.Equal(int32(3), groups[0].GetCapacity())
	suite.Equal([]int64{1, 2, 3}, groups[0].GetNodes())

	suite.Equal("rg2", groups[1].GetName())
	suite.Equal(int32(3), groups[1].GetCapacity())
	suite.Equal([]int64{4, 5}, groups[1].GetNodes())
}

func (suite *StoreTestSuite) TestLoadRelease() {
	// TODO(sunby): add ut
}

func TestStoreSuite(t *testing.T) {
	suite.Run(t, new(StoreTestSuite))
}
