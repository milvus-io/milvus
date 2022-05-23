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

package indexnode

import (
	"context"
	"errors"
	"math/rand"
	"path"
	"strconv"
	"testing"

	"github.com/milvus-io/milvus/internal/util/timerecord"

	"github.com/golang/protobuf/proto"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/stretchr/testify/assert"
)

func TestIndexBuildTask_saveIndexMeta(t *testing.T) {
	Params.Init()
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.NoError(t, err)
	assert.NotNil(t, etcdCli)
	etcdKV := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
	assert.NotNil(t, etcdKV)
	indexBuildID := rand.Int63()
	indexMeta := &indexpb.IndexMeta{
		IndexBuildID: indexBuildID,
		State:        commonpb.IndexState_InProgress,
		NodeID:       1,
		Version:      1,
	}
	metaPath := path.Join("indexes", strconv.FormatInt(indexMeta.IndexBuildID, 10))
	metaValue, err := proto.Marshal(indexMeta)
	assert.NoError(t, err)
	err = etcdKV.Save(metaPath, string(metaValue))
	assert.NoError(t, err)
	indexBuildTask := &IndexBuildTask{
		BaseTask: BaseTask{
			internalErr: errors.New("internal err"),
		},
		etcdKV: etcdKV,
		req: &indexpb.CreateIndexRequest{
			IndexBuildID: indexBuildID,
			Version:      1,
			MetaPath:     metaPath,
		},
		tr: &timerecord.TimeRecorder{},
	}
	err = indexBuildTask.saveIndexMeta(context.Background())
	assert.NoError(t, err)

	indexMeta2, _, err := indexBuildTask.loadIndexMeta(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, indexMeta2)
	assert.Equal(t, commonpb.IndexState_Unissued, indexMeta2.State)

	err = etcdKV.Remove(metaPath)
	assert.NoError(t, err)
}
