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

package streamingnode

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestGetVChannelBase(t *testing.T) {
	ctx := t.Context()
	client, _ := kvfactory.GetEtcdAndPath()
	kv := etcdkv.NewEtcdKV(client, "qv-l0-base-"+uuid.NewString())
	t.Cleanup(func() { require.NoError(t, kv.RemoveWithPrefix(context.Background(), "")) })
	catalog := NewCataLog(kv)
	base, err := catalog.GetVChannelBase(ctx, "p1", "v1")
	require.NoError(t, err)
	require.Nil(t, base)
	data, err := proto.Marshal(&streamingpb.VChannelMeta{Vchannel: "v1", TransformMaterializedTimeTick: 300})
	require.NoError(t, err)
	require.NoError(t, kv.Save(ctx, buildVChannelKey("p1", "v1"), string(data)))
	// The single-key read must neither list nor parse historical schemas.
	require.NoError(t, kv.Save(ctx, buildVChannelSchemaKey("p1", "v1", 1), "corrupt schema"))
	base, err = catalog.GetVChannelBase(ctx, "p1", "v1")
	require.NoError(t, err)
	require.EqualValues(t, 300, base.GetTransformMaterializedTimeTick())
	require.Empty(t, base.GetCollectionInfo().GetSchemas())
	require.NoError(t, kv.Save(ctx, buildVChannelKey("p1", "v1"), "corrupt base"))
	_, err = catalog.GetVChannelBase(ctx, "p1", "v1")
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.NoError(t, kv.Save(ctx, buildVChannelKey("p1", "other"), string(data)))
	_, err = catalog.GetVChannelBase(ctx, "p1", "other")
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	failure := merr.WrapErrServiceUnavailableMsg("injected read failure")
	read := mockey.Mock(mockey.GetMethod(kv, "Load")).Return("", failure).Build()
	defer read.UnPatch()
	_, err = catalog.GetVChannelBase(ctx, "p1", "v1")
	require.ErrorIs(t, err, failure)
}
