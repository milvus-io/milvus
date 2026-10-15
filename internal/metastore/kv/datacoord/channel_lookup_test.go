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

package datacoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestChannelExists(t *testing.T) {
	for _, tc := range []struct {
		name    string
		value   string
		loadErr error
		exists  bool
		wantErr error
	}{
		{name: "active", value: NonRemoveFlagTomestone, exists: true},
		{name: "removed", value: RemoveFlagTomestone},
		{name: "not found", loadErr: merr.WrapErrIoKeyNotFound("channel")},
		{name: "wrapped not found", loadErr: merr.Wrap(merr.WrapErrIoKeyNotFound("channel"), "load")},
		{name: "deadline", loadErr: context.DeadlineExceeded, wantErr: context.DeadlineExceeded},
		{name: "canceled", loadErr: context.Canceled, wantErr: context.Canceled},
		{name: "permission denied", loadErr: rpctypes.ErrPermissionDenied, wantErr: rpctypes.ErrPermissionDenied},
		{name: "unavailable", loadErr: merr.ErrServiceUnavailable, wantErr: merr.ErrServiceUnavailable},
		{name: "io failure", loadErr: merr.ErrIoFailed, wantErr: merr.ErrIoFailed},
		{name: "empty marker preserves legacy behavior"},
		{name: "unknown marker preserves legacy behavior", value: "unexpected"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			kv := etcdkv.NewEtcdKV(nil, "channel-lookup")
			load := mockey.Mock(mockey.GetMethod(kv, "Load")).To(func(gotCtx context.Context, key string) (string, error) {
				require.Same(t, ctx, gotCtx)
				require.Equal(t, buildChannelRemovePath("channel"), key)
				return tc.value, tc.loadErr
			}).Build()
			defer load.UnPatch()
			catalog := &Catalog{MetaKv: kv}
			exists, err := catalog.ChannelExists(ctx, "channel")
			require.Equal(t, tc.exists, exists)
			if tc.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.wantErr)
				require.Equal(t, merr.Code(tc.wantErr), merr.Code(err), "preserve the source error code")
				require.Contains(t, err.Error(), "channel")
			}
			require.Equal(t, 1, load.Times())
		})
	}
}
