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

	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestServerInitRequiresNativeStoragePlugin(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"ready", nil},
		{"plugin initialization failed", context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			factory := dependency.NewMockFactory(t)
			factory.EXPECT().Init(Params).Once()
			server := CreateServer(context.Background(), factory)
			initialized, sessionStarted := false, false
			initialize := mockey.Mock(initcore.InitPluginLoader).To(func() error {
				initialized = true
				return tc.err
			}).Build()
			defer initialize.UnPatch()
			// Stop at the first external initialization boundary. Native storage
			// must be ready before the server can start sessions or task readers.
			session := mockey.Mock((*Server).initSession).To(func(*Server) error {
				sessionStarted = true
				require.True(t, initialized)
				return context.Canceled
			}).Build()
			defer session.UnPatch()
			err := server.Init()
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
				require.False(t, sessionStarted)
			} else {
				require.ErrorIs(t, err, context.Canceled)
				require.True(t, sessionStarted)
			}
		})
	}
}
