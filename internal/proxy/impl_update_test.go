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

package proxy

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestPKUpdateDefaultAdmission(t *testing.T) {
	paramtable.Init()
	// No initialized cache/WAL: disabled requests must stop at the public entry.
	result, err := (&Proxy{}).UpdateRecords(t.Context(), &milvuspb.UpdateRecordsRequest{})
	require.NoError(t, err)
	require.ErrorIs(t, merr.Error(result.GetStatus()), merr.ErrServiceUnimplemented)
	require.False(t, result.GetAcknowledged())
	_, req := fillDatabase(GetContextWithDB(t.Context(), "root:test", "people_db"), &milvuspb.UpdateRecordsRequest{})
	require.Equal(t, "people_db", req.(*milvuspb.UpdateRecordsRequest).GetDbName())
	require.IsType(t, &milvuspb.MutationResult{}, GetFailedResponse(req, merr.ErrServiceUnimplemented))
}

func TestPKUpdateAdmissionConfig(t *testing.T) {
	paramtable.Init()
	t.Cleanup(func() { paramtable.Get().Reset("proxy.pkUpdate.enabled") })
	for _, enabled := range []string{"false", "true", "false"} {
		require.NoError(t, paramtable.Get().Save("proxy.pkUpdate.enabled", enabled))
		result, err := (&Proxy{}).UpdateRecords(t.Context(), &milvuspb.UpdateRecordsRequest{})
		require.NoError(t, err)
		require.False(t, result.GetAcknowledged())
		if enabled == "false" {
			require.ErrorIs(t, merr.Error(result.GetStatus()), merr.ErrServiceUnimplemented)
			require.Contains(t, result.GetStatus().GetReason(), "proxy.pkUpdate.enabled")
		} else {
			require.ErrorIs(t, merr.Error(result.GetStatus()), merr.ErrServiceNotReady)
		}
	}
}
