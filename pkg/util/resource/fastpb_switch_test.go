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

package resource

import (
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/mem"

	milvuspb "github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	schemapb "github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestFastPBStartupDoesNotInitializeConfig(t *testing.T) {
	if os.Getenv("MILVUS_FASTPB_STARTUP_TEST") != "child" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestFastPBStartupDoesNotInitializeConfig$")
		cmd.Env = append(os.Environ(), "MILVUS_FASTPB_STARTUP_TEST=child")
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, string(output))
		return
	}
	require.Nil(t, paramtable.GetIfInitialized())
	require.True(t, fastPBEnabled())
	// Trusted internal strings skip UTF-8 checks only on the fast decoder path.
	wire := []byte{0x3a, 1, 0xff}
	require.NoError(t, releaseCodec{}.Unmarshal(mem.BufferSlice{mem.SliceBuffer(wire)}, &internalpb.RetrieveResults{}))
	require.NoError(t, UnmarshalSearchResultData(wire, &schemapb.SearchResultData{}))
	require.Nil(t, paramtable.GetIfInitialized(), "decode must not trigger parameter initialization")
}

func TestFastPBEnabledLiveToggle(t *testing.T) {
	base := paramtable.NewBaseTable(paramtable.SkipRemote(true), paramtable.SkipEnv(true))
	require.NoError(t, base.Save("localStorage.path", t.TempDir()))
	paramtable.InitWithBaseTable(base)
	params := paramtable.GetIfInitialized()
	require.NotNil(t, params)
	key := params.CommonCfg.EnableFastPB.Key
	old := params.CommonCfg.EnableFastPB.GetValue()
	t.Cleanup(func() { require.NoError(t, params.Save(key, old)) })
	codec := releaseCodec{}
	for _, enabled := range []bool{true, false, true} {
		value := "false"
		if enabled {
			value = "true"
		}
		require.NoError(t, params.Save(key, value))
		require.Equal(t, enabled, fastPBEnabled())
		wire := []byte{0x3a, 1, 0xff}
		result := &internalpb.RetrieveResults{}
		err := codec.Unmarshal(mem.BufferSlice{mem.SliceBuffer(wire)}, result)
		search := &schemapb.SearchResultData{}
		searchErr := UnmarshalSearchResultData(wire, search)
		if enabled {
			require.NoError(t, err)
			require.NoError(t, searchErr)
			require.Equal(t, []string{string([]byte{0xff})}, result.ChannelIDsRetrieved)
			require.Equal(t, []string{string([]byte{0xff})}, search.OutputFields)
			wire[2] = 0
			require.Equal(t, string([]byte{0xff}), result.ChannelIDsRetrieved[0])
			require.Equal(t, string([]byte{0xff}), search.OutputFields[0])
		} else {
			require.Error(t, err)
			require.Error(t, searchErr)
		}
		validWire := []byte{0x3a, 1, 'x'}
		ownedRetrieve := &internalpb.RetrieveResults{}
		ownedSearch := &schemapb.SearchResultData{}
		require.NoError(t, codec.Unmarshal(mem.BufferSlice{mem.SliceBuffer(validWire)}, ownedRetrieve))
		require.NoError(t, UnmarshalSearchResultData(validWire, ownedSearch))
		validWire[2] = 'z'
		require.Equal(t, []string{"x"}, ownedRetrieve.ChannelIDsRetrieved)
		require.Equal(t, []string{"x"}, ownedSearch.OutputFields)
		clientWire := mem.BufferSlice{mem.SliceBuffer([]byte{0x1a, 1, 0xff})}
		for _, message := range []any{&milvuspb.InsertRequest{}, &milvuspb.UpsertRequest{}} {
			err := codec.Unmarshal(clientWire, message)
			require.Error(t, err)
			if enabled {
				require.Contains(t, err.Error(), "fastpb: invalid UTF-8")
			} else {
				require.NotContains(t, err.Error(), "fastpb:")
			}
		}
	}
}
