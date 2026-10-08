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
	"errors"
	"os"
	"os/exec"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/fastpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func invalidResultString() []byte {
	wire := protowire.AppendTag(nil, 7, protowire.BytesType)
	return protowire.AppendBytes(wire, []byte{0xff})
}

func TestFastPBBeforeInitialization(t *testing.T) {
	if os.Getenv("MILVUS_TEST_FASTPB_PREINIT") == "1" {
		require.Nil(t, paramtable.GetIfInitialized())
		require.True(t, fastPBEnabled())
		require.NoError(t, (releaseCodec{}).Unmarshal(mem.BufferSlice{mem.SliceBuffer(invalidResultString())}, &internalpb.RetrieveResults{}))
		require.NoError(t, UnmarshalSearchResultData(invalidResultString(), &schemapb.SearchResultData{}))
		require.Nil(t, paramtable.GetIfInitialized(), "codec initialized configuration")
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestFastPBBeforeInitialization$")
	cmd.Env = append(os.Environ(), "MILVUS_TEST_FASTPB_PREINIT=1")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", output)
}

func initFastPBTestParams(t *testing.T) *paramtable.ComponentParam {
	t.Helper()
	base := paramtable.NewBaseTable(paramtable.Files([]string{}), paramtable.SkipRemote(true), paramtable.SkipEnv(true))
	require.NoError(t, base.Save("localStorage.path", t.TempDir()))
	paramtable.InitWithBaseTable(base)
	params := paramtable.GetIfInitialized()
	require.NotNil(t, params)
	old := params.CommonCfg.EnableFastPB.GetValue()
	t.Cleanup(func() { require.NoError(t, params.Save("common.enableFastPB", old)) })
	return params
}

func TestFastPBRuntimeResultDecoderSelection(t *testing.T) {
	params := initFastPBTestParams(t)
	require.True(t, params.CommonCfg.EnableFastPB.GetAsBool())
	for _, value := range []string{"true", "false", "true"} {
		require.NoError(t, params.Save("common.enableFastPB", value))
		enabled := value == "true"
		retrieve := &internalpb.RetrieveResults{}
		search := &schemapb.SearchResultData{}
		retrieveErr := (releaseCodec{}).Unmarshal(mem.BufferSlice{mem.SliceBuffer(invalidResultString())}, retrieve)
		searchErr := UnmarshalSearchResultData(invalidResultString(), search)
		if enabled {
			require.NoError(t, retrieveErr)
			require.NoError(t, searchErr)
			require.Equal(t, []string{string([]byte{0xff})}, retrieve.ChannelIDsRetrieved)
			require.Equal(t, []string{string([]byte{0xff})}, search.OutputFields)
		} else {
			require.Error(t, retrieveErr)
			require.Error(t, searchErr)
		}
	}
}

func TestFastPBRuntimeAllRPCDispatch(t *testing.T) {
	params := initFastPBTestParams(t)
	selected := errors.New("fast decoder selected")
	calls := 0
	patch := mockey.Mock(fastpb.TryUnmarshal).To(func(v any, data []byte) (bool, error) {
		calls++
		switch v.(type) {
		case *internalpb.RetrieveResults, *milvuspb.InsertRequest, *milvuspb.UpsertRequest:
			return true, selected
		default:
			return false, nil
		}
	}).Build()
	defer patch.UnPatch()
	for _, value := range []string{"true", "false", "true"} {
		require.NoError(t, params.Save("common.enableFastPB", value))
		for _, message := range []proto.Message{&internalpb.RetrieveResults{}, &milvuspb.InsertRequest{}, &milvuspb.UpsertRequest{}} {
			before := calls
			err := (releaseCodec{}).Unmarshal(nil, message)
			if value == "true" {
				require.ErrorIs(t, err, selected)
				require.Equal(t, before+1, calls)
			} else {
				require.NoError(t, err)
				require.Equal(t, before, calls)
			}
		}
		require.NoError(t, (releaseCodec{}).Unmarshal(nil, &commonpb.Status{}))
	}
}
