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
	"io"
	"os"
	"os/exec"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/fastpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestFastPBDefaultBeforeParamtableInit(t *testing.T) {
	if os.Getenv("MILVUS_TEST_FASTPB_PREINIT") != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestFastPBDefaultBeforeParamtableInit$") // #nosec G204 G702 -- re-executes this test binary with fixed arguments.
		cmd.Env = append(os.Environ(), "MILVUS_TEST_FASTPB_PREINIT=1")
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", out)
		return
	}
	require.Nil(t, paramtable.GetIfInitialized())
	require.True(t, fastPBEnabled())
	result := &schemapb.SearchResultData{}
	require.NoError(t, UnmarshalSearchResultData(nil, result))
	require.NoError(t, (releaseCodec{}).Unmarshal(mem.BufferSlice{mem.SliceBuffer(nil)}, &internalpb.RetrieveResults{}))
	require.Nil(t, paramtable.GetIfInitialized(), "decoding must not initialize configuration")
}

func TestFastPBToggleRPCAndSearchResults(t *testing.T) {
	bt := paramtable.NewBaseTable(paramtable.SkipRemote(true), paramtable.SkipEnv(true))
	t.Cleanup(bt.Manager().Close)
	require.NoError(t, bt.Save("localStorage.path", t.TempDir()))
	paramtable.InitWithBaseTable(bt)
	params := paramtable.Get()
	key := params.CommonCfg.EnableFastPB.Key
	original := params.CommonCfg.EnableFastPB.GetValue()
	t.Cleanup(func() { require.NoError(t, params.Save(key, original)) })
	codec := releaseCodec{}
	// Trusted internal strings deliberately bypass UTF-8 checks only on fastpb.
	invalidString := protowire.AppendString(protowire.AppendTag(nil, 7, protowire.BytesType), string([]byte{0xff}))
	field := protowire.AppendString(protowire.AppendTag(nil, 2, protowire.BytesType), string([]byte{0xff}))
	invalidSearch := protowire.AppendBytes(protowire.AppendTag(nil, 3, protowire.BytesType), field)
	invalidClient := protowire.AppendString(protowire.AppendTag(nil, 3, protowire.BytesType), string([]byte{0xff}))
	validSearch := &schemapb.SearchResultData{NumQueries: 1, FieldsData: []*schemapb.FieldData{{FieldName: "owned"}}}
	validWire, err := proto.Marshal(validSearch)
	require.NoError(t, err)
	for _, enabled := range []string{"true", "false", "true"} {
		require.NoError(t, params.Save(key, enabled))
		wantFast := enabled == "true"
		rr := &internalpb.RetrieveResults{}
		err = codec.Unmarshal(mem.BufferSlice{mem.SliceBuffer(invalidString)}, rr)
		require.Equal(t, wantFast, err == nil, "retrieve dispatch enabled=%s", enabled)
		result := &schemapb.SearchResultData{}
		err = UnmarshalSearchResultData(invalidSearch, result)
		require.Equal(t, wantFast, err == nil, "search dispatch enabled=%s", enabled)
		for _, msg := range []proto.Message{&milvuspb.InsertRequest{}, &milvuspb.UpsertRequest{}} {
			require.Error(t, codec.Unmarshal(mem.BufferSlice{mem.SliceBuffer(invalidClient)}, msg), "client UTF-8 must be rejected enabled=%s", enabled)
		}
		// Reusing the source must not invalidate decoded output in either mode.
		wire := append([]byte(nil), validWire...)
		result = &schemapb.SearchResultData{}
		require.NoError(t, UnmarshalSearchResultData(wire, result))
		for i := range wire {
			wire[i] = 0
		}
		require.True(t, proto.Equal(validSearch, result))
		// Marshal's message pin lifecycle is independent of the decoder selection.
		pinned := &internalpb.SearchResults{}
		released := 0
		MsgPins.Pin(pinned, func() { released++ })
		out, err := codec.Marshal(pinned)
		require.NoError(t, err)
		require.Equal(t, 1, released)
		require.False(t, MsgPins.HasPinned(pinned))
		out.Free()
	}
}

// The ingress decoders both validate UTF-8, so an injected result distinguishes
// actual fast-path dispatch from coincidentally equal official decoding.
func TestFastPBToggleCodecDispatchAllHotMessages(t *testing.T) {
	bt := paramtable.NewBaseTable(paramtable.SkipRemote(true), paramtable.SkipEnv(true))
	t.Cleanup(bt.Manager().Close)
	require.NoError(t, bt.Save("localStorage.path", t.TempDir()))
	paramtable.InitWithBaseTable(bt)
	params := paramtable.Get()
	key := params.CommonCfg.EnableFastPB.Key
	original := params.CommonCfg.EnableFastPB.GetValue()
	t.Cleanup(func() { require.NoError(t, params.Save(key, original)) })
	patch := mockey.Mock(fastpb.TryUnmarshal).Return(true, io.ErrUnexpectedEOF).Build()
	defer patch.UnPatch()
	for _, enabled := range []string{"true", "false", "true"} {
		require.NoError(t, params.Save(key, enabled))
		for _, msg := range []proto.Message{&internalpb.RetrieveResults{}, &milvuspb.InsertRequest{}, &milvuspb.UpsertRequest{}} {
			err := (releaseCodec{}).Unmarshal(mem.BufferSlice{mem.SliceBuffer(nil)}, msg)
			if enabled == "true" {
				require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			} else {
				require.NoError(t, err)
			}
		}
	}
}
