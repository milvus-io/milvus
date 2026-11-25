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

package dml

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestUpdateTaskType(t *testing.T) {
	msgType := (&UpdateTask{}).Type()
	require.Equal(t, commonpb.MsgType_Update, msgType)
	require.Equal(t, "Update", msgType.String())
}

func TestBuildUpdateMessagesMaxInsertSize(t *testing.T) {
	paramtable.Init()
	p := paramtable.Get()
	set := func(t *testing.T, item *paramtable.ParamItem, value string) {
		old := item.SwapTempValue(value)
		t.Cleanup(func() { item.SwapTempValue(old) })
	}
	set(t, &p.MQCfg.Type, "pulsar")
	set(t, &p.PulsarCfg.MessageReserveSize, "1024")
	set(t, &p.PulsarCfg.MaxMessageSize, "2048")
	set(t, &p.QuotaConfig.MaxInsertSize, "-1")

	keys := make([]int64, 256)
	for i := range keys {
		keys[i] = int64(i)
	}
	ids := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: keys}}}
	value := &schemapb.ValueField{Data: &schemapb.ValueField_StringData{StringData: strings.Repeat("v", 256)}}
	assignments := []*messagespb.UpdateAssignment{{
		FieldId: 101,
		Literal: &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_ScalarValue{ScalarValue: value}},
	}}
	build := func(channels []string) ([]message.MutableMessage, error) {
		return buildUpdateMessages(10, 7, -1, channels, ids, assignments, nil)
	}

	for _, tc := range []struct {
		name     string
		split    bool
		channels []string
	}{
		{"across channels", false, []string{"ch0", "ch1"}},
		{"within one channel", true, []string{"ch0"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			set(t, &p.ProxyCfg.SplitChunkProxy, strconv.FormatBool(tc.split))
			msgs, err := build(tc.channels)
			require.NoError(t, err)
			require.Greater(t, len(msgs), 1)
			total := 0
			for _, msg := range msgs {
				total += proto.Size(msg.IntoMessageProto())
			}

			set(t, &p.QuotaConfig.MaxInsertSize, strconv.Itoa(total))
			atLimit, err := build(tc.channels)
			require.NoError(t, err)
			require.Len(t, atLimit, len(msgs))

			for _, limit := range []int{total - 1, 0} {
				set(t, &p.QuotaConfig.MaxInsertSize, strconv.Itoa(limit))
				overLimit, err := build(tc.channels)
				require.ErrorIs(t, err, merr.ErrParameterTooLarge)
				require.Nil(t, overLimit)
			}
		})
	}

	t.Run("single message limit with unlimited total", func(t *testing.T) {
		set(t, &p.ProxyCfg.SplitChunkProxy, "true")
		set(t, &p.PulsarCfg.MaxMessageSize, "1025")
		msgs, err := build([]string{"ch0"})
		require.ErrorIs(t, err, merr.ErrParameterTooLarge)
		require.Nil(t, msgs)
	})
}
