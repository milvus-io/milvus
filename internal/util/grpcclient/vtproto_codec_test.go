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

package grpcclient

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

// mockGogoMessage simulates a gogoproto message (e.g. etcd's mvccpb.KeyValue)
// that only implements gogoMarshaler / gogoUnmarshaler, not vtProtoMarshaler
// or proto.Message.
type mockGogoMessage struct {
	Value string
}

func (m *mockGogoMessage) Marshal() ([]byte, error) {
	return []byte(m.Value), nil
}

func (m *mockGogoMessage) Unmarshal(data []byte) error {
	m.Value = string(data)
	return nil
}

// mockGogoMessageErr always fails on Marshal/Unmarshal.
type mockGogoMessageErr struct{}

func (m *mockGogoMessageErr) Marshal() ([]byte, error) {
	return nil, errors.New("marshal error")
}

func (m *mockGogoMessageErr) Unmarshal(_ []byte) error {
	return errors.New("unmarshal error")
}

// unknownType implements none of the codec interfaces.
type unknownType struct{}

var codec = vtprotoCodec{}

// --- Name ---

func TestVtprotoCodec_Name(t *testing.T) {
	assert.Equal(t, "proto", codec.Name())
}

// --- vtProtoMarshaler path (Milvus internal proto types) ---

func TestVtprotoCodec_VtProto_RoundTrip(t *testing.T) {
	original := &internalpb.SearchResults{
		NumQueries: 5,
		TopK:       10,
		MetricType: "L2",
	}

	data, err := codec.Marshal(original)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	decoded := &internalpb.SearchResults{}
	err = codec.Unmarshal(data, decoded)
	require.NoError(t, err)
	assert.Equal(t, original.NumQueries, decoded.NumQueries)
	assert.Equal(t, original.TopK, decoded.TopK)
	assert.Equal(t, original.MetricType, decoded.MetricType)
}

// --- proto.Message path (standard google protobuf v2, no vtproto generated) ---

func TestVtprotoCodec_ProtoMessage_RoundTrip(t *testing.T) {
	// timestamppb.Timestamp is a google protobuf well-known type that does NOT
	// have vtproto generated code, so it falls through to the proto.Message path.
	original := timestamppb.Now()

	data, err := codec.Marshal(original)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	decoded := &timestamppb.Timestamp{}
	err = codec.Unmarshal(data, decoded)
	require.NoError(t, err)
	assert.Equal(t, original.Seconds, decoded.Seconds)
	assert.Equal(t, original.Nanos, decoded.Nanos)
}

// --- gogoMarshaler path (etcd-style gogoproto types) ---

func TestVtprotoCodec_Gogo_RoundTrip(t *testing.T) {
	original := &mockGogoMessage{Value: "hello-etcd"}

	data, err := codec.Marshal(original)
	require.NoError(t, err)
	assert.Equal(t, []byte("hello-etcd"), data)

	decoded := &mockGogoMessage{}
	err = codec.Unmarshal(data, decoded)
	require.NoError(t, err)
	assert.Equal(t, original.Value, decoded.Value)
}

func TestVtprotoCodec_Gogo_MarshalError(t *testing.T) {
	_, err := codec.Marshal(&mockGogoMessageErr{})
	assert.Error(t, err)
}

func TestVtprotoCodec_Gogo_UnmarshalError(t *testing.T) {
	err := codec.Unmarshal([]byte("data"), &mockGogoMessageErr{})
	assert.Error(t, err)
}

// --- Unknown type fallback ---

func TestVtprotoCodec_UnknownType_Marshal(t *testing.T) {
	_, err := codec.Marshal(&unknownType{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "vtprotoCodec")
}

func TestVtprotoCodec_UnknownType_Unmarshal(t *testing.T) {
	err := codec.Unmarshal([]byte("data"), &unknownType{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "vtprotoCodec")
}
