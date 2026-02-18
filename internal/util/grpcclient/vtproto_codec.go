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
	"fmt"

	"google.golang.org/grpc/encoding"
	"google.golang.org/protobuf/proto"
)

// vtProtoMarshaler is implemented by vtprotobuf generated types.
type vtProtoMarshaler interface {
	MarshalVT() ([]byte, error)
}

// vtProtoUnmarshaler is implemented by vtprotobuf generated types.
type vtProtoUnmarshaler interface {
	UnmarshalVT([]byte) error
}

// gogoMarshaler matches gogo/protobuf's proto.Marshaler interface
// (used by etcd and other dependencies that haven't migrated to google protobuf v2).
type gogoMarshaler interface {
	Marshal() ([]byte, error)
}

// gogoUnmarshaler matches gogo/protobuf's proto.Unmarshaler interface.
type gogoUnmarshaler interface {
	Unmarshal([]byte) error
}

// vtprotoCodec is a gRPC codec that preferentially uses vtprotobuf's generated
// MarshalVT/UnmarshalVT methods for Milvus proto types, and falls back to
// standard proto or gogoproto for third-party types (e.g. etcd, gRPC health).
// It replaces the default gRPC proto codec via Name() == "proto".
type vtprotoCodec struct{}

func (vtprotoCodec) Name() string { return "proto" }

func (vtprotoCodec) Marshal(v interface{}) ([]byte, error) {
	if m, ok := v.(vtProtoMarshaler); ok {
		return m.MarshalVT()
	}
	if m, ok := v.(proto.Message); ok {
		return proto.Marshal(m)
	}
	if m, ok := v.(gogoMarshaler); ok {
		return m.Marshal()
	}
	return nil, fmt.Errorf("vtprotoCodec: %T is not a proto message", v)
}

func (vtprotoCodec) Unmarshal(data []byte, v interface{}) error {
	if m, ok := v.(vtProtoUnmarshaler); ok {
		return m.UnmarshalVT(data)
	}
	if m, ok := v.(proto.Message); ok {
		return proto.Unmarshal(data, m)
	}
	if m, ok := v.(gogoUnmarshaler); ok {
		return m.Unmarshal(data)
	}
	return fmt.Errorf("vtprotoCodec: %T is not a proto message", v)
}

func init() {
	encoding.RegisterCodec(vtprotoCodec{})
}
