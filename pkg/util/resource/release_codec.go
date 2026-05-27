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
	"fmt"

	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// releaseCodec is a gRPC CodecV2 that wraps the standard proto codec.
// After marshaling a MsgPinnable message it calls MsgPins.Release, which
// triggers any cleanup registered via MsgPins.Pin — for example, freeing
// C/CGO memory that was referenced zero-copy from a proto field.
//
// Only messages that implement MsgPinnable incur a map lookup; all others
// are marshaled with zero additional overhead.
//
// It is registered globally via init() so any gRPC server or client that
// imports this package (or any package that imports pkg/util/resource)
// automatically uses it.
type releaseCodec struct{}

func (releaseCodec) Name() string { return "proto" }

func (releaseCodec) Marshal(v any) (mem.BufferSlice, error) {
	msg := messageV2Of(v)
	if msg == nil {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("releaseCodec: %T does not implement proto.Message", v))
	}

	// Only release messages that opt in via MsgPinnable. This avoids a map
	// lookup for the vast majority of gRPC messages that carry no C-backed memory.
	if _, ok := v.(MsgPinnable); ok {
		defer MsgPins.Release(v)
	}

	size := proto.Size(msg)
	marshalOptions := proto.MarshalOptions{UseCachedSize: true}
	var out mem.BufferSlice

	if mem.IsBelowBufferPoolingThreshold(size) {
		buf, err := marshalOptions.Marshal(msg)
		if err != nil {
			return nil, err
		}
		out = mem.BufferSlice{mem.SliceBuffer(buf)}
	} else {
		pool := mem.DefaultBufferPool()
		pbuf := pool.Get(size)
		if buf, err := marshalOptions.MarshalAppend((*pbuf)[:0], msg); err != nil {
			pool.Put(pbuf)
			return nil, err
		} else {
			*pbuf = buf
		}
		out = mem.BufferSlice{mem.NewBuffer(pbuf, pool)}
	}

	return out, nil
}

func (releaseCodec) Unmarshal(data mem.BufferSlice, v any) error {
	msg := messageV2Of(v)
	if msg == nil {
		return merr.WrapErrServiceInternal(fmt.Sprintf("releaseCodec: %T does not implement proto.Message", v))
	}

	buf := data.MaterializeToBuffer(mem.DefaultBufferPool())
	defer buf.Free()
	return proto.Unmarshal(buf.ReadOnlyData(), msg)
}

// messageV2Of converts v to a proto.Message, handling both google protobuf v2
// messages and legacy v1 messages (e.g., gogo-protobuf used by etcd).
func messageV2Of(v any) proto.Message {
	switch v := v.(type) {
	case protoadapt.MessageV1:
		return protoadapt.MessageV2Of(v)
	case protoadapt.MessageV2:
		return v
	}
	return nil
}

func init() {
	encoding.RegisterCodecV2(releaseCodec{})
}
