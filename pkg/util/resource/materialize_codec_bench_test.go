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
	"testing"

	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// stdProtoCodec is releaseCodec's generic path (pooled marshal buffer,
// pooled materialize + proto.Unmarshal), i.e. what MaterializeResults used
// before the dedicated path.
type stdProtoCodec struct{ releaseCodec }

func (c stdProtoCodec) Marshal(v any) (mem.BufferSlice, error) {
	msg := v.(proto.Message)
	pool := c.pool()
	pbuf := pool.Get(proto.Size(msg))
	buf, err := proto.MarshalOptions{UseCachedSize: true}.MarshalAppend((*pbuf)[:0], msg)
	if err != nil {
		return nil, err
	}
	*pbuf = buf
	return mem.BufferSlice{mem.NewBuffer(pbuf, pool)}, nil
}

func (c stdProtoCodec) Unmarshal(data mem.BufferSlice, v any) error {
	buf := data.MaterializeToBuffer(c.pool())
	defer buf.Free()
	return proto.Unmarshal(buf.ReadOnlyData(), v.(proto.Message))
}

// BenchmarkMaterializeCodec measures, for a MaterializeResults with 8 body
// entries: the delegator's relay (Unmarshal of the 16 KB-fragmented wire,
// then Marshal of the received message), and one hop from a Go-owned body
// (Marshal on the sender, Unmarshal of the fragmented wire on the receiver).
func BenchmarkMaterializeCodec(b *testing.B) {
	type codecV2 interface {
		Marshal(v any) (mem.BufferSlice, error)
		Unmarshal(data mem.BufferSlice, v any) error
	}
	for _, total := range []int{40 << 10, 400 << 10, 4 << 20} {
		msg := &internalpb.MaterializeResults{Header: testMaterializeHeader()}
		for i := 0; i < 8; i++ {
			msg.Body = append(msg.Body, patternBytes(total/8, byte(i)))
		}
		wire, err := proto.Marshal(msg)
		if err != nil {
			b.Fatal(err)
		}
		recv := splitBufferSlice(wire, 16<<10)
		// The delegator's relay: receive the fragmented reply, send it on.
		for _, c := range []struct {
			name  string
			codec codecV2
		}{{"std", stdProtoCodec{}}, {"nocopy", releaseCodec{}}} {
			b.Run(fmt.Sprintf("%dKB/relay/%s", total>>10, c.name), func(b *testing.B) {
				b.SetBytes(int64(len(wire)))
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					got := &internalpb.MaterializeResults{}
					if err := c.codec.Unmarshal(recv, got); err != nil {
						b.Fatal(err)
					}
					out, err := c.codec.Marshal(got)
					if err != nil {
						b.Fatal(err)
					}
					if out.Len() != len(wire) {
						b.Fatal("bad relay")
					}
					out.Free()
				}
			})
		}
		for _, c := range []struct {
			name  string
			codec codecV2
		}{{"std", stdProtoCodec{}}, {"nocopy", releaseCodec{}}} {
			b.Run(fmt.Sprintf("%dKB/%s", total>>10, c.name), func(b *testing.B) {
				b.SetBytes(int64(len(wire)))
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					out, err := c.codec.Marshal(msg)
					if err != nil {
						b.Fatal(err)
					}
					// gRPC copies the payload into HTTP/2 frames before Free.
					var n int
					for _, buf := range out {
						n += len(buf.ReadOnlyData())
					}
					out.Free()
					got := &internalpb.MaterializeResults{}
					if err := c.codec.Unmarshal(recv, got); err != nil {
						b.Fatal(err)
					}
					body := TakeMaterializeBody(got)
					if n != len(wire) || body.Len() != 8 {
						b.Fatal("bad round trip")
					}
					body.Release()
				}
			})
		}
	}
}
