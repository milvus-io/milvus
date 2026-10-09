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
	"bytes"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"

	milvuspb "github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	schemapb "github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// Benchmarks use optimized compilation intentionally; mockey correctness tests
// run separately with -gcflags='all=-N -l'. Both sides use the real releaseCodec,
// with identical pool size classes and unchanged protobuf/fastPB behavior.
func BenchmarkReleaseCodecDirtyPool(b *testing.B) {
	for _, size := range []int{64 << 10, 1 << 20, 4 << 20} {
		values := make([]string, 64)
		for i := range values {
			values[i] = strings.Repeat("x", size/len(values))
		}
		field := &schemapb.FieldData{Type: schemapb.DataType_VarChar, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: values}}}}}
		messages := []struct {
			name  string
			value proto.Message
		}{
			{"RetrieveResults_varchar", &internalpb.RetrieveResults{FieldsData: []*schemapb.FieldData{field}}},
			{"SearchResults_blob", &internalpb.SearchResults{SlicedBlob: bytes.Repeat([]byte{0x42}, size)}},
			{"InsertRequest_varchar", &milvuspb.InsertRequest{FieldsData: []*schemapb.FieldData{field}, NumRows: 64}},
		}
		for _, message := range messages {
			wire, err := proto.Marshal(message.value)
			if err != nil {
				b.Fatal(err)
			}
			for _, kind := range []string{"default", "dirty"} {
				for _, operation := range []string{"Marshal", "Unmarshal_fragmented", "Unmarshal_single"} {
					b.Run(fmt.Sprintf("%s/%dB/%s/%s", message.name, size, kind, operation), func(b *testing.B) {
						var pool mem.BufferPool
						if kind == "default" {
							pool, err = mem.NewBinaryTieredBufferPool(8, 12, 14, 15, 20)
							if err != nil {
								b.Fatal(err)
							}
						} else {
							pool = new(codecBufferPool)
						}
						codec := releaseCodec{bufferPool: pool}
						chunk := 16384
						if operation == "Unmarshal_single" {
							chunk = len(wire)
						}
						input := fragments(wire, chunk)
						// Warm the pool through the same production operation before measuring.
						for i := 0; i < 2; i++ {
							if operation == "Marshal" {
								out, e := codec.Marshal(message.value)
								if e != nil {
									b.Fatal(e)
								}
								out.Free()
							} else {
								target := message.value.ProtoReflect().New().Interface()
								if e := codec.Unmarshal(input, target); e != nil {
									b.Fatal(e)
								}
							}
						}
						b.SetBytes(int64(len(wire)))
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							if operation == "Marshal" {
								out, e := codec.Marshal(message.value)
								if e != nil {
									b.Fatal(e)
								}
								out.Free()
							} else {
								target := message.value.ProtoReflect().New().Interface()
								if e := codec.Unmarshal(input, target); e != nil {
									b.Fatal(e)
								}
							}
						}
					})
				}
			}
		}
	}
}
