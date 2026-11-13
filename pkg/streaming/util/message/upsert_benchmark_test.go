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

package message

import (
	"crypto/aes"
	cryptocipher "crypto/cipher"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
)

func BenchmarkUpsertProjection(b *testing.B) {
	const rows = 1024
	body := &UpsertMessageBody{Rows: &InsertRows{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: make([]int64, rows)}}}, RowIdsEncoding: &InsertRows_RowIds{RowIds: &schemapb.LongArray{Data: make([]int64, rows)}}, Columns: []*schemapb.FieldData{{FieldId: 101, Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 128, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, rows*128)}}}}}}}}
	payload, err := proto.Marshal(body)
	if err != nil {
		b.Fatal(err)
	}
	block, err := aes.NewCipher(make([]byte, 32))
	if err != nil {
		b.Fatal(err)
	}
	aead, err := cryptocipher.NewGCM(block)
	if err != nil {
		b.Fatal(err)
	}
	nonce := make([]byte, aead.NonceSize())
	encrypted := aead.Seal(nil, nonce, payload, nil)
	for _, encryption := range []bool{false, true} {
		name := "plain"
		if encryption {
			name = "AES256GCM"
		}
		for _, projection := range []bool{false, true} {
			mode := "full"
			if projection {
				mode = "projection"
			}
			b.Run(name+"/"+mode, func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(payload)))
				b.ReportMetric(float64(len(payload)), "payload_bytes")
				for i := 0; i < b.N; i++ {
					data := payload
					if encryption {
						data, err = aead.Open(nil, nonce, encrypted, nil)
						if err != nil {
							b.Fatal(err)
						}
					}
					if projection {
						if _, err := decodeInsertOrUpsertProjection(data, true); err != nil {
							b.Fatal(err)
						}
					} else {
						if err := proto.Unmarshal(data, &UpsertMessageBody{}); err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		}
	}
}

func BenchmarkInsertOrUpsertProjectionKeys(b *testing.B) {
	const count = 4096
	for _, varchar := range []bool{false, true} {
		name := "Int64"
		keys := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: make([]int64, count)}}}
		if varchar {
			name = "VarChar"
			values := make([]string, count)
			for i := range values {
				values[i] = strings.Repeat("界", 40)
			}
			keys = &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: values}}}
		}
		for _, effects := range []bool{false, true} {
			body := &UpsertMessageBody{Rows: &InsertRows{PrimaryKeys: keys, RowIdsEncoding: &InsertRows_RowIds{RowIds: &schemapb.LongArray{Data: make([]int64, count)}}, PartitionIndexes: make([]uint32, count), RequestRowMapping: &RequestRowMapping{RowOffsets: make([]uint32, count)}}}
			for i := range body.Rows.GetRowIds().Data {
				body.Rows.GetRowIds().Data[i] = 1<<50 + int64(i)
				body.Rows.PartitionIndexes[i] = uint32(i % 8)
				body.Rows.RequestRowMapping.RowOffsets[i] = uint32(i)
			}
			if effects {
				body.DeleteEffects = []*messagespb.UpsertDeleteEffect{{KeySource: &messagespb.UpsertDeleteEffect_PrimaryKeys{PrimaryKeys: keys}, Scope: &messagespb.UpsertDeleteEffect_AllPartitions{AllPartitions: &messagespb.UpsertAllPartitions{}}}}
			}
			payload, err := proto.Marshal(body)
			if err != nil {
				b.Fatal(err)
			}
			b.Run(fmt.Sprintf("%s/effects_%t", name, effects), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(payload)))
				for i := 0; i < b.N; i++ {
					if _, err := decodeInsertOrUpsertProjection(payload, true); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
