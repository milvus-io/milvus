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

package index

import (
	"context"
	"runtime"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// indexMemIndexes is the number of indexes the benchmark loads: one vector
// index per collection, all built with the same parameters.
const indexMemIndexes = 10000

// indexMemCatalog serves freshly decoded indexes to NewIndexMeta, as the KV
// catalog does.
type indexMemCatalog struct {
	metastore.DataCoordCatalog
	template []byte
}

func (c *indexMemCatalog) ListIndexes(context.Context) ([]*model.Index, error) {
	indexes := make([]*model.Index, indexMemIndexes)
	for i := range indexes {
		fieldIndex := &indexpb.FieldIndex{}
		if err := proto.Unmarshal(c.template, fieldIndex); err != nil {
			return nil, err
		}
		fieldIndex.IndexInfo.CollectionID += int64(i)
		fieldIndex.IndexInfo.IndexID += int64(i)
		indexes[i] = model.UnmarshalIndexModel(fieldIndex)
	}
	return indexes, nil
}

func indexMemHeapAlloc() uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

// BenchmarkIndexMetaMemory measures the heap DataCoord's IndexMeta keeps per
// index definition when many collections share the same index parameters.
// It loads indexMemIndexes indexes into a fresh IndexMeta per iteration and
// reports the heap they keep reachable, per index.
func BenchmarkIndexMetaMemory(b *testing.B) {
	paramtable.Init()
	kv := func(k, v string) *commonpb.KeyValuePair { return &commonpb.KeyValuePair{Key: k, Value: v} }
	template, err := proto.Marshal(&indexpb.FieldIndex{
		IndexInfo: &indexpb.IndexInfo{
			CollectionID: 454000000000000000,
			FieldID:      101,
			IndexName:    "vector_index",
			IndexID:      455000000000000000,
			TypeParams:   []*commonpb.KeyValuePair{kv("dim", "768")},
			IndexParams: []*commonpb.KeyValuePair{
				kv("index_type", "HNSW"), kv("metric_type", "COSINE"),
				kv("M", "30"), kv("efConstruction", "360"),
			},
			UserIndexParams: []*commonpb.KeyValuePair{kv("index_type", "AUTOINDEX"), kv("metric_type", "COSINE")},
			IsAutoIndex:     true,
		},
		CreateTime: 454000000000000000,
	})
	if err != nil {
		b.Fatal(err)
	}
	catalog := &indexMemCatalog{template: template}

	var retained float64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		before := indexMemHeapAlloc()
		b.StartTimer()
		m, err := NewIndexMeta(context.Background(), catalog, nil)
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		after := indexMemHeapAlloc()
		if got := len(m.Indexes); got != indexMemIndexes {
			b.Fatalf("meta holds %d collections, want %d", got, indexMemIndexes)
		}
		runtime.KeepAlive(m)
		retained += float64(int64(after)-int64(before)) / indexMemIndexes
		b.StartTimer()
	}
	b.StopTimer()
	b.ReportMetric(retained/float64(b.N), "retained-B/index")
}
