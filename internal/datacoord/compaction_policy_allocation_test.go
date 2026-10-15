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

package datacoord

import (
	"context"
	"strconv"
	"testing"

	"github.com/blang/semver/v4"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type compactionCheckBenchmarkAllocator struct{}

func (*compactionCheckBenchmarkAllocator) AllocID(context.Context) (int64, error) {
	return 1, nil
}

func (*compactionCheckBenchmarkAllocator) AllocTimestamp(context.Context) (uint64, error) {
	return 1, nil
}

func (*compactionCheckBenchmarkAllocator) AllocN(n int64) (int64, int64, error) {
	return 1, n + 1, nil
}

// Benchmark the steady-state full scan: every collection has one flushed V3
// segment whose schema/storage versions already match the desired versions.
// The allocator is local, so its real RPC allocation cost is intentionally not
// included in this benchmark.
func BenchmarkCompactionPolicyNoCandidates(b *testing.B) {
	paramtable.Init()
	params := paramtable.Get()
	for _, setting := range []struct {
		param *paramtable.ParamItem
		value string
	}{
		{&params.CommonCfg.UseLoonFFI, "true"},
		{&params.DataCoordCfg.StorageVersionCompactionEnabled, "true"},
		{&params.DataCoordCfg.StorageFormatCompactionEnabled, "false"},
	} {
		key, previous := setting.param.Key, setting.param.GetValue()
		params.Save(key, setting.value)
		b.Cleanup(func() { params.Save(key, previous) })
	}

	for _, policyName := range []string{"schema", "storage"} {
		for _, collectionCount := range []int{1000, 10000} {
			b.Run(policyName+"/"+strconv.Itoa(collectionCount), func(b *testing.B) {
				m := &meta{
					ctx:         context.Background(),
					collections: typeutil.NewConcurrentMap[int64, *collectionInfo](),
					segments:    NewSegmentsInfo(),
				}
				schema := newBumpSchemaVersionTestCollection(1, 2).Schema
				for fieldID := int64(102); fieldID < 116; fieldID++ {
					schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
						FieldID: fieldID, Name: "field_" + strconv.FormatInt(fieldID, 10), DataType: schemapb.DataType_Int64,
					})
				}
				for id := int64(1); id <= int64(collectionCount); id++ {
					m.collections.Insert(id, &collectionInfo{ID: id, Schema: schema})
					m.segments.SetSegment(id, newBumpSchemaVersionTestSegment(id, id, 2, storage.StorageV3, "manifest"))
				}
				alloc := &compactionCheckBenchmarkAllocator{}
				handler := &ServerHandler{s: &Server{meta: m}}
				var policy CompactionPolicy
				if policyName == "schema" {
					policy = newBumpSchemaVersionPolicy(m, alloc, handler)
				} else {
					versionManager := NewMockVersionManager(b)
					versionManager.EXPECT().GetMinimalSessionVer().Return(semver.MustParse("3.0.0"))
					policy = newStorageVersionUpgradePolicy(m, alloc, handler, versionManager)
				}
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					events, err := policy.Trigger(ctx)
					if err != nil {
						b.Fatal(err)
					}
					for _, views := range events {
						if len(views) != 0 {
							b.Fatal("up-to-date segments must not produce compaction views")
						}
					}
				}
			})
		}
	}
}
