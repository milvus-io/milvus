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

package rootcoord

import (
	"context"
	"strconv"
	"testing"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
)

type collectionGCBenchmarkCatalog struct {
	metastore.RootCoordCatalog
}

func (collectionGCBenchmarkCatalog) DropCollection(context.Context, *model.Collection, Timestamp) error {
	return nil
}

func (collectionGCBenchmarkCatalog) DeleteGrantByCollectionName(context.Context, string, string, string) error {
	return nil
}

// Measure the local GC critical path independently of etcd and logging latency.
// The same benchmark can run against the parent revision without modification.
func BenchmarkMetaTableRemoveCollection(b *testing.B) {
	level := mlog.GetLevel()
	mlog.SetLevel(mlog.ErrorLevel)
	b.Cleanup(func() { mlog.SetLevel(level) })
	for _, count := range []int{1000, 10000, 100000, 1000000} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			meta := &MetaTable{
				catalog: collectionGCBenchmarkCatalog{}, names: newNameDb(), aliases: newNameDb(),
				collID2Meta: make(map[int64]*model.Collection),
			}
			for i := 1; i <= count; i++ {
				meta.names.insert(util.DefaultDBName, strconv.Itoa(i), int64(i))
			}
			coll := &model.Collection{
				CollectionID: -1, DBID: util.DefaultDBID, DBName: util.DefaultDBName,
				Name: "dropping", State: pb.CollectionState_CollectionDropping,
			}
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				meta.names.insert(coll.DBName, coll.Name, coll.CollectionID)
				meta.collID2Meta[coll.CollectionID] = coll
				if err := meta.RemoveCollection(ctx, coll.CollectionID, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
