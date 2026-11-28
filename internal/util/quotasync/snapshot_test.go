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

package quotasync

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
)

func TestQuotaPolicySemanticEquality(t *testing.T) {
	a := &proxypb.Limiter{Rates: []*internalpb.Rate{{Rt: internalpb.RateType_DMLInsert, R: 10}, {Rt: internalpb.RateType_DMLDelete, R: 20}}}
	b := &proxypb.Limiter{Rates: []*internalpb.Rate{{Rt: internalpb.RateType_DMLDelete, R: 20}, {Rt: internalpb.RateType_DMLInsert, R: 10}}}
	key := Key{Scope: internalpb.RateScope_Cluster}
	publisher := &Publisher{}
	require.True(t, publisher.Publish(1, map[Key]*proxypb.Limiter{key: Canonical(a)}, nil))
	version := publisher.Version
	require.False(t, publisher.PublishPatch(1, map[Key]*proxypb.Limiter{key: Canonical(b)}, nil, nil))
	require.Equal(t, version, publisher.Version)
	require.True(t, publisher.PublishPatch(2, nil, nil, nil))
	require.Equal(t, Version{Epoch: 2, Seq: 1}, publisher.Version)
	require.Nil(t, publisher.Delta)
}

func BenchmarkQuotaFullEncoding(b *testing.B) {
	for _, count := range []int{1000, 1050104} {
		for _, finite := range []bool{false, true} {
			b.Run(fmt.Sprintf("collections=%d/finite=%t", count, finite), func(b *testing.B) {
				policy := &proxypb.Limiter{}
				if finite {
					for _, rt := range []internalpb.RateType{internalpb.RateType_DMLInsert, internalpb.RateType_DMLDelete, internalpb.RateType_DMLBulkLoad, internalpb.RateType_DQLSearch, internalpb.RateType_DQLQuery, internalpb.RateType_DDLFlush} {
						policy.Rates = append(policy.Rates, &internalpb.Rate{Rt: rt, R: 100})
					}
				}
				publisher := &Publisher{Version: Version{1, 1}, Policies: map[Key]*proxypb.Limiter{{Scope: internalpb.RateScope_Cluster}: {}, {Scope: internalpb.RateScope_Database, DB: 1}: {}}}
				for i := 0; i < count; i++ {
					publisher.Policies[Key{Scope: internalpb.RateScope_Collection, DB: 1, Collection: 450000000000000000 + int64(i)}] = policy
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					full := publisher.Full()
					encoded, err := proto.Marshal(full)
					if err != nil {
						b.Fatal(err)
					}
					b.ReportMetric(float64(len(encoded)), "payload-B")
					decoded := &proxypb.SetRatesRequest{}
					if err := proto.Unmarshal(encoded, decoded); err != nil {
						b.Fatal(err)
					}
					if len(decoded.RootLimiter.Children[1].Children) != count {
						b.Fatal("incomplete full snapshot")
					}
				}
			})
		}
	}
}
