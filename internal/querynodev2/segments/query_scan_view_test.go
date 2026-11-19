// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package segments

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/internal/util/function/grep/native"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestQueryScanNativeSegmentInvalid(t *testing.T) {
	for name, s := range map[string]Segment{
		"nil": nil, "unsupported": &L0Segment{},
		"nil local": (*LocalSegment)(nil), "nil sealed": (*sealedReadState)(nil), "nil growing": (*viewQueryGrowingSegment)(nil),
		"empty local": &LocalSegment{}, "empty sealed": &sealedReadState{}, "empty growing": &viewQueryGrowingSegment{},
	} {
		t.Run(name, func(t *testing.T) {
			handle, _, err := queryScanNativeSegment(s)
			require.Nil(t, handle)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
		})
	}
}

// Exercise the real chain, including matching-row selection, up to native
// identity retrieval. The parent deliberately points at a different snapshot.
func TestQueryScanUsesQueryViewNativeHandle(t *testing.T) {
	paramtable.Init()
	for _, kind := range []string{"local", "sealed", "growing"} {
		for _, outcome := range []string{"empty", "scan error", "identity error"} {
			t.Run(kind+"/"+outcome, func(t *testing.T) {
				mockey.PatchConvey("retain the operation's native snapshot", t, func() {
					selected := &mock_segcore.MockCSegment{}
					latest := &mock_segcore.MockCSegment{}
					parent := &LocalSegment{baseSegment: baseSegment{segmentType: SegmentTypeSealed}, csegment: latest}
					read := &sealedReadState{Segment: parent, parent: parent, native: selected}
					read.refs.Store(1)
					var s Segment
					switch kind {
					case "local":
						s = &LocalSegment{baseSegment: baseSegment{segmentType: SegmentTypeSealed}, csegment: selected}
					case "sealed":
						s = read
					case "growing":
						s = NewGrowingSegmentForViewQuery(ViewQueryGrowingSegmentInfo{}, selected)
					}
					h, typ, err := queryScanNativeSegment(s)
					require.NoError(t, err)
					require.Same(t, selected, h)
					expectedType := SegmentTypeSealed
					if kind == "growing" {
						expectedType = SegmentTypeGrowing
					}
					require.Equal(t, expectedType, typ)
					pool := conc.NewPool[any](1)
					defer pool.Release()
					mockey.Mock(GetChainPool).Return(pool).Build()
					alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
					defer alloc.AssertSize(t, 0)
					scanCalls, identityCalls := 0, 0
					mockey.Mock(segcore.NewGrepSegment).To(func(actual segcore.CSegment) native.Segment {
						require.Same(t, selected, actual)
						return grepScanFunc(func(ctx context.Context, offsets *array.Int64, req native.ScanRequest) (*native.ScanResult, error) {
							scanCalls++
							require.Equal(t, []int64{7, 11}, offsets.Int64Values())
							require.EqualValues(t, 101, req.FieldID)
							if outcome == "scan error" {
								return nil, context.Canceled
							}
							builder := array.NewBuilder(alloc, native.GrepDetailsType())
							defer builder.Release()
							var positions []int
							if outcome == "identity error" {
								positions = []int{1}
								builder.(*array.BinaryBuilder).Append([]byte("[]"))
							}
							return &native.ScanResult{Positions: positions, Details: builder.NewArray()}, nil
						})
					}).Build()
					plan := &segcore.RetrievePlan{}
					mockey.Mock(segcore.RetrieveIdentitiesByOffsets).To(func(ctx context.Context, actual segcore.CSegment, p *segcore.RetrievePlanWithOffsets, take bool) (*segcore.RetrieveArrowResult, error) {
						identityCalls++
						require.Same(t, selected, actual)
						require.Same(t, plan, p.RetrievePlan)
						require.Equal(t, []int64{11}, p.Offsets)
						return nil, context.Canceled
					}).Build()
					repr, err := chain.ProtoChainToRepr(&schemapb.FunctionChain{
						Stage: schemapb.FunctionChainStage_FunctionChainStageQueryScan,
						Ops:   []*schemapb.FunctionChainOp{{Op: "grep", Inputs: []string{"text"}, Outputs: []string{"matches"}, Params: map[string]*schemapb.FunctionParamValue{"arguments": {Value: &schemapb.FunctionParamValue_StringValue{StringValue: "-e timeout"}}}}},
					})
					require.NoError(t, err)
					record := scanIdentityRecord(alloc, []int64{7, 11}, []int64{1, 2}, []int64{10, 20}, false)
					defer record.Release()
					result, err := runQueryScanSegment(context.Background(), 101, 10, repr, RetrieveSegmentResult{Segment: s, Record: record}, plan)
					if outcome == "empty" {
						require.NoError(t, err)
						require.NotNil(t, result)
						require.Zero(t, result.NumRows())
						result.Release()
					} else {
						require.ErrorIs(t, err, context.Canceled)
						require.Nil(t, result)
					}
					require.Equal(t, 1, scanCalls)
					expectedIdentities := 0
					if outcome == "identity error" {
						expectedIdentities = 1
					}
					require.Equal(t, expectedIdentities, identityCalls)
					require.EqualValues(t, 1, read.refs.Load(), "QueryScan must not release the query's read state")
					selected.AssertNotCalled(t, "Release")
					latest.AssertNotCalled(t, "Release")
				})
			})
		}
	}
}
