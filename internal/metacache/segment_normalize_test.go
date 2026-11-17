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

package metacache

import (
	"maps"
	"strings"
	"testing"
	"unique"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec/specutil"
)

func TestNormalizeStats_NullCounts(t *testing.T) {
	mixed := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, DataType: schemapb.DataType_FloatVector},
		{FieldID: 102, DataType: schemapb.DataType_VarChar, Nullable: true},
	}}
	allNullable := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 101, DataType: schemapb.DataType_FloatVector, Nullable: true},
		{FieldID: 102, DataType: schemapb.DataType_VarChar, Nullable: true},
	}}
	withStruct := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 102, DataType: schemapb.DataType_VarChar, Nullable: true},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			FieldID: 1000,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 1001, DataType: schemapb.DataType_ArrayOfVector},
			},
		}},
	}
	withVectorArray := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 104, DataType: schemapb.DataType_ArrayOfVector},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			FieldID: 1000,
			Fields: []*schemapb.FieldSchema{
				{FieldID: 1001, DataType: schemapb.DataType_ArrayOfVector},
				{FieldID: 1002, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32},
			},
		}},
	}
	withFunctionOutput := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 102, DataType: schemapb.DataType_VarChar, Nullable: true},
		{FieldID: 103, DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
	}}

	tests := []struct {
		name     string
		stats    *datapb.Statistics
		schema   *schemapb.CollectionSchema
		want     map[int64]int64
		samePtr  bool
		wantNil  bool
		inputLen int
	}{
		{
			name:     "drops not-nullable keys only",
			stats:    &datapb.Statistics{NullCounts: map[int64]int64{100: 0, 101: 0, 102: 3}, DeleteNumRows: 7},
			schema:   mixed,
			want:     map[int64]int64{102: 3},
			inputLen: 3,
		},
		{
			name:     "all nullable keeps the same object",
			stats:    &datapb.Statistics{NullCounts: map[int64]int64{101: 0, 102: 3}},
			schema:   allNullable,
			want:     map[int64]int64{101: 0, 102: 3},
			samePtr:  true,
			inputLen: 2,
		},
		{
			name:     "all not-nullable empties the map to nil",
			stats:    &datapb.Statistics{NullCounts: map[int64]int64{100: 0, 101: 0}},
			schema:   mixed,
			want:     nil,
			inputLen: 2,
		},
		{
			name:     "not-nullable VectorArray struct sub-field is kept",
			stats:    &datapb.Statistics{NullCounts: map[int64]int64{102: 1, 1001: 0}},
			schema:   withStruct,
			want:     map[int64]int64{102: 1, 1001: 0},
			samePtr:  true,
			inputLen: 2,
		},
		{
			name:     "not-nullable VectorArray keys are kept, other not-nullable keys dropped",
			stats:    &datapb.Statistics{NullCounts: map[int64]int64{100: 0, 104: 0, 1001: 0, 1002: 0}},
			schema:   withVectorArray,
			want:     map[int64]int64{104: 0, 1001: 0},
			inputLen: 4,
		},
		{
			name:     "only not-nullable VectorArray keys keeps the same object",
			stats:    &datapb.Statistics{NullCounts: map[int64]int64{104: 0, 1001: 0}},
			schema:   withVectorArray,
			want:     map[int64]int64{104: 0, 1001: 0},
			samePtr:  true,
			inputLen: 2,
		},
		{
			name:     "key not in schema is kept",
			stats:    &datapb.Statistics{NullCounts: map[int64]int64{102: 1, 999: 0}},
			schema:   mixed,
			want:     map[int64]int64{102: 1, 999: 0},
			samePtr:  true,
			inputLen: 2,
		},
		{
			name:     "function output field is dropped",
			stats:    &datapb.Statistics{NullCounts: map[int64]int64{102: 0, 103: 0}},
			schema:   withFunctionOutput,
			want:     map[int64]int64{102: 0},
			inputLen: 2,
		},
		{
			name:    "nil stats",
			stats:   nil,
			schema:  mixed,
			wantNil: true,
		},
		{
			name:     "nil schema keeps the same object",
			stats:    &datapb.Statistics{NullCounts: map[int64]int64{100: 0, 102: 3}},
			schema:   nil,
			want:     map[int64]int64{100: 0, 102: 3},
			samePtr:  true,
			inputLen: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := normalizeStats(test.stats, prunableFields(test.schema), true)
			if test.wantNil {
				assert.Nil(t, got)
				return
			}
			if test.samePtr {
				assert.Same(t, test.stats, got)
			} else {
				assert.NotSame(t, test.stats, got)
				assert.Equal(t, test.stats.GetDeleteNumRows(), got.GetDeleteNumRows(),
					"the clone keeps every other field")
			}
			if test.want == nil {
				assert.Nil(t, got.NullCounts)
			} else {
				assert.Equal(t, test.want, got.GetNullCounts())
			}
			assert.Len(t, test.stats.GetNullCounts(), test.inputLen, "the input map must not be modified")
		})
	}
}

func TestKeptNullCounts(t *testing.T) {
	prunable := map[int64]struct{}{100: {}, 101: {}}
	tests := []struct {
		name        string
		in          map[int64]int64
		want        map[int64]int64
		wantChanged bool
	}{
		{"some kept", map[int64]int64{100: 0, 101: 0, 102: 3}, map[int64]int64{102: 3}, true},
		{"all dropped", map[int64]int64{100: 0, 101: 0}, nil, true},
		{"nothing dropped", map[int64]int64{102: 3, 999: 0}, map[int64]int64{102: 3, 999: 0}, false},
		{"nil map", nil, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := maps.Clone(tt.in)
			got, changed := keptNullCounts(tt.in, prunable)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantChanged, changed)
			assert.Equal(t, before, tt.in, "the input map is never written")
		})
	}
}

// interned returns the copy unique.Make currently holds for s.
func interned(s string) string {
	return unique.Make(s).Value()
}

// fresh returns a copy of s with its own backing data, as proto decoding
// gives every decoded string.
func fresh(s string) string {
	return strings.Clone(s)
}

func TestOwnedString(t *testing.T) {
	owner := fresh("ownedstring-ch")
	owners := []string{fresh("ownedstring-other"), owner}
	freshCopy := fresh(owner)
	unlisted := fresh("ownedstring-unlisted")

	tests := []struct {
		name        string
		owners      []string
		in          string
		want        string
		wantChanged bool
	}{
		{name: "fresh copy of an owner", owners: owners, in: freshCopy, want: owner, wantChanged: true},
		{name: "the owner itself", owners: owners, in: owner, want: owner},
		{name: "unlisted", owners: owners, in: unlisted, want: unlisted},
		{name: "no owners", owners: nil, in: freshCopy, want: freshCopy},
		{name: "empty", owners: owners, in: "", want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, changed := ownedString(test.owners, test.in)
			assert.Equal(t, test.wantChanged, changed)
			assert.True(t, sameStringData(test.want, got), "got %q", got)
		})
	}
}

func TestNormalizePosition(t *testing.T) {
	ch := fresh("normpos-ch")
	vchannels := []string{ch}
	group := "normpos-group"
	internedGroup := interned(group)

	tests := []struct {
		name        string
		pos         *msgpb.MsgPosition
		copyOnWrite bool
		samePtr     bool
		// wantChannel and wantGroup are the copies the result holds; empty
		// means the input's own.
		wantChannel string
		wantGroup   string
	}{
		{name: "nil", pos: nil, copyOnWrite: true, samePtr: true},
		{name: "nil in place", pos: nil, samePtr: true},
		{
			name:        "owned channel",
			pos:         &msgpb.MsgPosition{ChannelName: ch, MsgGroup: fresh(group), Timestamp: 7},
			copyOnWrite: true, samePtr: true,
		},
		{
			name:        "fresh channel is cloned, group untouched",
			pos:         &msgpb.MsgPosition{ChannelName: fresh(ch), MsgGroup: fresh(group), MsgID: []byte{1}, Timestamp: 7},
			copyOnWrite: true, wantChannel: ch,
		},
		{
			name:        "unlisted channel, fresh group",
			pos:         &msgpb.MsgPosition{ChannelName: fresh("normpos-unlisted"), MsgGroup: fresh(group), Timestamp: 7},
			copyOnWrite: true, samePtr: true,
		},
		{
			name:    "in place: fresh channel and group",
			pos:     &msgpb.MsgPosition{ChannelName: fresh(ch), MsgGroup: fresh(group), Timestamp: 7},
			samePtr: true, wantChannel: ch, wantGroup: internedGroup,
		},
		{
			name:    "in place: unlisted channel, fresh group",
			pos:     &msgpb.MsgPosition{ChannelName: fresh("normpos-unlisted"), MsgGroup: fresh(group), Timestamp: 7},
			samePtr: true, wantGroup: internedGroup,
		},
		{
			name:    "in place: empty strings",
			pos:     &msgpb.MsgPosition{MsgID: []byte{1}, Timestamp: 7},
			samePtr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var before *msgpb.MsgPosition
			if test.pos != nil {
				before = proto.Clone(test.pos).(*msgpb.MsgPosition)
			}
			inCh, inGroup := test.pos.GetChannelName(), test.pos.GetMsgGroup()

			got := normalizePosition(test.pos, vchannels, test.copyOnWrite)
			if test.samePtr {
				require.Same(t, test.pos, got)
			} else {
				require.NotSame(t, test.pos, got)
				// The input is not written.
				assert.True(t, proto.Equal(before, test.pos))
				assert.True(t, sameStringData(inCh, test.pos.GetChannelName()))
				assert.True(t, sameStringData(inGroup, test.pos.GetMsgGroup()))
			}
			if got == nil {
				return
			}
			assert.True(t, proto.Equal(before, got), "the result keeps every value")
			wantCh, wantGroup := inCh, inGroup
			if test.wantChannel != "" {
				wantCh = test.wantChannel
			}
			if test.wantGroup != "" {
				wantGroup = test.wantGroup
			}
			assert.True(t, sameStringData(wantCh, got.GetChannelName()), "channel copy")
			assert.True(t, sameStringData(wantGroup, got.GetMsgGroup()), "group copy")
		})
	}
}

func TestNormalizeStats_Formats(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 102, DataType: schemapb.DataType_VarChar, Nullable: true},
	}}
	unknown := fresh("normstats-unknown-format")

	tests := []struct {
		name        string
		stats       *datapb.Statistics
		schema      *schemapb.CollectionSchema
		samePtr     bool
		wantFormats []string // the copies the result holds
		wantCounts  map[int64]int64
	}{
		{
			name:        "constant formats, nothing to prune",
			stats:       &datapb.Statistics{Formats: []string{specutil.FormatParquet}, NullCounts: map[int64]int64{102: 1}},
			schema:      schema,
			samePtr:     true,
			wantFormats: []string{specutil.FormatParquet}, wantCounts: map[int64]int64{102: 1},
		},
		{
			name:        "unknown format only",
			stats:       &datapb.Statistics{Formats: []string{unknown}},
			samePtr:     true,
			wantFormats: []string{unknown},
		},
		{
			name:        "fresh formats, unknown schema",
			stats:       &datapb.Statistics{Formats: []string{fresh("parquet"), fresh("vortex")}, NullCounts: map[int64]int64{100: 0}},
			wantFormats: []string{specutil.FormatParquet, specutil.FormatVortex},
			wantCounts:  map[int64]int64{100: 0},
		},
		{
			name:        "fresh, unknown and empty formats with prune in one clone",
			stats:       &datapb.Statistics{Formats: []string{unknown, fresh("lance-table"), ""}, NullCounts: map[int64]int64{100: 0, 102: 1}},
			schema:      schema,
			wantFormats: []string{unknown, specutil.FormatLanceTable, ""},
			wantCounts:  map[int64]int64{102: 1},
		},
		{
			name:        "constant formats, prune",
			stats:       &datapb.Statistics{Formats: []string{specutil.FormatVortex}, NullCounts: map[int64]int64{100: 0, 102: 1}},
			schema:      schema,
			wantFormats: []string{specutil.FormatVortex},
			wantCounts:  map[int64]int64{102: 1},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			before := proto.Clone(test.stats).(*datapb.Statistics)
			inFormats := append([]string(nil), test.stats.GetFormats()...)

			got := normalizeStats(test.stats, prunableFields(test.schema), true)
			if test.samePtr {
				assert.Same(t, test.stats, got)
			} else {
				assert.NotSame(t, test.stats, got)
			}
			assert.Equal(t, test.wantCounts, got.GetNullCounts())
			assert.Equal(t, before.GetFormats(), got.GetFormats())
			for i, f := range got.GetFormats() {
				assert.True(t, sameStringData(test.wantFormats[i], f), "format %q", f)
			}
			// The input is not written.
			assert.True(t, proto.Equal(before, test.stats))
			for i, f := range test.stats.GetFormats() {
				assert.True(t, sameStringData(inFormats[i], f))
			}
		})
	}
}
