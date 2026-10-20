// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package highlight

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestBytesOffsetToRuneOffset(t *testing.T) {
	// test with chinese
	text := "你好世界" // 12 bytes, 4 runes
	spans := SpanList{{0, 6}, {6, 12}}
	err := bytesOffsetToRuneOffset(text, spans)
	assert.NoError(t, err)
	assert.Equal(t, SpanList{{0, 2}, {2, 4}}, spans)

	// test with emoji
	text = "Hello👋World" // 15 bytes, 11 runes
	spans = SpanList{{0, 5}, {5, 9}, {9, 14}}
	err = bytesOffsetToRuneOffset(text, spans)
	assert.NoError(t, err)
	assert.Equal(t, SpanList{{0, 5}, {5, 6}, {6, 11}}, spans)
}

func TestBuildFragments(t *testing.T) {
	token := func(text string, start, end int64) *milvuspb.AnalyzerToken {
		return &milvuspb.AnalyzerToken{Token: text, StartOffset: start, EndOffset: end}
	}
	task := &querypb.HighlightTask{Texts: []string{"hello", "world", "hello world hello", ""}, SearchTextNum: 1, CorpusTextNum: 2, Queries: []*querypb.HighlightQuery{{}}, Options: &querypb.HighlightOptions{FragmentSize: 5, NumOfFragments: 5}}
	tokens := [][]*milvuspb.AnalyzerToken{{token("hello", 0, 5)}, {token("world", 0, 5)}, {token("hello", 12, 17), token("world", 6, 11), token("hello", 0, 5)}, {}}
	result, err := BuildFragments(task, []int64{2}, tokens)
	require.NoError(t, err)
	require.Len(t, result, 2)
	require.Len(t, result[0].Fragments, 3)
	require.Empty(t, result[1].Fragments)
	for _, invalid := range []string{"negative search", "negative corpus", "text count", "result count", "negative topk", "topk count", "start offset", "end offset"} {
		t.Run(invalid, func(t *testing.T) {
			input := proto.Clone(task).(*querypb.HighlightTask)
			analyzed := append([][]*milvuspb.AnalyzerToken(nil), tokens...)
			topks := []int64{2}
			switch invalid {
			case "negative search":
				input.SearchTextNum = -1
			case "negative corpus":
				input.CorpusTextNum = -1
			case "text count":
				input.SearchTextNum = 2
			case "result count":
				analyzed = analyzed[:1]
			case "negative topk":
				topks = []int64{-1}
			case "topk count":
				topks = []int64{1}
			case "start offset":
				analyzed[2] = []*milvuspb.AnalyzerToken{token("hello", -1, 5)}
			case "end offset":
				analyzed[2] = []*milvuspb.AnalyzerToken{token("hello", 0, 99)}
			}
			_, err := BuildFragments(input, topks, analyzed)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
		})
	}
	// Explicit highlight queries also apply when no search text is highlighted.
	task.SearchTextNum = 0
	task.Texts = task.Texts[1:]
	tokens = tokens[1:]
	result, err = BuildFragments(task, []int64{1, 1}, tokens)
	require.NoError(t, err)
	require.Equal(t, []int64{6, 11}, result[0].Fragments[0].Offsets)
}

func TestLexicalFragmentsAndOffsets(t *testing.T) {
	require.Equal(t, SpanList{{0, 6}, {8, 10}}, mergeOffsets(SpanList{{4, 6}, {0, 3}, {0, 2}, {2, 5}, {8, 10}}))
	require.Empty(t, mergeOffsets(nil))
	text := "01234567890123456789"
	require.Empty(t, fetchFragmentsFromOffsets(text, SpanList{{1, 3}}, 0, 4, 0))
	fragments := fetchFragmentsFromOffsets(text, SpanList{{1, 3}, {4, 9}, {15, 18}}, 2, 5, 1)
	require.Len(t, fragments, 1)
	require.Equal(t, int64(0), fragments[0].StartOffset)
	require.Equal(t, int64(9), fragments[0].EndOffset)
	require.Equal(t, []int64{1, 3, 4, 9}, fragments[0].Offsets)
}
