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

package milvusclient

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/client/v3/entity"
)

func TestLexicalHighlighterProtoBasic(t *testing.T) {
	hl := NewLexicalHighlighter().
		WithQuery("text", "hello", "TextMatch").
		WithQuery("text", "world", "TextMatch").
		WithPreTags("<em>").
		WithPostTags("</em>").
		WithFragmentSize(50).
		WithFragmentOffset(5).
		WithNumFragments(2).
		WithHighlightSearchText(true)

	pb, err := hl.protoMessage()
	require.NoError(t, err)
	require.NotNil(t, pb)
	require.Equal(t, HighlightTypeLexical, pb.GetType())

	params := entity.KvPairsMap(pb.GetParams())
	require.Equal(t, "[\"<em>\"]", params[preTagsKey])
	require.Equal(t, "[\"</em>\"]", params[postTagsKey])
	require.Equal(t, "true", params[highlightSearchTextKey])
	require.Equal(t, "50", params[fragmentSizeKey])
	require.Equal(t, "5", params[fragmentOffsetKey])
	require.Equal(t, "2", params[fragmentNumKey])

	var queries []map[string]string
	require.NoError(t, json.Unmarshal([]byte(params[highlightQueryKey]), &queries))
	require.Len(t, queries, 2)
	require.Equal(t, "TextMatch", queries[0]["type"])
	require.Equal(t, "text", queries[0]["field"])
	require.Equal(t, "hello", queries[0]["text"])
	require.Equal(t, "world", queries[1]["text"])
}

func TestLexicalHighlighterProtoDefaults(t *testing.T) {
	hl := NewLexicalHighlighter()
	pb, err := hl.protoMessage()
	require.NoError(t, err)
	require.NotNil(t, pb)
	require.Equal(t, HighlightTypeLexical, pb.GetType())
	// No builders called → Params is empty → server uses defaults.
	require.Empty(t, pb.GetParams())
}

func TestLexicalHighlighterTypeMethod(t *testing.T) {
	require.Equal(t, HighlightTypeLexical, NewLexicalHighlighter().Type())
}

func TestHighlightTypeAliases(t *testing.T) {
	// HighlightTypeLexical / HighlightTypeSemantic are aliases of the proto enum.
	require.Equal(t, commonpb.HighlightType_Lexical, HighlightTypeLexical)
	require.Equal(t, commonpb.HighlightType_Semantic, HighlightTypeSemantic)
}

func TestAppendJSONParam(t *testing.T) {
	t.Run("nil skips", func(t *testing.T) {
		out, err := appendJSONParam(nil, "k", nil)
		require.NoError(t, err)
		require.Nil(t, out)
	})

	t.Run("string verbatim", func(t *testing.T) {
		out, err := appendJSONParam(nil, "k", "hello")
		require.NoError(t, err)
		require.Len(t, out, 1)
		require.Equal(t, "k", out[0].Key)
		require.Equal(t, "hello", out[0].Value)
	})

	t.Run("bool true", func(t *testing.T) {
		out, err := appendJSONParam(nil, "k", true)
		require.NoError(t, err)
		require.Equal(t, "true", out[0].Value)
	})

	t.Run("bool false", func(t *testing.T) {
		out, err := appendJSONParam(nil, "k", false)
		require.NoError(t, err)
		require.Equal(t, "false", out[0].Value)
	})

	t.Run("int", func(t *testing.T) {
		out, err := appendJSONParam(nil, "k", 5)
		require.NoError(t, err)
		require.Equal(t, "5", out[0].Value)
	})

	t.Run("string slice", func(t *testing.T) {
		out, err := appendJSONParam(nil, "k", []string{"a", "b"})
		require.NoError(t, err)
		require.Equal(t, "[\"a\",\"b\"]", out[0].Value)
	})

	t.Run("heterogeneous any slice", func(t *testing.T) {
		out, err := appendJSONParam(nil, "k", []any{"x", true, 3})
		require.NoError(t, err)
		require.Equal(t, `["x",true,3]`, out[0].Value)
	})

	t.Run("struct", func(t *testing.T) {
		out, err := appendJSONParam(nil, "k", struct {
			A string `json:"a"`
			B int    `json:"b"`
		}{A: "x", B: 1})
		require.NoError(t, err)
		require.Equal(t, `{"a":"x","b":1}`, out[0].Value)
	})

	t.Run("unmarshalable value", func(t *testing.T) {
		out, err := appendJSONParam(nil, "k", make(chan int))
		require.Error(t, err)
		require.Nil(t, out)
	})

	t.Run("appends without dropping", func(t *testing.T) {
		existing := []*commonpb.KeyValuePair{{Key: "first", Value: "1"}}
		out, err := appendJSONParam(existing, "second", "2")
		require.NoError(t, err)
		require.Len(t, out, 2)
		require.Equal(t, "first", out[0].Key)
		require.Equal(t, "second", out[1].Key)
	})

	t.Run("bytes treated as string", func(t *testing.T) {
		out, err := appendJSONParam(nil, "k", []byte("raw"))
		require.NoError(t, err)
		require.Equal(t, "raw", out[0].Value)
	})
}

func TestLexicalHighlighterValidate(t *testing.T) {
	cases := []struct {
		name string
		hl   *LexicalHighlighter
	}{
		{
			name: "empty query field",
			hl:   NewLexicalHighlighter().WithQuery("", "hello", "TextMatch"),
		},
		{
			name: "empty query text",
			hl:   NewLexicalHighlighter().WithQuery("text", "", "TextMatch"),
		},
		{
			name: "empty query type",
			hl:   NewLexicalHighlighter().WithQuery("text", "hello", ""),
		},
		{
			name: "negative fragment size",
			hl:   NewLexicalHighlighter().WithFragmentSize(-1),
		},
		{
			name: "zero fragment size",
			hl:   NewLexicalHighlighter().WithFragmentSize(0),
		},
		{
			name: "negative fragment offset",
			hl:   NewLexicalHighlighter().WithFragmentOffset(-1),
		},
		{
			name: "negative num fragments",
			hl:   NewLexicalHighlighter().WithNumFragments(-1),
		},
		{
			name: "empty pre tag in list",
			hl:   NewLexicalHighlighter().WithPreTags("", "ok"),
		},
		{
			name: "empty post tag in list",
			hl:   NewLexicalHighlighter().WithPostTags("ok", ""),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, tc.hl.Validate())
		})
	}
}

func TestLexicalHighlighterValidCases(t *testing.T) {
	// Sanity: every "happy-path" combination validates.
	cases := []struct {
		name string
		hl   *LexicalHighlighter
	}{
		{
			name: "no builders",
			hl:   NewLexicalHighlighter(),
		},
		{
			name: "only query",
			hl:   NewLexicalHighlighter().WithQuery("text", "hello", "TextMatch"),
		},
		{
			name: "highlight search text disabled",
			hl:   NewLexicalHighlighter().WithHighlightSearchText(false),
		},
		{
			name: "full",
			hl: NewLexicalHighlighter().
				WithQuery("text", "hello", "TextMatch").
				WithPreTags("<em>", "<b>").
				WithPostTags("</em>", "</b>").
				WithFragmentSize(50).
				WithFragmentOffset(0).
				WithNumFragments(1).
				WithHighlightSearchText(true),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, tc.hl.Validate())
		})
	}
}
