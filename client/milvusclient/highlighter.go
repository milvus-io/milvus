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
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
)

// Server-recognized param keys. Kept private — these are wire-protocol details,
// not public API. The server reads them from Highlighter.Params at
// internal/proxy/highlighter.go:28-40.
const (
	preTagsKey             = "pre_tags"
	postTagsKey            = "post_tags"
	highlightSearchTextKey = "highlight_search_text"
	highlightQueryKey      = "highlight_query"
	fragmentOffsetKey      = "fragment_offset"
	fragmentSizeKey        = "fragment_size"
	fragmentNumKey         = "num_of_fragments"
)

// HighlightType re-exports of the proto enum, scoped under milvusclient so
// callers do not need to import the proto package directly.
const (
	HighlightTypeLexical  = commonpb.HighlightType_Lexical
	HighlightTypeSemantic = commonpb.HighlightType_Semantic
)

// Highlighter is the union of supported highlighter configurations.
// Users obtain a value via NewLexicalHighlighter (and NewSemanticHighlighter
// in a future PR) and pass it to (*searchOption).WithHighlighter.
type Highlighter interface {
	Type() commonpb.HighlightType
	protoMessage() (*commonpb.Highlighter, error) // unexported; called by Request()
}

// lexicalQuery is one entry in the highlight_query JSON list. The server
// accepts a JSON array of these objects under the "highlight_query" key.
type lexicalQuery struct {
	Text  string `json:"text"`
	Type  string `json:"type"`
	Field string `json:"field"`
}

// LexicalHighlighter builds the tantivy-backed lexical highlighter config.
// It is suitable for both BM25 search-term highlighting and TEXT_MATCH
// filter-term highlighting. The server default for fragment_size is 100 and
// for num_of_fragments is 5; both can be overridden via the corresponding
// WithFragmentSize / WithNumFragments builder methods.
type LexicalHighlighter struct {
	queries             []lexicalQuery
	preTags             []string
	postTags            []string
	fragmentSize        int
	fragmentOffset      int
	numFragments        int
	highlightSearchText bool

	fragmentSizeSet        bool
	fragmentOffsetSet      bool
	numFragmentsSet        bool
	highlightSearchTextSet bool
}

// NewLexicalHighlighter creates a new LexicalHighlighter with default settings.
// All builder methods are optional; defaults are taken from the server.
func NewLexicalHighlighter() *LexicalHighlighter {
	return &LexicalHighlighter{}
}

// WithQuery appends one entry to the highlight_query list. queryType must be
// one of the values the server recognizes (currently "TextMatch"). Calling
// WithQuery multiple times builds a list — every entry is included in the
// JSON array that becomes the "highlight_query" param value.
func (l *LexicalHighlighter) WithQuery(field, text, queryType string) *LexicalHighlighter {
	l.queries = append(l.queries, lexicalQuery{Text: text, Type: queryType, Field: field})
	return l
}

// WithPreTags sets the tags inserted before each matched term. Tags rotate by
// match sequence when multiple are provided. The server default is ["<em>"].
func (l *LexicalHighlighter) WithPreTags(tags ...string) *LexicalHighlighter {
	l.preTags = append([]string(nil), tags...)
	return l
}

// WithPostTags sets the tags inserted after each matched term. The server
// default is ["</em>"].
func (l *LexicalHighlighter) WithPostTags(tags ...string) *LexicalHighlighter {
	l.postTags = append([]string(nil), tags...)
	return l
}

// WithFragmentSize sets the maximum length of each fragment to return.
// The server default is 100.
func (l *LexicalHighlighter) WithFragmentSize(n int) *LexicalHighlighter {
	l.fragmentSize = n
	l.fragmentSizeSet = true
	return l
}

// WithFragmentOffset sets the number of characters to reserve before the first
// matched term. The server default is 0.
func (l *LexicalHighlighter) WithFragmentOffset(n int) *LexicalHighlighter {
	l.fragmentOffset = n
	l.fragmentOffsetSet = true
	return l
}

// WithNumFragments sets the maximum number of fragments to return. The server
// default is 5.
func (l *LexicalHighlighter) WithNumFragments(n int) *LexicalHighlighter {
	l.numFragments = n
	l.numFragmentsSet = true
	return l
}

// WithHighlightSearchText enables highlighting the BM25 search text itself.
// Only valid with a BM25 metric.
func (l *LexicalHighlighter) WithHighlightSearchText(enabled bool) *LexicalHighlighter {
	l.highlightSearchText = enabled
	l.highlightSearchTextSet = true
	return l
}

// Type returns the Highlighter type for this configuration.
func (l *LexicalHighlighter) Type() commonpb.HighlightType {
	return HighlightTypeLexical
}

// Validate checks that the highlighter is well-formed by attempting to build
// its proto representation. Returns the first validation error encountered.
func (l *LexicalHighlighter) Validate() error {
	_, err := l.protoMessage()
	return err
}

func (l *LexicalHighlighter) protoMessage() (*commonpb.Highlighter, error) {
	if l.fragmentSizeSet && l.fragmentSize <= 0 {
		return nil, errors.New("fragment_size must be positive")
	}
	if l.fragmentOffsetSet && l.fragmentOffset < 0 {
		return nil, errors.New("fragment_offset must be non-negative")
	}
	if l.numFragmentsSet && l.numFragments < 0 {
		return nil, errors.New("num_of_fragments must be non-negative")
	}
	for i, q := range l.queries {
		if q.Field == "" {
			return nil, fmt.Errorf("highlight_query[%d]: field must be non-empty", i)
		}
		if q.Text == "" {
			return nil, fmt.Errorf("highlight_query[%d]: text must be non-empty", i)
		}
		if q.Type == "" {
			return nil, fmt.Errorf("highlight_query[%d]: type must be non-empty", i)
		}
	}
	for i, tag := range l.preTags {
		if tag == "" {
			return nil, fmt.Errorf("pre_tags[%d] must be non-empty", i)
		}
	}
	for i, tag := range l.postTags {
		if tag == "" {
			return nil, fmt.Errorf("post_tags[%d] must be non-empty", i)
		}
	}

	params := make([]*commonpb.KeyValuePair, 0, 7)
	var err error
	if len(l.queries) > 0 {
		var encoded []byte
		encoded, err = marshalNoHTMLEscape(l.queries)
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal highlight_query")
		}
		params = append(params, &commonpb.KeyValuePair{Key: highlightQueryKey, Value: string(encoded)})
	}
	if l.highlightSearchTextSet {
		params, err = appendJSONParam(params, highlightSearchTextKey, l.highlightSearchText)
		if err != nil {
			return nil, err
		}
	}
	if len(l.preTags) > 0 {
		params, err = appendJSONParam(params, preTagsKey, l.preTags)
		if err != nil {
			return nil, err
		}
	}
	if len(l.postTags) > 0 {
		params, err = appendJSONParam(params, postTagsKey, l.postTags)
		if err != nil {
			return nil, err
		}
	}
	if l.fragmentSizeSet {
		params, err = appendJSONParam(params, fragmentSizeKey, l.fragmentSize)
		if err != nil {
			return nil, err
		}
	}
	if l.fragmentOffsetSet {
		params, err = appendJSONParam(params, fragmentOffsetKey, l.fragmentOffset)
		if err != nil {
			return nil, err
		}
	}
	if l.numFragmentsSet {
		params, err = appendJSONParam(params, fragmentNumKey, l.numFragments)
		if err != nil {
			return nil, err
		}
	}

	return &commonpb.Highlighter{
		Type:   HighlightTypeLexical,
		Params: params,
	}, nil
}

// appendJSONParam serializes value as JSON and appends a KeyValuePair to
// params. Strings are written verbatim (no JSON quoting); booleans, ints,
// floats, slices, and structs serialize naturally via encoding/json. A nil
// value skips the entry, letting the server fall back to its default.
//
// Used internally by highlighter builders to produce the wire-format
// KeyValuePair list. Mirrors the pattern in runAnalyzerOption.WithAnalyzerParams
// (read_options.go:956-963).
func appendJSONParam(params []*commonpb.KeyValuePair, key string, value any) ([]*commonpb.KeyValuePair, error) {
	if value == nil {
		return params, nil
	}
	var encoded string
	switch v := value.(type) {
	case string:
		encoded = v
	case []byte:
		encoded = string(v)
	default:
		b, err := marshalNoHTMLEscape(v)
		if err != nil {
			return params, errors.Wrapf(err, "failed to marshal value for key %q", key)
		}
		encoded = string(b)
	}
	return append(params, &commonpb.KeyValuePair{Key: key, Value: encoded}), nil
}

// marshalNoHTMLEscape serializes value as JSON without HTML-escaping <, >, &.
// Go's default json.Marshal HTML-escapes those characters in strings, which
// makes wire output noisy (e.g. "<em>" becomes "<em>") for
// parameters that are intended to contain HTML-like delimiters such as the
// highlighter's pre/post tags. The server parses both forms identically; we
// emit the unescaped form for parity with the Python SDK and for readable
// error messages.
func marshalNoHTMLEscape(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	out := buf.Bytes()
	// Encoder appends a trailing newline; strip it for byte-for-byte parity
	// with json.Marshal.
	if n := len(out); n > 0 && out[n-1] == '\n' {
		out = out[:n-1]
	}
	return out, nil
}
