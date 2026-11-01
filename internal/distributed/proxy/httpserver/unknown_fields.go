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

package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const (
	unknownFieldWarningHeader = "X-Milvus-Warning"
	unknownFieldPolicyQuery   = "unknownFieldPolicy"
)

// The request policy, when supplied, overrides the server's default for this
// REST v2 call only. Reject ambiguous or invalid values before business work.
func resolveV2UnknownFieldPolicy(c *gin.Context, serverPolicy string) (string, error) {
	rawQuery := c.Request.URL.RawQuery
	// Preserve the old behavior for requests that do not select a policy:
	// unrelated query parameters are owned by the existing REST handlers.
	supplied := false
	for rest := rawQuery; rest != ""; {
		pair, tail, hasNext := strings.Cut(rest, "&")
		key, _, _ := strings.Cut(pair, "=")
		decoded, err := url.QueryUnescape(key)
		if err == nil && decoded == unknownFieldPolicyQuery {
			supplied = true
			break
		}
		if !hasNext {
			break
		}
		rest = tail
	}
	if !supplied {
		return serverPolicy, nil
	}

	query, err := url.ParseQuery(rawQuery)
	if err != nil {
		return "", merr.WrapErrParameterInvalidMsg("invalid REST v2 query parameters")
	}
	values := query[unknownFieldPolicyQuery]
	if len(values) != 1 {
		return "", merr.WrapErrParameterInvalidMsg("unknownFieldPolicy must be exactly one of ignore, warn, strict")
	}
	policy := strings.ToLower(strings.TrimSpace(values[0]))
	switch policy {
	case "ignore", "warn", "strict":
		return policy, nil
	default:
		return "", merr.WrapErrParameterInvalidMsg("unknownFieldPolicy must be exactly one of ignore, warn, strict")
	}
}

// checkV2UnknownFields runs after binding, before business execution. It checks
// only API field names; it neither re-decodes values nor changes global Gin flags.
// In particular, ImportReq's custom decoder and insert/upsert's metadata-only
// decoder retain ownership of value validation and the cached request body.
func checkV2UnknownFields(c *gin.Context, req any, policy string) error {
	if policy == "ignore" {
		return nil
	}
	if policy != "warn" && policy != "strict" {
		return merr.WrapErrServiceInternalMsg("invalid REST v2 unknown field policy")
	}
	cached, exists := c.Get(gin.BodyBytesKey)
	body, ok := cached.([]byte)
	if !exists || !ok {
		return merr.WrapErrServiceInternalMsg("REST v2 bound request body is unavailable")
	}
	ctx := context.TODO()
	if c.Request != nil {
		ctx = c.Request.Context()
	}
	// Borrow the validated, request-owned buffer. Consume each value once;
	// repeatedly extracting nested raw objects would make deep inputs quadratic.
	scanner := jsonFieldScanner{body: body, ctx: ctx}
	path := scanner.walk(reflect.TypeOf(req), "")
	if scanner.err != nil {
		return scanner.err
	}
	if path == "" {
		return nil
	}
	// Never use client-controlled paths or field names as metric labels.
	method := routeToMethod[c.FullPath()]
	if method == "" {
		method = "unknown"
	}
	metrics.RestfulUnknownFieldRequests.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10), method, policy,
	).Inc()
	mlog.RatedInfo(ctx, 1, "REST v2 request contains an unknown field",
		mlog.String("method", method), mlog.String("policy", policy), mlog.String("field_path", path))
	// Only field paths, never their values. ASCII quoting prevents header injection.
	diagnostic := "unknown request field " + strconv.QuoteToASCII(path)
	if policy == "strict" {
		return merr.WrapErrParameterInvalidMsg("%s", diagnostic)
	}
	c.Header(unknownFieldWarningHeader, diagnostic+"; only the first unknown field is reported")
	exposed := strings.Join(c.Writer.Header().Values("Access-Control-Expose-Headers"), ", ")
	if exposed != "" {
		exposed += ", "
	}
	c.Writer.Header().Set("Access-Control-Expose-Headers", exposed+unknownFieldWarningHeader)
	return nil
}

// Open maps, interface values and RawMessage/[]byte are opaque. Free map keys
// with fixed struct values are not opaque: the values still have an API schema.
func fixedJSONShape(t reflect.Type) bool {
	if t == nil {
		return false
	}
	switch t.Kind() {
	case reflect.Struct:
		return true
	case reflect.Pointer, reflect.Slice, reflect.Array, reflect.Map:
		// Follow the element type below, detecting recursive open containers.
	default:
		return false
	}
	seen := map[reflect.Type]bool{}
	for t != nil && !seen[t] {
		seen[t] = true
		switch t.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Array, reflect.Map:
			t = t.Elem()
		case reflect.Struct:
			return true
		default:
			return false
		}
	}
	return false
}

// This cursor is only used after the existing binder validates JSON syntax.
// It decodes keys with encoding/json, but never decodes values (including huge
// numeric literals in RawMessage/entity data). The body is not copied or pooled.
type jsonFieldScanner struct {
	body []byte
	pos  int
	ctx  context.Context
	err  error
}

func (s *jsonFieldScanner) active() bool {
	if s.err == nil {
		s.err = s.ctx.Err()
	}
	return s.err == nil
}

func (s *jsonFieldScanner) whitespace() {
	for s.pos < len(s.body) {
		switch s.body[s.pos] {
		case ' ', '\t', '\r', '\n':
			s.pos++
		default:
			return
		}
	}
}

// skipString starts at an opening quote and ends just after the closing quote.
func (s *jsonFieldScanner) skipString() {
	s.pos++
	for s.pos < len(s.body) {
		offset := bytes.IndexByte(s.body[s.pos:], '"')
		if offset < 0 {
			s.pos = len(s.body)
			return
		}
		quote := s.pos + offset
		// Only an odd run of preceding backslashes escapes this quote.
		backslashes := 0
		for i := quote - 1; i >= s.pos && s.body[i] == '\\'; i-- {
			backslashes++
		}
		s.pos = quote + 1
		if backslashes%2 == 0 {
			return
		}
	}
}

func (s *jsonFieldScanner) skipValue() {
	s.whitespace()
	if !s.active() {
		return
	}
	depth := 0
	for s.pos < len(s.body) {
		switch s.body[s.pos] {
		case '"':
			s.skipString()
			if s.err != nil || depth == 0 {
				return
			}
			continue
		case '{', '[':
			depth++
		case '}', ']':
			if depth == 0 {
				return
			}
			depth--
			if depth == 0 {
				s.pos++
				return
			}
		case ',', ' ', '\t', '\r', '\n':
			if depth == 0 {
				return
			}
		}
		s.pos++
	}
}

func (s *jsonFieldScanner) walk(t reflect.Type, path string) string {
	if !s.active() {
		return ""
	}
	if !fixedJSONShape(t) {
		s.skipValue()
		return ""
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	s.whitespace()
	if s.err != nil || s.pos >= len(s.body) {
		return ""
	}
	switch t.Kind() {
	case reflect.Struct, reflect.Map:
		if s.body[s.pos] != '{' {
			s.skipValue() // Existing binding owns null/shape handling.
			return ""
		}
		s.pos++
		var fields *jsonAPIFields
		if t.Kind() == reflect.Struct {
			fields = jsonFieldsFor(t)
		}
		for s.pos < len(s.body) && s.active() {
			s.whitespace()
			if s.pos >= len(s.body) || s.body[s.pos] == '}' {
				s.pos++
				break
			}
			start := s.pos
			s.skipString()
			if s.err != nil {
				return ""
			}
			var key string
			if err := json.Unmarshal(s.body[start:s.pos], &key); err != nil {
				s.err = merr.WrapErrServiceInternalMsg("REST v2 bound JSON could not be inspected")
				return ""
			}
			s.whitespace()
			s.pos++ // colon; syntax has already been validated
			var childType reflect.Type
			if fields != nil {
				childType = fields.lookup(key)
			} else {
				childType = t.Elem()
			}
			if childType == nil {
				return unknownFieldPath(path, key)
			}
			if !fixedJSONShape(childType) {
				s.skipValue()
			} else if unknown := s.walk(childType, unknownFieldPath(path, key)); unknown != "" {
				return unknown
			}
			s.whitespace()
			if s.pos < len(s.body) && s.body[s.pos] == ',' {
				s.pos++
			}
		}
	case reflect.Slice, reflect.Array:
		if s.body[s.pos] != '[' {
			s.skipValue()
			return ""
		}
		s.pos++
		elemType := t.Elem()
		checkElem := fixedJSONShape(elemType)
		for index := 0; s.pos < len(s.body) && s.active(); index++ {
			s.whitespace()
			if s.pos >= len(s.body) || s.body[s.pos] == ']' {
				s.pos++
				break
			}
			if !checkElem {
				s.skipValue()
			} else if unknown := s.walk(elemType, unknownFieldPath(path, strconv.Itoa(index))); unknown != "" {
				return unknown
			}
			s.whitespace()
			if s.pos < len(s.body) && s.body[s.pos] == ',' {
				s.pos++
			}
		}
	}
	return ""
}

// RFC 6901 paths distinguish dots, slashes and empty keys. Bound diagnostics,
// not matching: truncating an error path must never affect field acceptance.
func unknownFieldPath(parent, key string) string {
	const limit = 256
	const truncated = "...[truncated]"
	if len(parent) > limit {
		return parent
	}
	if len(key) > limit {
		key = key[:limit] + truncated
	}
	key = strings.ReplaceAll(strings.ReplaceAll(key, "~", "~0"), "/", "~1")
	path := parent + "/" + key
	if len(path) > limit {
		return path[:limit] + truncated
	}
	return path
}

type jsonAPIField struct {
	name   string
	typeOf reflect.Type
}

type jsonAPIFields struct {
	exact   map[string]reflect.Type
	ordered []jsonAPIField
}

func (fields *jsonAPIFields) lookup(name string) reflect.Type {
	if t := fields.exact[name]; t != nil {
		return t
	}
	// Prefer exact names, then case-folded matches in declaration order.
	for _, field := range fields.ordered {
		if strings.EqualFold(field.name, name) {
			return field.typeOf
		}
	}
	return nil
}

var jsonAPIFieldsCache sync.Map // reflect.Type -> *jsonAPIFields; no request data

func jsonFieldsFor(t reflect.Type) *jsonAPIFields {
	if cached, ok := jsonAPIFieldsCache.Load(t); ok {
		return cached.(*jsonAPIFields)
	}
	fields := &jsonAPIFields{exact: map[string]reflect.Type{}}
	// REST v2 request structs use explicit fields, not promoted embedded fields.
	// Keep this schema cache scoped to those types, rather than implementing a
	// general-purpose Go JSON field resolver.
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}
		tag := field.Tag.Get("json")
		if tag == "-" {
			continue
		}
		name := strings.Split(tag, ",")[0]
		if name == "" {
			name = field.Name
		}
		fields.exact[name] = field.Type
		fields.ordered = append(fields.ordered, jsonAPIField{name: name, typeOf: field.Type})
	}
	actual, _ := jsonAPIFieldsCache.LoadOrStore(t, fields)
	return actual.(*jsonAPIFields)
}
