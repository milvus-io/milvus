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
	"encoding/base64"
	gojson "encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"github.com/spf13/cast"
	"github.com/tidwall/gjson"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	mhttp "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/proxy/accesslog"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/interceptor"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/parameterutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func HTTPReturn(c *gin.Context, code int, result gin.H) {
	c.Set(HTTPReturnCode, result[HTTPReturnCode])
	if errorMsg, ok := result[HTTPReturnMessage]; ok {
		c.Set(HTTPReturnMessage, errorMsg)
	}
	setTraceIDHeader(c)
	c.JSON(code, result)
}

// HTTPReturnStream uses custom jsonRender that encodes JSON data directly into the response writer.
// Timeout-wrapped REST routes still buffer encoded bytes before committing them to the client.
func HTTPReturnStream(c *gin.Context, code int, result gin.H) {
	c.Set(HTTPReturnCode, result[HTTPReturnCode])
	if errorMsg, ok := result[HTTPReturnMessage]; ok {
		c.Set(HTTPReturnMessage, errorMsg)
	}
	setTraceIDHeader(c)
	c.Render(code, jsonRender{Data: result})
}

func HTTPAbortReturn(c *gin.Context, code int, result gin.H) {
	c.Set(HTTPReturnCode, result[HTTPReturnCode])
	if errorMsg, ok := result[HTTPReturnMessage]; ok {
		c.Set(HTTPReturnMessage, errorMsg)
	}
	setTraceIDHeader(c)
	c.AbortWithStatusJSON(code, result)
}

func TraceIDHandlerFunc(c *gin.Context) {
	ctx := otel.GetTextMapPropagator().Extract(c.Request.Context(), propagation.HeaderCarrier(c.Request.Header))
	ctx, span := otel.Tracer(typeutil.ProxyRole).Start(ctx, c.Request.URL.Path)
	defer span.End()

	traceID := span.SpanContext().TraceID()
	if traceID.IsValid() {
		traceIDStr := traceID.String()
		c.Set("traceID", traceIDStr)
		c.Request = c.Request.WithContext(ctx)
		setTraceIDHeader(c)
	}

	c.Next()
}

func getTraceID(c *gin.Context) (string, bool) {
	traceID, ok := c.Get("traceID")
	if ok {
		traceIDStr, ok := traceID.(string)
		if ok && traceIDStr != "" {
			return traceIDStr, true
		}
	}

	if c.Request == nil {
		return "", false
	}
	spanTraceID := oteltrace.SpanFromContext(c.Request.Context()).SpanContext().TraceID()
	if !spanTraceID.IsValid() {
		return "", false
	}
	return spanTraceID.String(), true
}

func setTraceIDHeader(c *gin.Context) {
	traceID, ok := getTraceID(c)
	if !ok {
		return
	}
	setTraceIDHeaderTo(c.Writer.Header(), traceID)
}

func setTraceIDHeaderTo(header http.Header, traceID string) {
	header.Set(HTTPHeaderMilvusTraceID, traceID)
}

func ParseUsernamePassword(c *gin.Context) (string, string, bool) {
	username, password, ok := c.Request.BasicAuth()
	if !ok {
		token := GetAuthorization(c)
		i := strings.IndexAny(token, util.CredentialSeparator)
		if i != -1 {
			username = token[:i]
			password = token[i+1:]
		}
	} else {
		c.Header("WWW-Authenticate", `Basic realm="restricted", charset="UTF-8"`)
	}
	return username, password, username != "" && password != ""
}

func GetAuthorization(c *gin.Context) string {
	auth := c.Request.Header.Get("Authorization")
	return strings.TrimPrefix(auth, "Bearer ")
}

// find the primary field of collection
func getPrimaryField(schema *schemapb.CollectionSchema) (*schemapb.FieldSchema, bool) {
	for _, field := range schema.Fields {
		if field.IsPrimaryKey {
			return field, true
		}
	}
	return nil, false
}

// primaryKeyTemplateVar is the template variable the generated primary key
// filter binds its id list to. It is deliberately not a name a caller could
// collide with from exprParams, which this path does not read anyway.
const primaryKeyTemplateVar = "__pk_ids"

// checkGetPrimaryKey builds the filter for the id based get and delete
// endpoints: `pk in {__pk_ids}`, with the ids carried as a template value.
//
// The ids used to be formatted into the expression text, a VARCHAR id as
// fmt.Sprintf("%q") with no escaping. A quote in an id therefore reached the
// parser as syntax:
//
//	id ["alice\", \"bob"]  ->  pk in ["alice", "bob"]
//
// so one named id matched two rows, and on the v1 delete endpoint removed a row
// the caller never named. An id that merely contained a quote as data, such as
// `say "hi"`, could not be fetched at all because the generated expression did
// not parse.
//
// Passing the ids as a template value removes the text entirely: nothing the
// caller sends is parsed as expression syntax.
func checkGetPrimaryKey(coll *schemapb.CollectionSchema, idResult gjson.Result) (string, map[string]*schemapb.TemplateValue, error) {
	primaryField, ok := getPrimaryField(coll)
	if !ok {
		return "", nil, merr.WrapErrParameterInvalidMsg("collection: %s has no primary field", coll.Name)
	}

	ids, err := primaryKeyTemplateValue(primaryField, idResult)
	if err != nil {
		return "", nil, err
	}
	filter := fmt.Sprintf("%s in {%s}", primaryField.Name, primaryKeyTemplateVar)
	return filter, map[string]*schemapb.TemplateValue{primaryKeyTemplateVar: ids}, nil
}

// primaryKeyTemplateValue converts the id list into a typed template array.
//
// An id that is absent, or null, is a malformed request: on the delete
// endpoints it means the caller named neither a filter nor an id. An id that is
// an empty list is a different thing and stays what it was -- it used to build
// the filter `pk in []`, which the planner accepts and which matches nothing,
// so a batch loop handed an empty batch did nothing and reported success. The
// element type comes from the schema rather than from the elements, so the
// empty list still produces a well-typed array.
func primaryKeyTemplateValue(field *schemapb.FieldSchema, result gjson.Result) (*schemapb.TemplateValue, error) {
	if !result.Exists() || result.Type == gjson.Null {
		return nil, merr.WrapErrParameterInvalidMsg("%s is required", DefaultPrimaryFieldName)
	}
	elements := result.Array()

	switch field.DataType {
	case schemapb.DataType_Int64:
		values := make([]int64, 0, len(elements))
		for _, element := range elements {
			value, err := primaryKeyInt64(element)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		return &schemapb.TemplateValue{
			Val: &schemapb.TemplateValue_ArrayVal{
				ArrayVal: &schemapb.TemplateArrayValue{
					Data: &schemapb.TemplateArrayValue_LongData{
						LongData: &schemapb.LongArray{Data: values},
					},
				},
			},
		}, nil

	case schemapb.DataType_VarChar:
		values := make([]string, 0, len(elements))
		for _, element := range elements {
			switch element.Type {
			case gjson.String:
				values = append(values, element.Str)
			case gjson.Number:
				// The same reading convertIDsToSchemapbIDs gives an id and
				// stringFieldValue gives a VarChar field: the literal, not a
				// float64 rendering of it. Refusing it here instead would make
				// a row that insert stores and search-by-id finds impossible to
				// get or delete by the id it was stored under.
				values = append(values, element.Raw)
			default:
				return nil, merr.WrapErrParameterInvalidMsg(
					"%s must be a string or a number for a VarChar primary key, got: %s",
					DefaultPrimaryFieldName, element.Raw)
			}
		}
		return &schemapb.TemplateValue{
			Val: &schemapb.TemplateValue_ArrayVal{
				ArrayVal: &schemapb.TemplateArrayValue{
					Data: &schemapb.TemplateArrayValue_StringData{
						StringData: &schemapb.StringArray{Data: values},
					},
				},
			},
		}, nil

	default:
		return nil, merr.WrapErrParameterInvalidMsg(
			"unsupported primary key type: %s", field.DataType.String())
	}
}

// primaryKeyInt64 reads one id for an Int64 primary key.
//
// A quoted id is read as base 10. cast used strconv's base detection here too,
// so a zero-padded id such as "010" looked up primary key 8.
func primaryKeyInt64(element gjson.Result) (int64, error) {
	switch element.Type {
	case gjson.Number:
		value, ok := parseJSONInteger(element.Raw, 64)
		if !ok {
			return 0, merr.WrapErrParameterInvalidMsg(
				"%s must be an integer in the int64 range, got: %s", DefaultPrimaryFieldName, element.Raw)
		}
		return value, nil
	case gjson.String:
		value, err := strconv.ParseInt(element.Str, 10, 64)
		if err != nil {
			return 0, merr.WrapErrParameterInvalidMsg(
				"%s must be an integer for an Int64 primary key, got: %q", DefaultPrimaryFieldName, element.Str)
		}
		return value, nil
	default:
		return 0, merr.WrapErrParameterInvalidMsg(
			"%s must be an integer for an Int64 primary key, got: %s", DefaultPrimaryFieldName, element.Raw)
	}
}

// convertIDsToSchemapbIDs reads the id list for search-by-id from the literals
// the caller wrote.
//
// The ids used to arrive already decoded into []interface{}, so every number
// had been through float64 before it was examined. Two things followed. An id
// past 2^53 lost its low bits, so the search ran against a primary key the
// caller never asked for. And an id that was not an integer at all could round
// to one on the way in: 4503599627370496.5 becomes 4503599627370496.0, which
// then passes a fractional-part check. For a VarChar key the float was
// formatted with %v, so the id 1000000 was searched for as "1e+06".
func convertIDsToSchemapbIDs(ids []json.RawMessage, pkField *schemapb.FieldSchema) (*schemapb.IDs, error) {
	if len(ids) == 0 {
		return nil, merr.WrapErrParameterMissingMsg("ids array cannot be empty")
	}

	switch pkField.DataType {
	case schemapb.DataType_Int64:
		int64IDs := make([]int64, 0, len(ids))
		for i, raw := range ids {
			value := gjson.ParseBytes(raw)
			switch value.Type {
			case gjson.Number:
				parsed, ok := parseJSONInteger(value.Raw, 64)
				if !ok {
					return nil, merr.WrapErrParameterInvalidMsg(
						"invalid int64 id at index %d: %s is not an integer in the int64 range", i, value.Raw)
				}
				int64IDs = append(int64IDs, parsed)
			case gjson.String:
				parsed, err := strconv.ParseInt(value.Str, 10, 64)
				if err != nil {
					return nil, merr.WrapErrParameterInvalidErr(err, "invalid int64 id at index %d: %q", i, value.Str)
				}
				int64IDs = append(int64IDs, parsed)
			default:
				return nil, merr.WrapErrParameterInvalidMsg(
					"invalid id type at index %d: expected an integer, got %s", i, value.Raw)
			}
		}
		return &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: int64IDs},
			},
		}, nil

	case schemapb.DataType_VarChar:
		stringIDs := make([]string, 0, len(ids))
		for i, raw := range ids {
			value := gjson.ParseBytes(raw)
			var stringID string
			switch value.Type {
			case gjson.String:
				stringID = value.Str
			case gjson.Number:
				// the literal, not a float64 rendering of it
				stringID = value.Raw
			default:
				return nil, merr.WrapErrParameterInvalidMsg(
					"invalid id type at index %d: expected a string, got %s", i, value.Raw)
			}
			if stringID == "" {
				return nil, merr.WrapErrParameterInvalidMsg("empty string id at index %d", i)
			}
			stringIDs = append(stringIDs, stringID)
		}
		return &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{
				StrId: &schemapb.StringArray{Data: stringIDs},
			},
		}, nil

	default:
		return nil, merr.WrapErrParameterInvalidMsg(
			"unsupported primary key type: %s", pkField.DataType.String())
	}
}

// --------------------- collection details --------------------- //

func printFields(fields []*schemapb.FieldSchema) []gin.H {
	res := make([]gin.H, 0, len(fields))
	for _, field := range fields {
		if field.Name == common.MetaFieldName || field.Name == common.NamespaceFieldName {
			continue
		}
		fieldDetail := printFieldDetail(field, true)
		res = append(res, fieldDetail)
	}
	return res
}

func printFieldsV2(fields []*schemapb.FieldSchema) []gin.H {
	res := make([]gin.H, 0, len(fields))
	for _, field := range fields {
		if field.Name == common.MetaFieldName || field.Name == common.NamespaceFieldName {
			continue
		}
		fieldDetail := printFieldDetail(field, false)
		res = append(res, fieldDetail)
	}
	return res
}

func printFieldDetail(field *schemapb.FieldSchema, oldVersion bool) gin.H {
	fieldDetail := gin.H{
		HTTPReturnFieldName:          field.Name,
		HTTPReturnFieldPrimaryKey:    field.IsPrimaryKey,
		HTTPReturnFieldPartitionKey:  field.IsPartitionKey,
		HTTPReturnFieldClusteringKey: field.IsClusteringKey,
		HTTPReturnFieldAutoID:        field.AutoID,
		HTTPReturnDescription:        field.Description,
		HTTPReturnFieldNullable:      field.Nullable,
	}
	if field.DefaultValue != nil {
		fieldDetail[HTTPRequestDefaultValue] = field.DefaultValue
	}
	if field.GetIsFunctionOutput() {
		fieldDetail[HTTPReturnFieldIsFunctionOutput] = true
	}
	if field.GetExternalField() != "" {
		fieldDetail["externalField"] = field.GetExternalField()
	}
	if typeutil.IsVectorType(field.DataType) {
		fieldDetail[HTTPReturnFieldType] = field.DataType.String()
		if oldVersion {
			dim, _ := getDim(field)
			fieldDetail[HTTPReturnFieldType] = field.DataType.String() + "(" + strconv.FormatInt(dim, 10) + ")"
		}
	} else if field.DataType == schemapb.DataType_VarChar {
		fieldDetail[HTTPReturnFieldType] = field.DataType.String()
		if oldVersion {
			maxLength, _ := parameterutil.GetMaxLength(field)
			fieldDetail[HTTPReturnFieldType] = field.DataType.String() + "(" + strconv.FormatInt(maxLength, 10) + ")"
		}
	} else {
		fieldDetail[HTTPReturnFieldType] = field.DataType.String()
	}
	if !oldVersion {
		fieldDetail[HTTPReturnFieldID] = field.FieldID
		if field.TypeParams != nil {
			fieldDetail[Params] = field.TypeParams
		}
		if field.DataType == schemapb.DataType_Array || field.DataType == schemapb.DataType_ArrayOfVector {
			fieldDetail[HTTPReturnFieldElementType] = field.GetElementType().String()
		}
	}
	return fieldDetail
}

func printStructArrayFieldsV2(structFields []*schemapb.StructArrayFieldSchema) []gin.H {
	res := make([]gin.H, 0, len(structFields))
	for _, sf := range structFields {
		subs := make([]gin.H, 0, len(sf.GetFields()))
		for _, sub := range sf.GetFields() {
			detail := printFieldDetail(sub, false)
			if short, err := typeutil.ExtractStructFieldName(sub.GetName()); err == nil && short != "" {
				detail[HTTPReturnFieldName] = short
			}
			subs = append(subs, detail)
		}
		entry := gin.H{
			HTTPReturnFieldName:     sf.GetName(),
			HTTPReturnFieldID:       sf.GetFieldID(),
			HTTPReturnDescription:   sf.GetDescription(),
			HTTPReturnFieldNullable: sf.GetNullable(),
			HTTPReturnFieldType:     schemapb.DataType_ArrayOfStruct.String(),
			"fields":                subs,
		}
		if len(sf.GetTypeParams()) > 0 {
			entry[Params] = sf.GetTypeParams()
		}
		res = append(res, entry)
	}
	return res
}

func printFunctionDetails(functions []*schemapb.FunctionSchema) []gin.H {
	res := make([]gin.H, 0, len(functions))
	for _, function := range functions {
		res = append(res, gin.H{
			HTTPReturnFunctionName:             function.Name,
			HTTPReturnDescription:              function.Description,
			HTTPReturnFunctionType:             function.Type,
			HTTPReturnFunctionID:               function.Id,
			HTTPReturnFunctionInputFieldNames:  function.InputFieldNames,
			HTTPReturnFunctionOutputFieldNames: function.OutputFieldNames,
			HTTPReturnFunctionParams:           function.Params,
		})
	}
	return res
}

func getMetricType(pairs []*commonpb.KeyValuePair) string {
	metricType := DefaultMetricType
	for _, pair := range pairs {
		if pair.Key == common.MetricTypeKey {
			metricType = pair.Value
			break
		}
	}
	return metricType
}

func printIndexes(indexes []*milvuspb.IndexDescription) []gin.H {
	res := make([]gin.H, 0, len(indexes))
	for _, index := range indexes {
		res = append(res, gin.H{
			HTTPIndexName:             index.IndexName,
			HTTPIndexField:            index.FieldName,
			HTTPReturnIndexMetricType: getMetricType(index.Params),
		})
	}
	return res
}

// --------------------- insert param --------------------- //

// maxJSONDepth is where simdjson's DOM parser gives up with DEPTH_ERROR. The
// on-demand parser used by ordinary filters copes with far more, but the JSON
// index and JSON_CONTAINS build a DOM, so a deeper document is readable by some
// queries and not others. The request binder allows 9997 levels, so the gap is
// reachable.
const maxJSONDepth = 1024

// checkEngineCompatible reports the first reason the storage engine would not be
// able to read a document back, in one pass over it.
//
// Keeping the caller's bytes is only safe for values the engine can actually
// read. The request binder guarantees the body is syntactically valid JSON, but
// that is a weaker guarantee than it looks:
//
//   - it accepts an integer beyond 64 bits, which simdjson reports as
//     BIGINT_ERROR
//   - it accepts invalid UTF-8, replacing it with U+FFFD when it decodes, while
//     the raw bytes keep the offending byte and simdjson reports UTF8_ERROR
//   - it accepts nesting up to 9997 levels, past the DOM limit above
//   - it accepts an object that declares the same key twice, where the readers
//     disagree about which value wins: encoding/json, Python and PostgreSQL's
//     jsonb keep the last, gjson and simdjson keep the first
//
// The duplicate-key scan needs a full walk anyway, so the rest costs nothing on
// top of it.
func checkEngineCompatible(field string, document string) error {
	if !utf8.ValidString(document) {
		return merr.WrapErrParameterInvalidMsg(
			"field %s contains invalid UTF-8, which the JSON engine cannot read", field)
	}

	// One pass of the standard library's tokenizer, chosen over a recursive
	// gjson walk on purpose. gjson's ForEach re-scans each child's whole
	// subtree to find its extent, so a deep document with a wide leaf cost
	// O(depth * bytes) -- benchmarked at 37x on 500 levels around a 2KB leaf
	// -- and the only ways to keep that walk were a depth line the engine
	// does not have or a work budget with a tunable, both of them complexity
	// wearing a smaller number. The tokenizer reads every byte once; its
	// price is an allocation per token, about two extra microseconds on an
	// ordinary value, paid for never having to think about document shape
	// again. Sonic is not an option because its Decoder has no Token. With
	// UseNumber the tokenizer hands back each number's exact literal, which
	// is what the number check needs.
	//
	// The depth rule is simdjson's: it counts containers, not values, so 1023
	// arrays holding a scalar and 1024 empty arrays both parse while 1024
	// arrays holding a scalar do not -- the limit is on a node sitting past
	// maxJSONDepth, not on reaching it. Anything after the first complete
	// value is ignored, as the previous walk ignored it: every caller hands
	// over exactly one token.
	decoder := gojson.NewDecoder(strings.NewReader(document))
	decoder.UseNumber()

	type frame struct {
		isObject  bool
		expectKey bool
		seen      map[string]struct{}
	}
	var stack []frame

	for {
		token, err := decoder.Token()
		if err != nil {
			if err == io.EOF && len(stack) == 0 {
				return nil
			}
			return merr.WrapErrParameterInvalidMsg(
				"field %s is not a readable JSON document: %s", field, err.Error())
		}

		if delim, ok := token.(gojson.Delim); ok && (delim == ']' || delim == '}') {
			stack = stack[:len(stack)-1]
			if len(stack) == 0 {
				return nil // first value complete
			}
			top := &stack[len(stack)-1]
			if top.isObject {
				top.expectKey = true
			}
			continue
		}

		// a key, or a value node at depth len(stack)+1
		if len(stack) > 0 {
			top := &stack[len(stack)-1]
			if top.isObject && top.expectKey {
				name := token.(string)
				if _, dup := top.seen[name]; dup {
					return merr.WrapErrParameterInvalidMsg(
						"field %s declares the key %s twice; JSON object names must be unique",
						field, name)
				}
				top.seen[name] = struct{}{}
				top.expectKey = false
				continue
			}
			if top.isObject {
				top.expectKey = true
			}
		}

		switch value := token.(type) {
		case gojson.Delim: // '[' or '{'
			if len(stack)+1 > maxJSONDepth {
				return merr.WrapErrParameterInvalidMsg(
					"field %s nests deeper than %d levels, which the JSON engine cannot read",
					field, maxJSONDepth)
			}
			next := frame{isObject: value == '{', expectKey: value == '{'}
			if next.isObject {
				next.seen = make(map[string]struct{})
			}
			stack = append(stack, next)
		case gojson.Number:
			if len(stack)+1 > maxJSONDepth {
				return merr.WrapErrParameterInvalidMsg(
					"field %s nests deeper than %d levels, which the JSON engine cannot read",
					field, maxJSONDepth)
			}
			if _, err := jsonNumberLiteral(field, value.String()); err != nil {
				return err
			}
		default: // string, bool, nil
			if len(stack)+1 > maxJSONDepth {
				return merr.WrapErrParameterInvalidMsg(
					"field %s nests deeper than %d levels, which the JSON engine cannot read",
					field, maxJSONDepth)
			}
		}
		if len(stack) == 0 {
			return nil // scalar document, complete
		}
	}
}

// jsonDocumentForStorage returns the bytes to store for a JSON document,
// rejecting what the engine cannot read.
//
// A lone surrogate is the one case that falls back rather than being rejected.
// The binder accepts it and replaces it with U+FFFD when it decodes, so the
// value was readable before the token was kept; decoding and re-encoding
// reproduces exactly what used to be stored.
func jsonDocumentForStorage(field string, document string) ([]byte, error) {
	// Check the document the caller actually sent. Checking the normalized form
	// instead is too late: decoding resolves a duplicate key to one value and
	// turns an oversized integer into a float, so both would pass.
	if err := checkEngineCompatible(field, document); err != nil {
		return nil, err
	}

	if hasLoneSurrogate(document) {
		// Decode with UseNumber so every number keeps its literal. Going through
		// float64 would rewrite the ones that need more than 53 bits, and an
		// earlier version tried to predict which those were: it asked whether the
		// literal fit an int64, so 2^63 was judged safe and came back as
		// 9223372036854776000, while 9007199254740994 was refused even though it
		// round-trips exactly. Not converting at all removes the question.
		decoder := json.NewDecoder(bytes.NewReader([]byte(document)))
		decoder.UseNumber()
		var decoded interface{}
		if err := decoder.Decode(&decoded); err != nil {
			return nil, merr.WrapErrParameterInvalidMsg(
				"field %s is not a readable JSON document: %s", field, err.Error())
		}
		normalized, err := json.Marshal(decoded)
		if err != nil {
			return nil, merr.WrapErrParameterInvalidMsg(
				"field %s is not a readable JSON document: %s", field, err.Error())
		}
		if err := checkEngineCompatible(field, string(normalized)); err != nil {
			return nil, err
		}
		return normalized, nil
	}

	return []byte(document), nil
}

// hasLoneSurrogate reports whether raw contains a \uXXXX escape that is half of
// a surrogate pair without its partner.
//
// Such an escape is accepted by the request binder, which silently replaces it
// with U+FFFD, but simdjson refuses to decode the string it belongs to and
// reports STRING_ERROR. Storing the document verbatim would therefore make that
// value unreadable, so the callers fall back to the decoded form, which is what
// was stored before the token was kept.
//
// The scan is skipped entirely for a document with no \u escape at all, which is
// the overwhelmingly common case.
func hasLoneSurrogate(raw string) bool {
	if !strings.Contains(raw, `\u`) {
		return false
	}
	for i := 0; i+5 < len(raw); i++ {
		if raw[i] != '\\' || raw[i+1] != 'u' {
			continue
		}
		// A backslash that is itself escaped does not start an escape.
		backslashes := 0
		for j := i - 1; j >= 0 && raw[j] == '\\'; j-- {
			backslashes++
		}
		if backslashes%2 == 1 {
			continue
		}
		code, err := strconv.ParseUint(raw[i+2:i+6], 16, 32)
		if err != nil {
			continue
		}
		if code < 0xD800 || code > 0xDFFF {
			continue
		}
		if code >= 0xDC00 {
			// a low surrogate can never come first
			return true
		}
		// a high surrogate must be followed by a low one
		if i+11 >= len(raw) || raw[i+6] != '\\' || raw[i+7] != 'u' {
			return true
		}
		low, err := strconv.ParseUint(raw[i+8:i+12], 16, 32)
		if err != nil || low < 0xDC00 || low > 0xDFFF {
			return true
		}
		i += 11
	}
	return false
}

// jsonNumberLiteral keeps the original JSON number literal instead of decoding
// it into int64/float64 and re-encoding it. Both the JSON field and the dynamic
// field are stored as JSON text, so that round trip can only lose information:
// gjson's String() renders numbers through float64, and cast.ToInt64 discards
// its error and yields 0, so 1e300, 1e19 and integers beyond int64 were all
// silently stored as 0.
//
// Values the JSON engine cannot represent are rejected here rather than stored,
// so they surface at insert time instead of failing every query that touches the
// path. Integers are limited to 64 bits because simdjson reports BIGINT_ERROR
// beyond that; a caller that only needs the magnitude can send a floating-point
// literal, and one that needs the exact digits can send a string.
func jsonNumberLiteral(field string, raw string) (json.Number, error) {
	if !strings.ContainsAny(raw, ".eE") {
		if _, err := strconv.ParseInt(raw, 10, 64); err == nil {
			return json.Number(raw), nil
		}
		if _, err := strconv.ParseUint(raw, 10, 64); err == nil {
			return json.Number(raw), nil
		}
		return "", merr.WrapErrParameterInvalidMsg(
			"field %s integer %s exceeds the 64-bit range supported by the JSON engine, "+
				"write it as a floating-point literal or as a string", field, raw)
	}
	// The request binder already rejects numbers outside the float64 range, so
	// this only guards against that gate changing.
	if value, err := strconv.ParseFloat(raw, 64); err != nil || math.IsInf(value, 0) {
		return "", merr.WrapErrParameterInvalidMsg(
			"field %s number %s is outside the range representable as a double", field, raw)
	}
	return json.Number(raw), nil
}

// stringFieldValue converts a JSON value for a VARCHAR or STRING field.
//
// A number is taken from the literal the caller wrote rather than from gjson's
// String(), which renders it through float64 with the 'f' verb: that turned
// 1e300 into a 301 byte decimal expansion, dropped the last digit of
// 9007199254740993.0, and rewrote 1.50 as 1.5.
//
// An object or an array is rejected. Storing its text is never what the caller
// meant and it hides the common mistake of addressing the wrong field.
//
// proxy.http.compatibilityMode restores the previous String() rendering for
// every kind, including the object case.
func stringFieldValue(field string, value gjson.Result, compatibilityMode bool) (string, error) {
	if compatibilityMode {
		return value.String(), nil
	}
	switch value.Type {
	case gjson.Number, gjson.True, gjson.False:
		return value.Raw, nil
	case gjson.JSON:
		kind := "object"
		if value.IsArray() {
			kind = "array"
		}
		return "", merr.WrapErrParameterInvalidMsg(
			"field %s expects a string, got a JSON %s", field, kind)
	default:
		// String, plus Null which the nullable handling above already resolved.
		return value.String(), nil
	}
}

// checkVectorSpelling refuses a vector handed over as the text of its own JSON
// shape: "[0.1, 0.2]" where [0.1, 0.2] was meant.
//
// gjson's String() hands back a string node's content with the quotes removed,
// so the two reach the decoder as the same bytes and the difference is gone
// before the field type is consulted. Nothing decided that a vector may be
// spelled as text; the three branches that read String() rather than the node
// simply could not tell.
//
// Every neighbor that does read the node already refuses it: BinaryVector and
// the two 16-bit floats take a string as their base64 spelling, struct
// sub-vectors ask IsArray, and search reads the raw element -- so a row written
// with the quoted form could not be looked up with it, which is REST
// disagreeing with itself rather than being generous.
//
// A string is a vector only where the type has a base64 spelling for one.
func checkVectorSpelling(field string, dataType schemapb.DataType, value gjson.Result) error {
	if value.Type != gjson.String || vectorAcceptsBase64(dataType) {
		return nil
	}
	return merr.WrapErrParameterInvalidMsg(
		"field %s expects a vector, got the text of one: %s", field, value.Raw)
}

// vectorAcceptsBase64 reports whether a vector of this type may arrive as a
// base64 string rather than as an array of numbers.
//
// Int8Vector is deliberately absent, having been listed here at first: neither
// the insert branch nor serializeInt8Vectors can read base64 for it, and the
// entry exempted it from the check below, where "null" decodes to a nil slice
// without an error and was stored as an empty vector. The list has to say what
// the decoders do, not what the type name suggests.
func vectorAcceptsBase64(dataType schemapb.DataType) bool {
	switch dataType {
	case schemapb.DataType_BinaryVector,
		schemapb.DataType_Float16Vector,
		schemapb.DataType_BFloat16Vector:
		return true
	default:
		return false
	}
}

// nullElementIn returns the position of the first null element in an array
// value. An array has no element-level validity in Milvus, so a null can only
// be stored as the element type's zero value and is never recoverable.
func nullElementIn(value gjson.Result) (int, bool) {
	// A null element can only exist where the letters "null" appear in the
	// text, and a vector's text is digits, brackets and commas. The substring
	// scan is vectorized and the walk below is not, so the walk runs only for
	// the rare value that could contain one; benchmarked, this is the
	// difference between the null rule being free and it doubling the parse.
	if !strings.Contains(value.Raw, "null") {
		return 0, false
	}
	// Only an array has elements to look at. gjson dispatches on the first
	// character, so text that is not JSON at all can still come back as a
	// literal: a base64 binary vector starting with "nu" is read as a partial
	// null, and that result hands itself to ForEach as a single element, which
	// looked like a null at index 0.
	if !value.IsArray() {
		return 0, false
	}

	idx, found := 0, false
	position := 0
	value.ForEach(func(_, element gjson.Result) bool {
		if element.Type == gjson.Null {
			idx, found = position, true
			return false
		}
		position++
		return true
	})
	return idx, found
}

func checkAndSetData(body []byte, collSchema *schemapb.CollectionSchema, partialUpdate bool) ([]map[string]interface{}, map[string][]bool, error) {
	var reallyDataArray []map[string]interface{}
	validDataMap := make(map[string][]bool)
	// Escape hatch for clients that relied on the previous value handling.
	// Read once per request rather than per field.
	compatibilityMode := paramtable.Get().HTTPCfg.CompatibilityMode.GetAsBool()
	nativeJSONResponse := paramtable.Get().HTTPCfg.NativeJSONResponse.GetAsBool()
	dataResult := gjson.GetBytes(body, HTTPRequestData)
	dataResultArray := dataResult.Array()
	if len(dataResultArray) == 0 {
		return reallyDataArray, validDataMap, merr.ErrMissingRequiredParameters
	}

	fieldNames := make([]string, 0, len(collSchema.Fields)+len(collSchema.StructArrayFields))
	for _, field := range collSchema.Fields {
		if field.IsDynamic {
			continue
		}
		fieldNames = append(fieldNames, field.Name)
	}
	for _, structField := range collSchema.StructArrayFields {
		fieldNames = append(fieldNames, structField.GetName())
	}

	for _, data := range dataResultArray {
		reallyData := map[string]interface{}{}
		if data.Type == gjson.JSON {
			for _, structField := range collSchema.StructArrayFields {
				rawValue := gjson.Get(data.Raw, structField.GetName())
				if !rawValue.Exists() {
					if partialUpdate {
						continue
					}
					if structField.GetNullable() {
						validDataMap[structField.GetName()] = append(validDataMap[structField.GetName()], false)
						continue
					}
					// Not gated by compatibilityMode: a missing struct array field
					// already failed before this change, in parseStructArrayRow,
					// so there is no lenient behavior to fall back to.
					return reallyDataArray, validDataMap, merr.WrapErrParameterMissingMsg(
						"field %s is required", structField.GetName())
				}
				if rawValue.Type == gjson.Null {
					if structField.GetNullable() {
						validDataMap[structField.GetName()] = append(validDataMap[structField.GetName()], false)
						continue
					}
					return reallyDataArray, validDataMap, merr.WrapErrParameterInvalidMsg(
						"field %s is not nullable", structField.GetName())
				}
				if structField.GetNullable() {
					validDataMap[structField.GetName()] = append(validDataMap[structField.GetName()], true)
				}
				structRow, err := parseStructArrayRow(rawValue.Raw, structField, compatibilityMode)
				if err != nil {
					return reallyDataArray, validDataMap, err
				}
				reallyData[structField.GetName()] = structRow
			}
			for _, field := range collSchema.Fields {
				if field.IsDynamic {
					continue
				}
				fieldType := field.DataType
				fieldName := field.Name
				fieldValue := data.Get(fieldName)

				// For partial update, missing fields mean "do not update this field".
				// Explicit JSON null is handled below as an update to null for nullable fields.
				if partialUpdate && !fieldValue.Exists() {
					continue
				}

				if field.Nullable || field.DefaultValue != nil {
					if fieldValue.Type == gjson.Null {
						validDataMap[fieldName] = append(validDataMap[fieldName], false)
						continue
					} else {
						validDataMap[fieldName] = append(validDataMap[fieldName], true)
					}
				}

				if fieldType == schemapb.DataType_Text {
					if !fieldValue.Exists() {
						continue
					}
					if fieldValue.Type != gjson.String {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(
							gjson.String, fieldValue.Type, "fieldName: "+fieldName)
					}
				}

				dataString := fieldValue.String()
				// A vector element cannot be null either: the decoder turns one
				// into 0, which is a coordinate the caller never sent. Not gated
				// by compatibilityMode -- a vector is a dense array of numbers
				// with nowhere to record "absent", so there is no previous
				// handling worth restoring, only a silently different point.
				if typeutil.IsVectorType(fieldType) {
					// A whole value of "null" decodes to a nil slice without an
					// error, which is stored as an empty vector -- an empty
					// sparse row for a sparse field. A vector sent as base64
					// never reads as this literal, so nothing legitimate is
					// caught here.
					// "null" is also valid base64 -- it decodes to the three
					// bytes 9e e9 65, a whole dim-24 binary vector -- so only
					// ask this of the types that have no base64 spelling.
					if !compatibilityMode && !vectorAcceptsBase64(fieldType) &&
						strings.TrimSpace(dataString) == "null" {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalidMsg(
							"field %s is null; a vector cannot be null", fieldName)
					}
					if idx, found := nullElementIn(gjson.Parse(dataString)); found {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalidMsg(
							"field %s has a null at index %d; a vector element cannot be null",
							fieldName, idx)
					}
					if !compatibilityMode {
						if err := checkVectorSpelling(fieldName, fieldType, fieldValue); err != nil {
							return reallyDataArray, validDataMap, err
						}
					}
				}
				// if has pass pk than just to try to set it
				if field.IsPrimaryKey && field.AutoID && len(dataString) == 0 {
					continue
				}

				// skip function output field if user didn't provide data,
				// let proxy validate when data is provided
				if field.GetIsFunctionOutput() && dataString == "" {
					continue
				}

				if !compatibilityMode {
					if !fieldValue.Exists() {
						return reallyDataArray, validDataMap, merr.WrapErrParameterMissingMsg("field %s is required", fieldName)
					}
					if fieldValue.Type == gjson.Null {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalidMsg("field %s is not nullable", fieldName)
					}
				}

				switch fieldType {
				case schemapb.DataType_FloatVector:
					if dataString == "" {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], "", "missing vector field: "+fieldName)
					}
					var vectorArray []float32
					err := json.Unmarshal([]byte(dataString), &vectorArray)
					if err != nil {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error())
					}
					reallyData[fieldName] = vectorArray
				case schemapb.DataType_BinaryVector:
					if dataString == "" {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], "", "missing vector field: "+fieldName)
					}
					vectorStr := fieldValue.Raw
					var vectorArray []byte
					err := json.Unmarshal([]byte(vectorStr), &vectorArray)
					if err != nil {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error())
					}
					reallyData[fieldName] = vectorArray
				case schemapb.DataType_SparseFloatVector:
					if dataString == "" {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], "", "missing vector field: "+fieldName)
					}
					sparseVec, err := typeutil.CreateSparseFloatRowFromJSON([]byte(dataString))
					if err != nil {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error())
					}
					reallyData[fieldName] = sparseVec
				case schemapb.DataType_Float16Vector:
					fallthrough
				case schemapb.DataType_BFloat16Vector:
					if dataString == "" {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], "", "missing vector field: "+fieldName)
					}
					vectorJSON := fieldValue
					// Clients may send float32 vector because they are inconvenient of processing float16 or bfloat16.
					// Float32 vector is an array in JSON format, like `[1.0, 2.0, 3.0]`, `[1, 2, 3]`, etc,
					// while float16 or bfloat16 vector is a string in JSON format, like `"4z1jPgAAgL8="`, `"gD+AP4A/gD8="`, etc.
					if vectorJSON.IsArray() {
						// `data` is a float32 vector
						// same as `case schemapb.DataType_FloatVector`
						var vectorArray []float32
						err := json.Unmarshal([]byte(dataString), &vectorArray)
						if err != nil {
							return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error())
						}
						reallyData[fieldName] = vectorArray
					} else if vectorJSON.Type == gjson.String {
						// `data` is a float16 or bfloat16 vector
						// same as `case schemapb.DataType_BinaryVector`
						vectorStr := fieldValue.Raw
						var vectorArray []byte
						err := json.Unmarshal([]byte(vectorStr), &vectorArray)
						if err != nil {
							return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error())
						}
						reallyData[fieldName] = vectorArray
					} else {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, "invalid vector field: "+fieldName)
					}
				case schemapb.DataType_Int8Vector:
					if dataString == "" {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], "", "missing vector field: "+fieldName)
					}
					var vectorArray []int8
					err := json.Unmarshal([]byte(dataString), &vectorArray)
					if err != nil {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error())
					}
					reallyData[fieldName] = vectorArray
				case schemapb.DataType_Bool:
					result, err := cast.ToBoolE(dataString)
					if err != nil {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error())
					}
					reallyData[fieldName] = result
				case schemapb.DataType_Int8:
					if compatibilityMode {
						legacy, err := cast.ToInt8E(dataString)
						if err != nil {
							return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error())
						}
						reallyData[fieldName] = legacy
						break
					}
					result, actual, ok := parseRESTInteger(fieldValue, 8)
					if !ok {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(
							schemapb.DataType_name[int32(fieldType)], actual,
							fmt.Sprintf("field %s value must be an integer in range [%d, %d]", fieldName, math.MinInt8, math.MaxInt8))
					}
					reallyData[fieldName] = int8(result)
				case schemapb.DataType_Int16:
					if compatibilityMode {
						legacy, err := cast.ToInt16E(dataString)
						if err != nil {
							return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error())
						}
						reallyData[fieldName] = legacy
						break
					}
					result, actual, ok := parseRESTInteger(fieldValue, 16)
					if !ok {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(
							schemapb.DataType_name[int32(fieldType)], actual,
							fmt.Sprintf("field %s value must be an integer in range [%d, %d]", fieldName, math.MinInt16, math.MaxInt16))
					}
					reallyData[fieldName] = int16(result)
				case schemapb.DataType_Int32:
					if compatibilityMode {
						legacy, err := cast.ToInt32E(dataString)
						if err != nil {
							return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error())
						}
						reallyData[fieldName] = legacy
						break
					}
					result, actual, ok := parseRESTInteger(fieldValue, 32)
					if !ok {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(
							schemapb.DataType_name[int32(fieldType)], actual,
							fmt.Sprintf("field %s value must be an integer in range [%d, %d]", fieldName, math.MinInt32, math.MaxInt32))
					}
					reallyData[fieldName] = int32(result)
				case schemapb.DataType_Int64:
					// Only the JSON-number form goes through the raw literal.
					// gjson's String() renders a number through float64 as soon
					// as the raw text is not all digits, so 9007199254740993.0
					// reached json.Number as 9007199254740992 and was accepted.
					// Quoted integers keep their base-10 parsing: this path also
					// carries Int64 primary keys, and strconv's base detection
					// would silently reinterpret a zero-padded id such as "010".
					var result int64
					if fieldValue.Type == gjson.Number && !compatibilityMode {
						parsed, ok := parseJSONInteger(fieldValue.Raw, 64)
						if !ok {
							return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(
								schemapb.DataType_name[int32(fieldType)], fieldValue.Raw,
								fmt.Sprintf("field %s value must be an integer in range [%d, %d]",
									fieldName, int64(math.MinInt64), int64(math.MaxInt64)))
						}
						result = parsed
					} else {
						parsed, err := json.Number(dataString).Int64()
						if err != nil {
							return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error())
						}
						result = parsed
					}
					reallyData[fieldName] = result
				case schemapb.DataType_Array:
					// A null element has nowhere to go: an array has no
					// element-level validity, so it can only be dropped into the
					// element's zero value. sonic already refuses one for an
					// integer or string element, but accepts it for a boolean or
					// a float, where it silently became false or 0. Refuse it for
					// every element type, and say which position it was at.
					if !compatibilityMode {
						// Parse dataString, not fieldValue: an array handed over as
						// a JSON string is unwrapped before it is decoded, and
						// checking the wrapper found no elements to look at.
						//
						// Require an array first. A whole value of "null" decodes
						// to a nil slice without an error and was stored as an
						// empty array, and the element scan below has nothing to
						// look at when the value is not an array at all.
						if !gjson.Parse(dataString).IsArray() {
							return reallyDataArray, validDataMap, merr.WrapErrParameterInvalidMsg(
								"field %s is an array field, but the value sent is not an array", fieldName)
						}
						if idx, found := nullElementIn(gjson.Parse(dataString)); found {
							return reallyDataArray, validDataMap, merr.WrapErrParameterInvalidMsg(
								"field %s has a null at index %d; an array element cannot be null",
								fieldName, idx)
						}
					}
					scalar, err := unmarshalScalarArray(field.ElementType, dataString)
					if err != nil {
						// An element can arrive in a form the plain decode does not
						// take: an Int64 rendered as a string because the caller did
						// not allow native Int64, or the quoted numbers and booleans
						// a plain column of the same type has always accepted. Read
						// those the way that column reads them, so a row this API
						// emits can be sent back to it unchanged. Only a payload the
						// decode already rejected reaches here, so nothing that was
						// accepted before is read differently now.
						//
						// compatibilityMode is about values -- it restores the
						// wrapping, truncating conversions of the releases before
						// this validation work -- not about how a value is spelled.
						// The element reader below keeps that split: it reads the
						// quoted spelling in either mode, and leaves the number
						// conversions of each mode alone. Reading a number through
						// gjson here would be the escape hatch growing a new
						// meaning, so this path never does.
						if !gjson.Parse(dataString).IsArray() {
							return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)]+
								" of "+schemapb.DataType_name[int32(field.ElementType)], dataString, err.Error())
						}
						lenient, lenientErr := parseScalarArrayElements(field.ElementType, gjson.Parse(dataString).Array(), false)
						if lenientErr != nil {
							// The element reader says which element failed and why;
							// the decode above only knows the whole value did.
							return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)]+
								" of "+schemapb.DataType_name[int32(field.ElementType)], dataString, lenientErr.Error())
						}
						scalar = lenient
					}
					if scalar != nil {
						reallyData[fieldName] = scalar
					}
				case schemapb.DataType_JSON:
					// Store the original JSON token verbatim. gjson's String()
					// unquotes JSON strings ("hello" -> hello) and renders numbers
					// through float64 (1e400 -> +Inf), both of which are not valid
					// JSON documents and make the stored field unparsable. The
					// request body is already validated as JSON by the gin binder
					// before it reaches here, so Raw is always a well-formed token.
					// A JSON document supplied as a JSON string is still unwrapped,
					// preserving the existing input form.
					if compatibilityMode {
						reallyData[fieldName] = []byte(dataString)
						break
					}
					// What a JSON string means here depends on the shape this
					// deployment serves. While a JSON field reads back as the text
					// of its document, that text is the field's wire form, and
					// decoding it is how a caller sends a document at all -- not a
					// guess about what they meant. Once the field reads back as
					// the document itself, a string is a string: unwrapping it
					// would store a value the caller did not send, and would leave
					// no way to store "123" or "true" as the text it is.
					// The unwrapped token is validated like any other: it used to
					// be trusted, which let an oversized integer, a duplicate key
					// or a lone surrogate in through the string form.
					document := fieldValue.Raw
					if !nativeJSONResponse && fieldValue.Type == gjson.String && json.Valid([]byte(dataString)) {
						document = dataString
					}
					stored, err := jsonDocumentForStorage(fieldName, document)
					if err != nil {
						return reallyDataArray, validDataMap, err
					}
					reallyData[fieldName] = stored
				case schemapb.DataType_Geometry:
					reallyData[fieldName] = dataString
				case schemapb.DataType_Float:
					if !compatibilityMode && fieldValue.Type == gjson.Number {
						// Read through float64 and narrowed, the way every path
						// this value will be compared against reads it: the
						// expression parser, an exprParams value carried as a
						// double, and a row written through pymilvus, whose
						// Python float is a double before it becomes a float32.
						// Parsing straight to float32 rounds once and is the
						// more faithful reading of the decimal, but it lands a
						// literal sitting on a float32 rounding midpoint on a
						// different value than any of those paths produce, and
						// the row cannot then be found with the literal that
						// wrote it. The same dialect everywhere wins.
						//
						// What that costs is one check the float32 parser would
						// have made for free: 3.5e38 is finite as a float64, so
						// the parse succeeds and the narrowing silently yields
						// +Inf -- a value no caller sent. The range is checked
						// here, before the cast destroys the evidence.
						parsed, err := strconv.ParseFloat(fieldValue.Raw, 64)
						if err != nil || math.IsInf(parsed, 0) {
							return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(
								schemapb.DataType_name[int32(fieldType)], fieldValue.Raw, "invalid float value")
						}
						if math.Abs(parsed) > math.MaxFloat32 {
							return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(
								schemapb.DataType_name[int32(fieldType)], fieldValue.Raw,
								"value is outside the float32 range")
						}
						reallyData[fieldName] = float32(parsed)
						break
					}
					result, err := cast.ToFloat32E(dataString)
					if err != nil {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error())
					}
					reallyData[fieldName] = result
				case schemapb.DataType_Double:
					result, err := cast.ToFloat64E(dataString)
					if err != nil {
						return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(fieldType)], dataString, err.Error())
					}
					reallyData[fieldName] = result
				case schemapb.DataType_Timestamptz:
					reallyData[fieldName] = dataString
				case schemapb.DataType_VarChar, schemapb.DataType_String, schemapb.DataType_Text:
					value, err := stringFieldValue(fieldName, fieldValue, compatibilityMode)
					if err != nil {
						return reallyDataArray, validDataMap, err
					}
					reallyData[fieldName] = value
				default:
					return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid("", schemapb.DataType_name[int32(fieldType)], "fieldName: "+fieldName)
				}
			}

			// fill dynamic schema

			// Two keys that differ only in a lone surrogate escape both decode to
			// U+FFFD, so Map() collapses them and one field disappears without a
			// word. The decoded key passes utf8.ValidString, so it has to be
			// caught on the raw text before the map is built.
			if !compatibilityMode {
				var keyErr error
				data.ForEach(func(key, _ gjson.Result) bool {
					if hasLoneSurrogate(key.Raw) {
						keyErr = merr.WrapErrParameterInvalidMsg(
							"field name %s contains an unpaired surrogate, which the JSON engine cannot read", key.Raw)
						return false
					}
					return true
				})
				if keyErr != nil {
					return nil, nil, keyErr
				}
			}

			// Map() keeps the first occurrence when a row declares the same key
			// twice, so {"dyn": 1, "dyn": 2} silently stores 1. Duplicate keys
			// *inside* a dynamic value are rejected, which makes the two
			// inconsistent, but catching this one needs a scan of the whole row
			// before the map is built and the behavior predates this change, so
			// it is left alone deliberately.
			for mapKey, mapValue := range data.Map() {
				if !containsString(fieldNames, mapKey) {
					if collSchema.EnableDynamicField {
						if mapKey == common.MetaFieldName {
							return nil, nil, merr.WrapErrParameterInvalidMsg("use the invalid field name(%s) when enable dynamicField", mapKey)
						}
						// A key is re-encoded with the wrapper below, which turns
						// invalid UTF-8 into U+FFFD. Two different keys can then
						// normalize to the same one, so the stored document ends
						// up declaring a key twice.
						if !compatibilityMode && !utf8.ValidString(mapKey) {
							return nil, nil, merr.WrapErrParameterInvalidMsg(
								"dynamic field name contains invalid UTF-8, which the JSON engine cannot read")
						}
						mapValueStr := mapValue.String()
						switch mapValue.Type {
						case gjson.True, gjson.False:
							reallyData[mapKey] = cast.ToBool(mapValueStr)
						case gjson.String:
							// The value is re-encoded on the way out, which would
							// replace invalid UTF-8 with U+FFFD and silently
							// rewrite what the caller sent.
							if !compatibilityMode && !utf8.ValidString(mapValueStr) {
								return nil, nil, merr.WrapErrParameterInvalidMsg(
									"dynamic field %s contains invalid UTF-8, which the JSON engine cannot read", mapKey)
							}
							reallyData[mapKey] = mapValueStr
						case gjson.Number:
							if compatibilityMode {
								if strings.Contains(mapValue.Raw, ".") {
									reallyData[mapKey] = cast.ToFloat64(mapValue.Raw)
								} else {
									reallyData[mapKey] = cast.ToInt64(mapValueStr)
								}
								break
							}
							number, err := jsonNumberLiteral(mapKey, mapValue.Raw)
							if err != nil {
								return nil, nil, err
							}
							reallyData[mapKey] = number
						case gjson.JSON:
							// Value() decodes the whole subtree into Go values,
							// turning every nested number into a float64, so a
							// nested 9007199254740993 came back as ...992. Keep
							// the subtree as written, once it is known to be
							// readable.
							if compatibilityMode {
								reallyData[mapKey] = mapValue.Value()
								break
							}
							document, err := jsonDocumentForStorage(mapKey, mapValue.Raw)
							if err != nil {
								return nil, nil, err
							}
							reallyData[mapKey] = json.RawMessage(document)
						case gjson.Null:
							// An absent key and an explicit null are different
							// requests: the first says nothing about the field,
							// the second says it should be null. Skipping the
							// null collapsed them, so a partial update could not
							// clear a dynamic field -- {"tag": null} merged to
							// nothing and the old value survived.
							if compatibilityMode {
								break
							}
							reallyData[mapKey] = json.RawMessage("null")
						default:
							mlog.Warn(context.TODO(), "unknown json type found", mlog.Int("mapValue.Type", int(mapValue.Type)))
						}
					} else {
						return nil, nil, merr.WrapErrParameterInvalidMsg("has pass more field without dynamic schema, please check it")
					}
				}
			}

			reallyDataArray = append(reallyDataArray, reallyData)
		} else {
			return reallyDataArray, validDataMap, merr.WrapErrParameterInvalid(gjson.JSON, data.Type, "NULL:0, FALSE:1, NUMBER:2, STRING:3, TRUE:4, JSON:5")
		}
	}
	return reallyDataArray, validDataMap, nil
}

func containsString(arr []string, s string) bool {
	for _, str := range arr {
		if str == s {
			return true
		}
	}
	return false
}

func getDim(field *schemapb.FieldSchema) (int64, error) {
	dimensionInSchema, err := funcutil.GetAttrByKeyFromRepeatedKV(common.DimKey, field.TypeParams)
	if err != nil {
		return 0, err
	}
	dim, err := strconv.Atoi(dimensionInSchema)
	if err != nil {
		return 0, err
	}
	return int64(dim), nil
}

type structArrayRow map[string]interface{}

func structFieldShortName(name string) string {
	short, err := typeutil.ExtractStructFieldName(name)
	if err != nil || short == "" {
		return name
	}
	return short
}

func subShortName(sub *schemapb.FieldSchema) string {
	return structFieldShortName(sub.GetName())
}

func parseStructArrayRow(rawJSON string, structSchema *schemapb.StructArrayFieldSchema, compatibilityMode bool) (structArrayRow, error) {
	if rawJSON == "" {
		return nil, merr.WrapErrParameterInvalidMsg("missing struct array field: %s", structSchema.GetName())
	}
	parsed := gjson.Parse(rawJSON)
	if !parsed.IsArray() {
		return nil, merr.WrapErrParameterInvalidMsg(
			"struct array field %s expects a JSON array of objects", structSchema.GetName())
	}
	elems := parsed.Array()
	collected := make(map[string][]gjson.Result, len(structSchema.GetFields()))
	expected := make(map[string]struct{}, len(structSchema.GetFields()))
	for _, sub := range structSchema.GetFields() {
		key := subShortName(sub)
		collected[key] = make([]gjson.Result, 0, len(elems))
		expected[key] = struct{}{}
	}
	for idx, elem := range elems {
		if elem.Type != gjson.JSON || !elem.IsObject() {
			return nil, merr.WrapErrParameterInvalidMsg(
				"struct array field %s element #%d must be a JSON object", structSchema.GetName(), idx)
		}
		seen := make(map[string]struct{}, len(structSchema.GetFields()))
		elem.ForEach(func(key, value gjson.Result) bool {
			name := key.String()
			if _, ok := expected[name]; !ok {
				return true
			}
			collected[name] = append(collected[name], value)
			seen[name] = struct{}{}
			return true
		})
		if len(seen) != len(expected) {
			for name := range expected {
				if _, ok := seen[name]; !ok {
					return nil, merr.WrapErrParameterInvalidMsg(
						"struct array field %s element #%d missing sub-field %s",
						structSchema.GetName(), idx, name)
				}
			}
		}
	}

	row := structArrayRow{}
	for _, sub := range structSchema.GetFields() {
		key := subShortName(sub)
		vals := collected[key]
		switch sub.GetDataType() {
		case schemapb.DataType_Array:
			scalar, err := buildStructSubArrayScalar(sub, vals, compatibilityMode)
			if err != nil {
				return nil, err
			}
			row[key] = scalar
		case schemapb.DataType_ArrayOfVector:
			vec, err := buildStructSubVectorField(sub, vals)
			if err != nil {
				return nil, err
			}
			row[key] = vec
		default:
			return nil, merr.WrapErrParameterInvalidMsg(
				"sub-field %s of struct %s has unsupported data type %s",
				key, structSchema.GetName(), sub.GetDataType())
		}
	}
	return row, nil
}

func isJSONDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

// parseJSONInteger parses an exact integer from a JSON number literal.
// Integer-valued decimal and exponent forms are accepted without converting
// through float64 or computing attacker-controlled arbitrary-precision powers.
func parseJSONInteger(raw string, bitSize int) (int64, bool) {
	switch bitSize {
	case 8, 16, 32, 64:
	default:
		return 0, false
	}
	if raw == "" {
		return 0, false
	}

	i := 0
	negative := false
	if raw[i] == '-' {
		negative = true
		i++
		if i == len(raw) {
			return 0, false
		}
	}

	// An int64 has at most 19 decimal digits. Keep only the prefix that can
	// possibly survive decimal scaling; the remaining input is still scanned
	// to validate syntax and count significant/trailing digits.
	var significantPrefix [19]byte
	prefixLen := 0
	significantDigits := 0
	trailingZeros := 0
	coefficientIsZero := true
	recordDigit := func(ch byte) {
		if coefficientIsZero {
			if ch == '0' {
				return
			}
			coefficientIsZero = false
		}
		significantDigits++
		if prefixLen < len(significantPrefix) {
			significantPrefix[prefixLen] = ch
			prefixLen++
		}
		if ch == '0' {
			trailingZeros++
		} else {
			trailingZeros = 0
		}
	}

	// JSON integer part: 0 or a non-zero digit followed by digits.
	if raw[i] == '0' {
		recordDigit(raw[i])
		i++
		if i < len(raw) && isJSONDigit(raw[i]) {
			return 0, false
		}
	} else if raw[i] >= '1' && raw[i] <= '9' {
		for i < len(raw) && isJSONDigit(raw[i]) {
			recordDigit(raw[i])
			i++
		}
	} else {
		return 0, false
	}

	fractionDigits := 0
	if i < len(raw) && raw[i] == '.' {
		i++
		fractionStart := i
		for i < len(raw) && isJSONDigit(raw[i]) {
			recordDigit(raw[i])
			fractionDigits++
			i++
		}
		if i == fractionStart {
			return 0, false
		}
	}

	exponentAbs := 0
	exponentNegative := false
	if i < len(raw) && (raw[i] == 'e' || raw[i] == 'E') {
		i++
		if i < len(raw) && (raw[i] == '+' || raw[i] == '-') {
			exponentNegative = raw[i] == '-'
			i++
		}
		exponentStart := i
		// Values beyond this limit cannot bring a non-zero coefficient into
		// the int64 range. Saturating avoids integer overflow while preserving
		// work proportional to the input length.
		exponentLimit := len(raw) + 20
		for i < len(raw) && isJSONDigit(raw[i]) {
			digit := int(raw[i] - '0')
			if exponentAbs < exponentLimit {
				if exponentAbs > (exponentLimit-digit)/10 {
					exponentAbs = exponentLimit
				} else {
					exponentAbs = exponentAbs*10 + digit
				}
			}
			i++
		}
		if i == exponentStart {
			return 0, false
		}
	}
	if i != len(raw) {
		return 0, false
	}
	if coefficientIsZero {
		return 0, true
	}

	trimDigits, appendZeros := 0, 0
	if exponentNegative {
		trimDigits = exponentAbs + fractionDigits
	} else if exponentAbs >= fractionDigits {
		appendZeros = exponentAbs - fractionDigits
	} else {
		trimDigits = fractionDigits - exponentAbs
	}
	if trimDigits > trailingZeros {
		return 0, false
	}

	keptDigits := significantDigits - trimDigits
	if keptDigits <= 0 || keptDigits > prefixLen || appendZeros > len(significantPrefix)-keptDigits {
		return 0, false
	}

	var integerLiteral [20]byte
	integerLen := 0
	if negative {
		integerLiteral[integerLen] = '-'
		integerLen++
	}
	copy(integerLiteral[integerLen:], significantPrefix[:keptDigits])
	integerLen += keptDigits
	for idx := 0; idx < appendZeros; idx++ {
		integerLiteral[integerLen] = '0'
		integerLen++
	}

	value, err := strconv.ParseInt(string(integerLiteral[:integerLen]), 10, bitSize)
	if err != nil {
		return 0, false
	}
	return value, true
}

// parseRESTInteger preserves the raw token for JSON numbers so validation runs
// before gjson can normalize decimal or exponent forms through float64.
//
// Quoted integers are read as base 10. cast used strconv's base detection, so
// a zero-padded id such as "010" became 8 in an Int8/Int16/Int32 field while
// the Int64 field read the same string as 10 through json.Number. Decimal is
// the only reading a REST caller can have meant, and it makes the integer
// types agree.
func parseRESTInteger(value gjson.Result, bitSize int) (int64, string, bool) {
	actual := value.String()
	if value.Type == gjson.Number {
		actual = value.Raw
		parsed, ok := parseJSONInteger(actual, bitSize)
		return parsed, actual, ok
	}

	parsed, err := strconv.ParseInt(actual, 10, bitSize)
	return parsed, actual, err == nil
}

// unmarshalScalarArray decodes an array column the way it has always been
// decoded: straight into the Go slice the element type maps to. It answers a nil
// field, and no error, for an element type this path never supported.
func unmarshalScalarArray(elementType schemapb.DataType, dataString string) (*schemapb.ScalarField, error) {
	switch elementType {
	case schemapb.DataType_Bool:
		arr := make([]bool, 0)
		if err := json.Unmarshal([]byte(dataString), &arr); err != nil {
			return nil, err
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: arr}},
		}, nil
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		arr := make([]int32, 0)
		if err := json.Unmarshal([]byte(dataString), &arr); err != nil {
			return nil, err
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: arr}},
		}, nil
	case schemapb.DataType_Int64:
		arr := make([]int64, 0)
		if err := json.Unmarshal([]byte(dataString), &arr); err != nil {
			return nil, err
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: arr}},
		}, nil
	case schemapb.DataType_Float:
		arr := make([]float32, 0)
		if err := json.Unmarshal([]byte(dataString), &arr); err != nil {
			return nil, err
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: arr}},
		}, nil
	case schemapb.DataType_Double:
		arr := make([]float64, 0)
		if err := json.Unmarshal([]byte(dataString), &arr); err != nil {
			return nil, err
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: arr}},
		}, nil
	case schemapb.DataType_VarChar:
		arr := make([]string, 0)
		if err := json.Unmarshal([]byte(dataString), &arr); err != nil {
			return nil, err
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: arr}},
		}, nil
	default:
		return nil, nil
	}
}

// arrayElementError says which element of an array could not be read and why,
// without naming the field: the same element rules serve a top-level Array
// column and a struct array's sub-field, and each caller words the failure the
// way the rest of its own errors are worded.
type arrayElementError struct {
	index  int
	value  gjson.Result
	reason string
}

func (e *arrayElementError) Error() string {
	return fmt.Sprintf("element %d: %s (value=%s)", e.index, e.reason, e.value.Raw)
}

func elementErr(index int, value gjson.Result, format string, args ...any) error {
	return &arrayElementError{index: index, value: value, reason: fmt.Sprintf(format, args...)}
}

// parseScalarArrayElements turns the elements of one JSON array into a scalar
// array. It is the only place that decides what an array element may look like,
// so a top-level Array column and a struct array's sub-field cannot drift apart.
//
// The accepted spellings match what the same type accepts as a plain column: an
// integer, float or boolean may arrive quoted, because that is how this API
// renders an Int64 without Accept-Type-Allow-Int64 and how callers whose
// language has no distinct numeric types send everything. A quoted integer is
// read in base 10, so a zero-padded id keeps its value. A spelling carries the
// same value either way, so it is read the same in every mode.
//
// legacyNumbers is a different question: whether a JSON number that does not
// denote a value of this type -- a fraction for an integer, a magnitude past the
// type's range -- is converted anyway, truncating or wrapping. Only the struct
// sub-field path under compatibilityMode ever did that, and only it asks for it
// here; nothing else grows that behavior by sharing this reader.
func parseScalarArrayElements(elementType schemapb.DataType, vals []gjson.Result, legacyNumbers bool) (*schemapb.ScalarField, error) {
	switch elementType {
	case schemapb.DataType_Bool:
		arr := make([]bool, 0, len(vals))
		for idx, v := range vals {
			switch v.Type {
			case gjson.True, gjson.False:
				arr = append(arr, v.Bool())
			case gjson.String:
				parsed, err := cast.ToBoolE(v.String())
				if err != nil {
					return nil, elementErr(idx, v, "expect bool")
				}
				arr = append(arr, parsed)
			default:
				return nil, elementErr(idx, v, "expect bool")
			}
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: arr}},
		}, nil
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		bitSize := 32
		minValue, maxValue := int64(math.MinInt32), int64(math.MaxInt32)
		switch elementType {
		case schemapb.DataType_Int8:
			bitSize = 8
			minValue, maxValue = math.MinInt8, math.MaxInt8
		case schemapb.DataType_Int16:
			bitSize = 16
			minValue, maxValue = math.MinInt16, math.MaxInt16
		}
		arr := make([]int32, 0, len(vals))
		for idx, v := range vals {
			if legacyNumbers && v.Type == gjson.Number {
				arr = append(arr, int32(v.Int()))
				continue
			}
			if v.Type != gjson.Number && v.Type != gjson.String {
				return nil, elementErr(idx, v, "expect integer")
			}
			// parseRESTInteger is what a plain Int8/Int16/Int32 column uses: the
			// raw literal for a number, base 10 for a quoted one.
			value, _, ok := parseRESTInteger(v, bitSize)
			if !ok {
				return nil, elementErr(idx, v, "expect integer in range [%d, %d]", minValue, maxValue)
			}
			arr = append(arr, int32(value))
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: arr}},
		}, nil
	case schemapb.DataType_Int64:
		arr := make([]int64, 0, len(vals))
		for idx, v := range vals {
			if legacyNumbers && v.Type == gjson.Number {
				arr = append(arr, v.Int())
				continue
			}
			if v.Type != gjson.Number && v.Type != gjson.String {
				return nil, elementErr(idx, v, "expect integer")
			}
			value, _, ok := parseRESTInteger(v, 64)
			if !ok {
				return nil, elementErr(idx, v, "expect integer in range [%d, %d]",
					int64(math.MinInt64), int64(math.MaxInt64))
			}
			arr = append(arr, value)
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: arr}},
		}, nil
	case schemapb.DataType_Float:
		arr := make([]float32, 0, len(vals))
		for idx, v := range vals {
			switch {
			case v.Type == gjson.Number && legacyNumbers:
				arr = append(arr, float32(v.Float()))
			case v.Type == gjson.Number:
				// through float64 like every path this value will be compared
				// against; see the Float case in checkAndSetData
				parsed, err := strconv.ParseFloat(v.Raw, 64)
				if err != nil {
					return nil, elementErr(idx, v, "expect float in the float32 range")
				}
				// Verified after the cast, which is where a magnitude past the
				// float32 range turns into an infinity.
				value := float32(parsed)
				if typeutil.VerifyFloat(float64(value)) != nil {
					return nil, elementErr(idx, v, "expect float in the float32 range")
				}
				arr = append(arr, value)
			case v.Type == gjson.String:
				// cast.ToFloat32E is what a plain Float column falls back to, but
				// it reads "NaN", "Inf" and "Infinity" -- case-insensitively --
				// without an error, and an array element has no later check that
				// would catch them: checkArrayElement compares the element's type
				// and never calls VerifyFloats32 the way a plain column does. One
				// of those stored is a row that cannot be served, since the
				// encoder fails on it after the status and part of the body are
				// already written.
				parsed, err := cast.ToFloat32E(v.String())
				if err != nil || typeutil.VerifyFloat(float64(parsed)) != nil {
					return nil, elementErr(idx, v, "expect float in the float32 range")
				}
				arr = append(arr, parsed)
			default:
				return nil, elementErr(idx, v, "expect float")
			}
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: arr}},
		}, nil
	case schemapb.DataType_Double:
		arr := make([]float64, 0, len(vals))
		for idx, v := range vals {
			switch {
			case v.Type == gjson.Number && legacyNumbers:
				arr = append(arr, v.Float())
			case v.Type == gjson.Number:
				// gjson reads a magnitude past float64 as +Inf, a value no
				// caller sent and one the decode this falls back from refuses.
				parsed, err := strconv.ParseFloat(v.Raw, 64)
				if err != nil || typeutil.VerifyFloat(parsed) != nil {
					return nil, elementErr(idx, v, "expect double in the float64 range")
				}
				arr = append(arr, parsed)
			case v.Type == gjson.String:
				// strconv reads NaN, Inf and Infinity without an error.
				parsed, err := cast.ToFloat64E(v.String())
				if err != nil || typeutil.VerifyFloat(parsed) != nil {
					return nil, elementErr(idx, v, "expect double in the float64 range")
				}
				arr = append(arr, parsed)
			default:
				return nil, elementErr(idx, v, "expect double")
			}
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: arr}},
		}, nil
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		arr := make([]string, 0, len(vals))
		for idx, v := range vals {
			if v.Type != gjson.String {
				return nil, elementErr(idx, v, "expect string")
			}
			arr = append(arr, v.String())
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: arr}},
		}, nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg("unsupported array element type %s", elementType)
	}
}

func buildStructSubArrayScalar(sub *schemapb.FieldSchema, vals []gjson.Result, compatibilityMode bool) (*schemapb.ScalarField, error) {
	scalar, err := parseScalarArrayElements(sub.GetElementType(), vals, compatibilityMode)
	if err != nil {
		var elemErr *arrayElementError
		if errors.As(err, &elemErr) {
			return nil, wrapStructSubParseError(sub, elemErr.value, elemErr.reason)
		}
		return nil, merr.WrapErrParameterInvalidMsg(
			"sub-field %s has unsupported array element type %s",
			sub.GetName(), sub.GetElementType())
	}
	return scalar, nil
}

func buildStructSubVectorField(sub *schemapb.FieldSchema, vals []gjson.Result) (*schemapb.VectorField, error) {
	dim, err := getDim(sub)
	if err != nil {
		return nil, merr.WrapErrParameterInvalidMsg(
			"sub-field %s: %s", sub.GetName(), err.Error())
	}

	// A null coordinate decodes to 0, which is a coordinate the caller never
	// sent and which takes full part in the distance computation. A vector is a
	// dense fixed-width array of numbers with no per-element validity to record
	// "absent" in -- VectorField carries only a dim and a packed array -- so
	// there is nothing to store but a number. Not gated by compatibilityMode:
	// there is no such thing as a null coordinate to stay compatible with.
	//
	// The check on top-level vectors runs in checkAndSetData, which never sees
	// these: a struct's sub-vectors are decoded here instead. nullElementIn
	// ignores anything that is not an array, so a base64 row is left alone.
	for _, v := range vals {
		if idx, found := nullElementIn(v); found {
			return nil, wrapStructSubParseError(sub, v,
				fmt.Sprintf("null at index %d; a vector element cannot be null", idx))
		}
	}

	switch sub.GetElementType() {
	case schemapb.DataType_FloatVector:
		packed := &schemapb.FloatArray{}
		for _, v := range vals {
			if !v.IsArray() {
				return nil, wrapStructSubParseError(sub, v, "expect float vector array")
			}
			var row []float32
			if err := json.Unmarshal([]byte(v.Raw), &row); err != nil {
				return nil, wrapStructSubParseError(sub, v, err.Error())
			}
			if int64(len(row)) != dim {
				return nil, merr.WrapErrParameterInvalidMsg(
					"sub-field %s vector dim mismatch: expect %d, got %d",
					sub.GetName(), dim, len(row))
			}
			packed.Data = append(packed.Data, row...)
		}
		return &schemapb.VectorField{
			Dim:  dim,
			Data: &schemapb.VectorField_FloatVector{FloatVector: packed},
		}, nil
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		isFloat16 := sub.GetElementType() == schemapb.DataType_Float16Vector
		bytesPerVec := dim * 2
		buf := make([]byte, 0, int(bytesPerVec)*len(vals))
		for _, v := range vals {
			b, err := decodeByteVectorElement(v, dim, bytesPerVec, isFloat16)
			if err != nil {
				return nil, wrapStructSubParseError(sub, v, err.Error())
			}
			buf = append(buf, b...)
		}
		if isFloat16 {
			return &schemapb.VectorField{
				Dim:  dim,
				Data: &schemapb.VectorField_Float16Vector{Float16Vector: buf},
			}, nil
		}
		return &schemapb.VectorField{
			Dim:  dim,
			Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: buf},
		}, nil
	case schemapb.DataType_BinaryVector:
		bytesPerVec := dim / 8
		buf := make([]byte, 0, int(bytesPerVec)*len(vals))
		for _, v := range vals {
			if v.Type != gjson.String {
				return nil, wrapStructSubParseError(sub, v, "binary vector must be base64-encoded string")
			}
			var row []byte
			if err := json.Unmarshal([]byte(v.Raw), &row); err != nil {
				return nil, wrapStructSubParseError(sub, v, err.Error())
			}
			if int64(len(row)) != bytesPerVec {
				return nil, merr.WrapErrParameterInvalidMsg(
					"sub-field %s binary vector byte-length mismatch: expect %d, got %d",
					sub.GetName(), bytesPerVec, len(row))
			}
			buf = append(buf, row...)
		}
		return &schemapb.VectorField{
			Dim:  dim,
			Data: &schemapb.VectorField_BinaryVector{BinaryVector: buf},
		}, nil
	case schemapb.DataType_Int8Vector:
		buf := make([]byte, 0, int(dim)*len(vals))
		for _, v := range vals {
			if !v.IsArray() {
				return nil, wrapStructSubParseError(sub, v, "expect int8 vector array")
			}
			var row []int8
			if err := json.Unmarshal([]byte(v.Raw), &row); err != nil {
				return nil, wrapStructSubParseError(sub, v, err.Error())
			}
			if int64(len(row)) != dim {
				return nil, merr.WrapErrParameterInvalidMsg(
					"sub-field %s int8 vector dim mismatch: expect %d, got %d",
					sub.GetName(), dim, len(row))
			}
			for _, x := range row {
				buf = append(buf, byte(x))
			}
		}
		return &schemapb.VectorField{
			Dim:  dim,
			Data: &schemapb.VectorField_Int8Vector{Int8Vector: buf},
		}, nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg(
			"sub-field %s has unsupported vector element type %s",
			sub.GetName(), sub.GetElementType())
	}
}

func decodeByteVectorElement(v gjson.Result, dim, bytesPerVec int64, isFloat16 bool) ([]byte, error) {
	if v.IsArray() {
		var row []float32
		if err := json.Unmarshal([]byte(v.Raw), &row); err != nil {
			return nil, err
		}
		if int64(len(row)) != dim {
			return nil, merr.WrapErrParameterInvalidMsg("vector dim mismatch: expect %d, got %d", dim, len(row))
		}
		if isFloat16 {
			return typeutil.Float32ArrayToFloat16Bytes(row), nil
		}
		return typeutil.Float32ArrayToBFloat16Bytes(row), nil
	}
	if v.Type != gjson.String {
		return nil, merr.WrapErrParameterInvalidMsg("expect float vector array or base64 string")
	}
	var row []byte
	if err := json.Unmarshal([]byte(v.Raw), &row); err != nil {
		return nil, err
	}
	if int64(len(row)) != bytesPerVec {
		return nil, merr.WrapErrParameterInvalidMsg("byte length mismatch: expect %d, got %d", bytesPerVec, len(row))
	}
	return row, nil
}

func wrapStructSubParseError(sub *schemapb.FieldSchema, v gjson.Result, msg string) error {
	return merr.WrapErrParameterInvalidMsg(
		"sub-field %s parse error: %s (value=%s)", sub.GetName(), msg, v.Raw)
}

func isEmbeddingListData(body string) bool {
	raw := gjson.Get(body, HTTPRequestData)
	if !raw.IsArray() {
		return false
	}
	arr := raw.Array()
	if len(arr) == 0 {
		return false
	}
	first := arr[0]
	if first.Type == gjson.String {
		return false
	}
	if !first.IsArray() {
		return false
	}
	inner := first.Array()
	if len(inner) == 0 {
		return false
	}
	firstInner := inner[0]
	return firstInner.IsArray() || firstInner.Type == gjson.String
}

func convertEmbListQueries2Placeholder(body string, elemType schemapb.DataType, dim int64) (*commonpb.PlaceholderValue, error) {
	raw := gjson.Get(body, HTTPRequestData)
	if !raw.IsArray() {
		return nil, merr.WrapErrParameterInvalidMsg("search data must be an array of embedding lists")
	}
	queries := raw.Array()
	if len(queries) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("search data is empty")
	}

	placeholderType, err := embListPlaceholderType(elemType)
	if err != nil {
		return nil, err
	}

	values := make([][]byte, 0, len(queries))
	for qIdx, q := range queries {
		if !q.IsArray() {
			return nil, merr.WrapErrParameterInvalidMsg(
				"search data[%d] must be an array of vectors", qIdx)
		}
		vecs := q.Array()
		if len(vecs) == 0 {
			return nil, merr.WrapErrParameterInvalidMsg(
				"search data[%d] embedding list is empty", qIdx)
		}
		buf, err := encodeEmbListQuery(vecs, elemType, dim, qIdx)
		if err != nil {
			return nil, err
		}
		values = append(values, buf)
	}
	return &commonpb.PlaceholderValue{
		Tag:    "$0",
		Type:   placeholderType,
		Values: values,
	}, nil
}

func embListPlaceholderType(elemType schemapb.DataType) (commonpb.PlaceholderType, error) {
	switch elemType {
	case schemapb.DataType_FloatVector:
		return commonpb.PlaceholderType_EmbListFloatVector, nil
	case schemapb.DataType_Float16Vector:
		return commonpb.PlaceholderType_EmbListFloat16Vector, nil
	case schemapb.DataType_BFloat16Vector:
		return commonpb.PlaceholderType_EmbListBFloat16Vector, nil
	case schemapb.DataType_BinaryVector:
		return commonpb.PlaceholderType_EmbListBinaryVector, nil
	case schemapb.DataType_Int8Vector:
		return commonpb.PlaceholderType_EmbListInt8Vector, nil
	default:
		return 0, merr.WrapErrParameterInvalidMsg(
			"unsupported embedding list element type %s", elemType)
	}
}

func encodeEmbListQuery(vecs []gjson.Result, elemType schemapb.DataType, dim int64, qIdx int) ([]byte, error) {
	// Same rule as a plain search vector: a null coordinate decodes to 0 and
	// then passes the dimension check, so the search runs against a point the
	// caller never sent. nullElementIn ignores anything that is not an array, so
	// a base64 row is left alone.
	for vIdx, v := range vecs {
		if idx, found := nullElementIn(v); found {
			return nil, merr.WrapErrParameterInvalidMsg(
				"search data[%d][%d] has a null at index %d; a vector element cannot be null",
				qIdx, vIdx, idx)
		}
	}

	switch elemType {
	case schemapb.DataType_FloatVector:
		buf := make([]byte, 0, int(dim*4)*len(vecs))
		for vIdx, v := range vecs {
			if !v.IsArray() {
				return nil, merr.WrapErrParameterInvalidMsg(
					"search data[%d][%d] must be a float vector array", qIdx, vIdx)
			}
			var row []float32
			if err := json.Unmarshal([]byte(v.Raw), &row); err != nil {
				return nil, merr.WrapErrParameterInvalidMsg(
					"search data[%d][%d] parse fail: %s", qIdx, vIdx, err.Error())
			}
			if int64(len(row)) != dim {
				return nil, merr.WrapErrParameterInvalidMsg(
					"search data[%d][%d] dim mismatch: expect %d got %d", qIdx, vIdx, dim, len(row))
			}
			buf = append(buf, typeutil.Float32ArrayToBytes(row)...)
		}
		return buf, nil
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		isFloat16 := elemType == schemapb.DataType_Float16Vector
		bytesPerVec := dim * 2
		buf := make([]byte, 0, int(bytesPerVec)*len(vecs))
		for vIdx, v := range vecs {
			b, err := decodeByteVectorElement(v, dim, bytesPerVec, isFloat16)
			if err != nil {
				return nil, merr.WrapErrParameterInvalidMsg(
					"search data[%d][%d]: %s", qIdx, vIdx, err.Error())
			}
			buf = append(buf, b...)
		}
		return buf, nil
	case schemapb.DataType_BinaryVector:
		bytesPerVec := dim / 8
		buf := make([]byte, 0, int(bytesPerVec)*len(vecs))
		for vIdx, v := range vecs {
			if v.Type != gjson.String {
				return nil, merr.WrapErrParameterInvalidMsg(
					"search data[%d][%d] binary vector must be base64-encoded string", qIdx, vIdx)
			}
			var row []byte
			if err := json.Unmarshal([]byte(v.Raw), &row); err != nil {
				return nil, merr.WrapErrParameterInvalidMsg(
					"search data[%d][%d] parse fail: %s", qIdx, vIdx, err.Error())
			}
			if int64(len(row)) != bytesPerVec {
				return nil, merr.WrapErrParameterInvalidMsg(
					"search data[%d][%d] byte-length mismatch: expect %d got %d", qIdx, vIdx, bytesPerVec, len(row))
			}
			buf = append(buf, row...)
		}
		return buf, nil
	case schemapb.DataType_Int8Vector:
		buf := make([]byte, 0, int(dim)*len(vecs))
		for vIdx, v := range vecs {
			if !v.IsArray() {
				return nil, merr.WrapErrParameterInvalidMsg(
					"search data[%d][%d] must be an int8 vector array", qIdx, vIdx)
			}
			var row []int8
			if err := json.Unmarshal([]byte(v.Raw), &row); err != nil {
				return nil, merr.WrapErrParameterInvalidMsg(
					"search data[%d][%d] parse fail: %s", qIdx, vIdx, err.Error())
			}
			if int64(len(row)) != dim {
				return nil, merr.WrapErrParameterInvalidMsg(
					"search data[%d][%d] dim mismatch: expect %d got %d", qIdx, vIdx, dim, len(row))
			}
			buf = append(buf, typeutil.Int8ArrayToBytes(row)...)
		}
		return buf, nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg(
			"unsupported embedding list element type %s", elemType)
	}
}

func buildStructArrayFieldData(structSchema *schemapb.StructArrayFieldSchema, perRow []structArrayRow) (*schemapb.FieldData, error) {
	return buildStructArrayFieldDataInternal(structSchema, perRow, nil)
}

func buildNullableStructArrayFieldData(structSchema *schemapb.StructArrayFieldSchema, perRow []structArrayRow, validData []bool) (*schemapb.FieldData, error) {
	return buildStructArrayFieldDataInternal(structSchema, perRow, validData)
}

func buildStructArrayFieldDataInternal(structSchema *schemapb.StructArrayFieldSchema, perRow []structArrayRow, validData []bool) (*schemapb.FieldData, error) {
	if len(perRow) == 0 && len(validData) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("struct array field %s has no rows", structSchema.GetName())
	}
	if len(validData) > 0 {
		validCount := 0
		for _, valid := range validData {
			if valid {
				validCount++
			}
		}
		if validCount != len(perRow) {
			return nil, merr.WrapErrServiceInternalMsg(
				"struct array field %s has %d valid rows but %d payload rows",
				structSchema.GetName(), validCount, len(perRow))
		}
	}
	subs := structSchema.GetFields()
	subFieldData := make([]*schemapb.FieldData, 0, len(subs))
	for _, sub := range subs {
		short := subShortName(sub)
		switch sub.GetDataType() {
		case schemapb.DataType_Array:
			arrayArray := &schemapb.ArrayArray{
				Data:        make([]*schemapb.ScalarField, 0, len(perRow)),
				ElementType: sub.GetElementType(),
			}
			for rowIdx, row := range perRow {
				val, ok := row[short]
				if !ok {
					return nil, merr.WrapErrParameterInvalidMsg("struct %s row %d missing sub-field %s",
						structSchema.GetName(), rowIdx, short)
				}
				scalar, ok := val.(*schemapb.ScalarField)
				if !ok {
					return nil, merr.WrapErrParameterInvalidMsg("struct %s sub-field %s row %d: unexpected payload type %T",
						structSchema.GetName(), short, rowIdx, val)
				}
				arrayArray.Data = append(arrayArray.Data, scalar)
			}
			subFieldData = append(subFieldData, &schemapb.FieldData{
				Type:      schemapb.DataType_Array,
				FieldName: short,
				FieldId:   sub.GetFieldID(),
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_ArrayData{ArrayData: arrayArray},
					},
				},
			})
			typeutil.SetFieldDataValidData(subFieldData[len(subFieldData)-1], validData)
		case schemapb.DataType_ArrayOfVector:
			dim, err := getDim(sub)
			if err != nil {
				return nil, err
			}
			vecArray := &schemapb.VectorArray{
				ElementType: sub.GetElementType(),
				Dim:         dim,
				Data:        make([]*schemapb.VectorField, 0, len(perRow)),
			}
			for rowIdx, row := range perRow {
				val, ok := row[short]
				if !ok {
					return nil, merr.WrapErrParameterInvalidMsg("struct %s row %d missing sub-field %s",
						structSchema.GetName(), rowIdx, short)
				}
				vf, ok := val.(*schemapb.VectorField)
				if !ok {
					return nil, merr.WrapErrParameterInvalidMsg("struct %s sub-field %s row %d: unexpected payload type %T",
						structSchema.GetName(), short, rowIdx, val)
				}
				vecArray.Data = append(vecArray.Data, vf)
			}
			subFieldData = append(subFieldData, &schemapb.FieldData{
				Type:      schemapb.DataType_ArrayOfVector,
				FieldName: short,
				FieldId:   sub.GetFieldID(),
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim: dim,
						Data: &schemapb.VectorField_VectorArray{
							VectorArray: vecArray,
						},
					},
				},
			})
			typeutil.SetFieldDataValidData(subFieldData[len(subFieldData)-1], validData)
		default:
			return nil, merr.WrapErrParameterInvalidMsg("unsupported struct sub-field data type: %s", sub.GetDataType())
		}
	}
	return &schemapb.FieldData{
		Type:      schemapb.DataType_ArrayOfStruct,
		FieldName: structSchema.GetName(),
		FieldId:   structSchema.GetFieldID(),
		Field: &schemapb.FieldData_StructArrays{
			StructArrays: &schemapb.StructArrayField{Fields: subFieldData},
		},
	}, nil
}

type structArrayRowAccessor struct {
	fieldData    *schemapb.FieldData
	subFields    []*schemapb.FieldData
	subAccessors []*fieldDataRowAccessor
	subDims      map[string]int64
}

func newStructArrayRowAccessor(fd *schemapb.FieldData, schema *schemapb.CollectionSchema) (*structArrayRowAccessor, error) {
	subs := fd.GetStructArrays().GetFields()
	accessor := &structArrayRowAccessor{
		fieldData: fd,
		subFields: subs,
	}
	if len(subs) == 0 {
		return accessor, nil
	}

	expectedValidData := typeutil.GetFieldDataValidData(subs[0])
	accessor.subAccessors = make([]*fieldDataRowAccessor, 0, len(subs))
	for _, sub := range subs {
		if !slices.Equal(expectedValidData, typeutil.GetFieldDataValidData(sub)) {
			return nil, merr.WrapErrServiceInternalMsg(
				"struct array field %s sub-field %s has inconsistent valid data",
				fd.GetFieldName(), sub.GetFieldName())
		}
		subAccessor, err := newFieldDataRowAccessor(sub)
		if err != nil {
			return nil, merr.WrapErrServiceInternalErr(err,
				"invalid struct array field %s sub-field %s", fd.GetFieldName(), sub.GetFieldName())
		}
		accessor.subAccessors = append(accessor.subAccessors, subAccessor)
	}

	var err error
	accessor.subDims, err = structArraySubDims(fd.GetFieldName(), schema)
	if err != nil {
		return nil, err
	}
	return accessor, nil
}

func (accessor *structArrayRowAccessor) row(rowIdx int, enableInt64 bool) ([]map[string]interface{}, error) {
	fd := accessor.fieldData
	subs := accessor.subFields
	if len(subs) == 0 {
		return []map[string]interface{}{}, nil
	}
	rowIndices := make([]int, len(subs))
	rowValid := true
	for idx, sub := range subs {
		dataIdx, valid, err := accessor.subAccessors[idx].rowIndex(int64(rowIdx))
		if err != nil {
			return nil, merr.WrapErrServiceInternalErr(err,
				"read struct array field %s sub-field %s", fd.GetFieldName(), sub.GetFieldName())
		}
		if idx == 0 {
			rowValid = valid
		} else if valid != rowValid {
			return nil, merr.WrapErrServiceInternalMsg(
				"struct array field %s sub-field %s has inconsistent null state at row %d",
				fd.GetFieldName(), sub.GetFieldName(), rowIdx)
		}
		rowIndices[idx] = int(dataIdx)
	}
	if !rowValid {
		return nil, nil
	}

	elemCount, err := structSubElemCount(subs[0], rowIndices[0], accessor.subDims)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]interface{}, elemCount)
	for i := 0; i < elemCount; i++ {
		out[i] = make(map[string]interface{}, len(subs))
	}
	for subIdx, sub := range subs {
		dataIdx := rowIndices[subIdx]
		short := structFieldShortName(sub.GetFieldName())
		switch sub.GetType() {
		case schemapb.DataType_Array:
			rowData := sub.GetScalars().GetArrayData().GetData()
			if dataIdx >= len(rowData) {
				return nil, merr.WrapErrParameterInvalidMsg("struct sub-field %s missing row %d", short, dataIdx)
			}
			values := scalarArrayToInterfaces(rowData[dataIdx], enableInt64)
			if len(values) != elemCount {
				return nil, merr.WrapErrParameterInvalidMsg("struct sub-field %s element count mismatch: expect %d got %d",
					short, elemCount, len(values))
			}
			for i, v := range values {
				out[i][short] = v
			}
		case schemapb.DataType_ArrayOfVector:
			va := sub.GetVectors().GetVectorArray()
			if va == nil {
				return nil, merr.WrapErrParameterInvalidMsg("struct sub-field %s has no vector array", short)
			}
			if dataIdx >= len(va.GetData()) {
				return nil, merr.WrapErrParameterInvalidMsg("struct sub-field %s missing row %d", short, dataIdx)
			}
			dim, ok := accessor.subDims[short]
			if !ok || dim <= 0 {
				return nil, merr.WrapErrParameterInvalidMsg("schema missing dim for struct sub-field %s", short)
			}
			values, err := vectorFieldToInterfaces(va.GetData()[dataIdx], va.GetElementType(), dim)
			if err != nil {
				return nil, err
			}
			if len(values) != elemCount {
				return nil, merr.WrapErrParameterInvalidMsg("struct sub-field %s vector element count mismatch: expect %d got %d",
					short, elemCount, len(values))
			}
			for i, v := range values {
				out[i][short] = v
			}
		default:
			return nil, merr.WrapErrParameterInvalidMsg("unsupported struct sub-field type %s", sub.GetType())
		}
	}
	return out, nil
}

func structArraySubDims(fieldName string, schema *schemapb.CollectionSchema) (map[string]int64, error) {
	subDims := make(map[string]int64)
	for _, sf := range schema.GetStructArrayFields() {
		if sf.GetName() != fieldName {
			continue
		}
		for _, sub := range sf.GetFields() {
			if sub.GetDataType() != schemapb.DataType_ArrayOfVector {
				continue
			}
			dim, err := getDim(sub)
			if err != nil {
				return nil, merr.WrapErrParameterInvalidErr(err, "schema sub-field %s has no dim", sub.GetName())
			}
			subDims[subShortName(sub)] = dim
		}
		break
	}
	return subDims, nil
}

func structSubElemCount(sub *schemapb.FieldData, rowIdx int, subDims map[string]int64) (int, error) {
	switch sub.GetType() {
	case schemapb.DataType_Array:
		rowData := sub.GetScalars().GetArrayData().GetData()
		if rowIdx >= len(rowData) {
			return 0, merr.WrapErrParameterInvalidMsg("struct sub-field %s row %d out of range", sub.GetFieldName(), rowIdx)
		}
		// Element count is independent of the Int64 response mode; pass true to
		// skip the per-element string conversion.
		return len(scalarArrayToInterfaces(rowData[rowIdx], true)), nil
	case schemapb.DataType_ArrayOfVector:
		va := sub.GetVectors().GetVectorArray()
		if va == nil || rowIdx >= len(va.GetData()) {
			return 0, merr.WrapErrParameterInvalidMsg("struct sub-field %s row %d out of range", sub.GetFieldName(), rowIdx)
		}
		short := structFieldShortName(sub.GetFieldName())
		dim, ok := subDims[short]
		if !ok || dim <= 0 {
			return 0, merr.WrapErrParameterInvalidMsg("schema missing dim for struct sub-field %s", short)
		}
		return vectorFieldElemCount(va.GetData()[rowIdx], va.GetElementType(), dim)
	default:
		return 0, merr.WrapErrParameterInvalidMsg("unsupported struct sub-field type %s", sub.GetType())
	}
}

// scalarArrayToInterfaces flattens one array row into per-element values so the
// caller can zip them into the struct's per-element maps. enableInt64 carries the
// Accept-Type-Allow-Int64 semantics down to Int64 elements, matching what
// scalarFieldToRESTAny does for top-level Array fields.
func scalarArrayToInterfaces(sf *schemapb.ScalarField, enableInt64 bool) []interface{} {
	switch sf.GetData().(type) {
	case *schemapb.ScalarField_BoolData:
		src := sf.GetBoolData().GetData()
		out := make([]interface{}, len(src))
		for i, v := range src {
			out[i] = v
		}
		return out
	case *schemapb.ScalarField_IntData:
		src := sf.GetIntData().GetData()
		out := make([]interface{}, len(src))
		for i, v := range src {
			out[i] = v
		}
		return out
	case *schemapb.ScalarField_LongData:
		src := sf.GetLongData().GetData()
		out := make([]interface{}, len(src))
		for i, v := range src {
			if enableInt64 {
				out[i] = v
			} else {
				out[i] = strconv.FormatInt(v, 10)
			}
		}
		return out
	case *schemapb.ScalarField_FloatData:
		src := sf.GetFloatData().GetData()
		out := make([]interface{}, len(src))
		for i, v := range src {
			out[i] = v
		}
		return out
	case *schemapb.ScalarField_DoubleData:
		src := sf.GetDoubleData().GetData()
		out := make([]interface{}, len(src))
		for i, v := range src {
			out[i] = v
		}
		return out
	case *schemapb.ScalarField_StringData:
		src := sf.GetStringData().GetData()
		out := make([]interface{}, len(src))
		for i, v := range src {
			out[i] = v
		}
		return out
	default:
		return nil
	}
}

func vectorFieldElemCount(vf *schemapb.VectorField, elemType schemapb.DataType, dim int64) (int, error) {
	if dim <= 0 {
		return 0, merr.WrapErrParameterInvalidMsg("invalid dim %d", dim)
	}
	switch elemType {
	case schemapb.DataType_FloatVector:
		return len(vf.GetFloatVector().GetData()) / int(dim), nil
	case schemapb.DataType_Float16Vector:
		return len(vf.GetFloat16Vector()) / int(dim*2), nil
	case schemapb.DataType_BFloat16Vector:
		return len(vf.GetBfloat16Vector()) / int(dim*2), nil
	case schemapb.DataType_BinaryVector:
		return len(vf.GetBinaryVector()) / int(dim/8), nil
	case schemapb.DataType_Int8Vector:
		return len(vf.GetInt8Vector()) / int(dim), nil
	default:
		return 0, merr.WrapErrParameterInvalidMsg("unsupported vector element type %s", elemType)
	}
}

func vectorFieldToInterfaces(vf *schemapb.VectorField, elemType schemapb.DataType, dim int64) ([]interface{}, error) {
	if dim <= 0 {
		return nil, merr.WrapErrParameterInvalidMsg("invalid dim %d", dim)
	}
	switch elemType {
	case schemapb.DataType_FloatVector:
		buf := vf.GetFloatVector().GetData()
		count := len(buf) / int(dim)
		out := make([]interface{}, count)
		for i := 0; i < count; i++ {
			out[i] = buf[i*int(dim) : (i+1)*int(dim)]
		}
		return out, nil
	case schemapb.DataType_Float16Vector:
		buf := vf.GetFloat16Vector()
		step := int(dim * 2)
		count := len(buf) / step
		out := make([]interface{}, count)
		for i := 0; i < count; i++ {
			out[i] = base64.StdEncoding.EncodeToString(buf[i*step : (i+1)*step])
		}
		return out, nil
	case schemapb.DataType_BFloat16Vector:
		buf := vf.GetBfloat16Vector()
		step := int(dim * 2)
		count := len(buf) / step
		out := make([]interface{}, count)
		for i := 0; i < count; i++ {
			out[i] = base64.StdEncoding.EncodeToString(buf[i*step : (i+1)*step])
		}
		return out, nil
	case schemapb.DataType_BinaryVector:
		buf := vf.GetBinaryVector()
		step := int(dim / 8)
		count := len(buf) / step
		out := make([]interface{}, count)
		for i := 0; i < count; i++ {
			out[i] = base64.StdEncoding.EncodeToString(buf[i*step : (i+1)*step])
		}
		return out, nil
	case schemapb.DataType_Int8Vector:
		buf := vf.GetInt8Vector()
		step := int(dim)
		count := len(buf) / step
		out := make([]interface{}, count)
		for i := 0; i < count; i++ {
			seg := buf[i*step : (i+1)*step]
			row := make([]int8, step)
			for j, b := range seg {
				row[j] = int8(b)
			}
			out[i] = row
		}
		return out, nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg("unsupported vector element type %s", elemType)
	}
}

func convertFloatVectorToArray(vector [][]float32, dim int64) ([]float32, error) {
	floatArray := make([]float32, 0)
	for _, arr := range vector {
		if int64(len(arr)) != dim {
			return nil, merr.WrapErrParameterInvalidMsg("[]float32 size %d doesn't equal to vector dimension %d of %s",
				len(arr), dim, schemapb.DataType_name[int32(schemapb.DataType_FloatVector)])
		}
		for i := int64(0); i < dim; i++ {
			floatArray = append(floatArray, arr[i])
		}
	}
	return floatArray, nil
}

func convertBinaryVectorToArray(vector [][]byte, dim int64, dataType schemapb.DataType) ([]byte, error) {
	var bytesLen int64
	switch dataType {
	case schemapb.DataType_BinaryVector:
		bytesLen = dim / 8
	case schemapb.DataType_Float16Vector:
		bytesLen = dim * 2
	case schemapb.DataType_BFloat16Vector:
		bytesLen = dim * 2
	}
	binaryArray := make([]byte, 0, len(vector)*int(bytesLen))
	for _, arr := range vector {
		if int64(len(arr)) != bytesLen {
			return nil, merr.WrapErrParameterInvalidMsg("[]byte size %d doesn't equal to vector dimension %d of %s",
				len(arr), dim, schemapb.DataType_name[int32(dataType)])
		}
		for i := int64(0); i < bytesLen; i++ {
			binaryArray = append(binaryArray, arr[i])
		}
	}
	return binaryArray, nil
}

func convertInt8VectorToArray(vector [][]int8, dim int64) ([]byte, error) {
	byteArray := make([]byte, 0)
	for _, arr := range vector {
		if int64(len(arr)) != dim {
			return nil, merr.WrapErrParameterInvalidMsg("[]int8 size %d doesn't equal to vector dimension %d of %s",
				len(arr), dim, schemapb.DataType_name[int32(schemapb.DataType_Int8Vector)])
		}
		for i := int64(0); i < dim; i++ {
			byteArray = append(byteArray, byte(arr[i]))
		}
	}
	return byteArray, nil
}

type fieldCandi struct {
	name    string
	v       reflect.Value
	options map[string]string
}

func reflectValueCandi(v reflect.Value) (map[string]fieldCandi, error) {
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	result := make(map[string]fieldCandi)
	switch v.Kind() {
	case reflect.Map: // map[string]interface{}
		iter := v.MapRange()
		for iter.Next() {
			key := iter.Key().String()
			result[key] = fieldCandi{
				name: key,
				v:    iter.Value(),
			}
		}
		return result, nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg("unsupport row type: %s", v.Kind().String())
	}
}

func convertToIntArray(dataType schemapb.DataType, arr interface{}) []int32 {
	var res []int32
	switch dataType {
	case schemapb.DataType_Int8:
		for _, num := range arr.([]int8) {
			res = append(res, int32(num))
		}
	case schemapb.DataType_Int16:
		for _, num := range arr.([]int16) {
			res = append(res, int32(num))
		}
	}
	return res
}

func anyToColumns(rows []map[string]interface{}, validDataMap map[string][]bool, sch *schemapb.CollectionSchema, inInsert bool, partialUpdate bool) ([]*schemapb.FieldData, error) {
	rowsLen := len(rows)
	if rowsLen == 0 {
		return []*schemapb.FieldData{}, merr.WrapErrParameterInvalidMsg("no row need to be convert to columns")
	}

	isDynamic := sch.EnableDynamicField
	allowInsertAutoID, _ := common.IsAllowInsertAutoID(sch.GetProperties()...)
	isAutoIDPK := false
	pkFieldName := ""

	nameColumns := make(map[string]interface{})
	nameDims := make(map[string]int64)
	fieldData := make(map[string]*schemapb.FieldData)

	// Pre-compute the set of field names present across all rows,
	// so we can skip absent function output fields with a map lookup instead of scanning rows.
	presentFieldNames := make(map[string]struct{})
	for _, row := range rows {
		for name := range row {
			presentFieldNames[name] = struct{}{}
		}
	}

	for _, field := range sch.Fields {
		if field.IsPrimaryKey {
			pkFieldName = field.Name
			if field.AutoID {
				isAutoIDPK = true
			}
		}
		if (field.IsPrimaryKey && field.AutoID && inInsert && !allowInsertAutoID) || field.IsDynamic {
			continue
		}
		// skip function output field if no row provides data for it
		if field.GetIsFunctionOutput() {
			if _, ok := presentFieldNames[field.Name]; !ok {
				continue
			}
		}
		var data interface{}
		switch field.DataType {
		case schemapb.DataType_Bool:
			data = make([]bool, 0, rowsLen)
		case schemapb.DataType_Int8:
			data = make([]int8, 0, rowsLen)
		case schemapb.DataType_Int16:
			data = make([]int16, 0, rowsLen)
		case schemapb.DataType_Int32:
			data = make([]int32, 0, rowsLen)
		case schemapb.DataType_Int64:
			data = make([]int64, 0, rowsLen)
		case schemapb.DataType_Float:
			data = make([]float32, 0, rowsLen)
		case schemapb.DataType_Double:
			data = make([]float64, 0, rowsLen)
		case schemapb.DataType_Timestamptz:
			data = make([]string, 0, rowsLen)
		case schemapb.DataType_String, schemapb.DataType_VarChar, schemapb.DataType_Text:
			data = make([]string, 0, rowsLen)
		case schemapb.DataType_Array:
			data = make([]*schemapb.ScalarField, 0, rowsLen)
		case schemapb.DataType_JSON:
			data = make([][]byte, 0, rowsLen)
		case schemapb.DataType_Geometry:
			data = make([]string, 0, rowsLen)
		case schemapb.DataType_FloatVector:
			data = make([][]float32, 0, rowsLen)
			dim, _ := getDim(field)
			nameDims[field.Name] = dim
		case schemapb.DataType_BinaryVector:
			data = make([][]byte, 0, rowsLen)
			dim, _ := getDim(field)
			nameDims[field.Name] = dim
		case schemapb.DataType_Float16Vector:
			data = make([][]byte, 0, rowsLen)
			dim, _ := getDim(field)
			nameDims[field.Name] = dim
		case schemapb.DataType_BFloat16Vector:
			data = make([][]byte, 0, rowsLen)
			dim, _ := getDim(field)
			nameDims[field.Name] = dim
		case schemapb.DataType_SparseFloatVector:
			data = make([][]byte, 0, rowsLen)
			nameDims[field.Name] = int64(0)
		case schemapb.DataType_Int8Vector:
			data = make([][]int8, 0, rowsLen)
			dim, _ := getDim(field)
			nameDims[field.Name] = dim
		default:
			return nil, merr.WrapErrParameterInvalidMsg("the type(%v) of field(%v) is not supported, use other sdk please", field.DataType, field.Name)
		}
		nameColumns[field.Name] = data
		fieldData[field.Name] = &schemapb.FieldData{
			Type:      field.DataType,
			FieldName: field.Name,
			FieldId:   field.FieldID,
			IsDynamic: field.IsDynamic,
		}
	}
	if len(typeutil.GetVectorFieldSchemas(sch)) == 0 && len(sch.Functions) == 0 && !partialUpdate {
		return nil, merr.WrapErrParameterInvalidMsg("collection: %s has no vector field or functions", sch.Name)
	}

	dynamicCol := make([][]byte, 0, rowsLen)
	fieldLen := make(map[string]int)

	for _, row := range rows {
		// collection schema name need not be same, since receiver could have other names
		v := reflect.ValueOf(row)
		set, err := reflectValueCandi(v)
		if err != nil {
			return nil, err
		}
		for idx, field := range sch.Fields {
			if field.IsDynamic {
				continue
			}
			candi, ok := set[field.Name]
			if field.IsPrimaryKey && field.AutoID && inInsert {
				if !ok {
					continue
				}
				if !allowInsertAutoID {
					return nil, merr.WrapErrParameterInvalidMsg("no need to pass pk field(%s) when autoid==true in insert", field.Name)
				}
			}
			if (field.Nullable || field.DefaultValue != nil) && !ok {
				continue
			}
			if field.GetIsFunctionOutput() {
				if _, allocated := nameColumns[field.Name]; !allocated {
					continue
				}
			}
			if !ok {
				if partialUpdate {
					continue
				}
				return nil, merr.WrapErrParameterInvalidMsg("row %d does not has field %s", idx, field.Name)
			}
			fieldLen[field.Name] += 1
			switch field.DataType {
			case schemapb.DataType_Bool:
				nameColumns[field.Name] = append(nameColumns[field.Name].([]bool), candi.v.Interface().(bool))
			case schemapb.DataType_Int8:
				nameColumns[field.Name] = append(nameColumns[field.Name].([]int8), candi.v.Interface().(int8))
			case schemapb.DataType_Int16:
				nameColumns[field.Name] = append(nameColumns[field.Name].([]int16), candi.v.Interface().(int16))
			case schemapb.DataType_Int32:
				nameColumns[field.Name] = append(nameColumns[field.Name].([]int32), candi.v.Interface().(int32))
			case schemapb.DataType_Int64:
				nameColumns[field.Name] = append(nameColumns[field.Name].([]int64), candi.v.Interface().(int64))
			case schemapb.DataType_Float:
				nameColumns[field.Name] = append(nameColumns[field.Name].([]float32), candi.v.Interface().(float32))
			case schemapb.DataType_Timestamptz:
				nameColumns[field.Name] = append(nameColumns[field.Name].([]string), candi.v.Interface().(string))
			case schemapb.DataType_Double:
				nameColumns[field.Name] = append(nameColumns[field.Name].([]float64), candi.v.Interface().(float64))
			case schemapb.DataType_String, schemapb.DataType_VarChar, schemapb.DataType_Text:
				nameColumns[field.Name] = append(nameColumns[field.Name].([]string), candi.v.Interface().(string))
			case schemapb.DataType_Array:
				nameColumns[field.Name] = append(nameColumns[field.Name].([]*schemapb.ScalarField), candi.v.Interface().(*schemapb.ScalarField))
			case schemapb.DataType_JSON:
				nameColumns[field.Name] = append(nameColumns[field.Name].([][]byte), candi.v.Interface().([]byte))
			case schemapb.DataType_Geometry:
				nameColumns[field.Name] = append(nameColumns[field.Name].([]string), candi.v.Interface().(string))
			case schemapb.DataType_FloatVector:
				nameColumns[field.Name] = append(nameColumns[field.Name].([][]float32), candi.v.Interface().([]float32))
			case schemapb.DataType_BinaryVector:
				nameColumns[field.Name] = append(nameColumns[field.Name].([][]byte), candi.v.Interface().([]byte))
			case schemapb.DataType_Float16Vector:
				switch candi.v.Interface().(type) {
				case []byte:
					nameColumns[field.Name] = append(nameColumns[field.Name].([][]byte), candi.v.Interface().([]byte))
				case []float32:
					vec := typeutil.Float32ArrayToFloat16Bytes(candi.v.Interface().([]float32))
					nameColumns[field.Name] = append(nameColumns[field.Name].([][]byte), vec)
				default:
					return nil, merr.WrapErrParameterInvalidMsg("invalid type(%v) of field(%v) ", field.DataType, field.Name)
				}
			case schemapb.DataType_BFloat16Vector:
				switch candi.v.Interface().(type) {
				case []byte:
					nameColumns[field.Name] = append(nameColumns[field.Name].([][]byte), candi.v.Interface().([]byte))
				case []float32:
					vec := typeutil.Float32ArrayToBFloat16Bytes(candi.v.Interface().([]float32))
					nameColumns[field.Name] = append(nameColumns[field.Name].([][]byte), vec)
				default:
					return nil, merr.WrapErrParameterInvalidMsg("invalid type(%v) of field(%v) ", field.DataType, field.Name)
				}
			case schemapb.DataType_SparseFloatVector:
				content := candi.v.Interface().([]byte)
				rowSparseDim := typeutil.SparseFloatRowDim(content)
				if rowSparseDim > nameDims[field.Name] {
					nameDims[field.Name] = rowSparseDim
				}
				nameColumns[field.Name] = append(nameColumns[field.Name].([][]byte), content)
			case schemapb.DataType_Int8Vector:
				nameColumns[field.Name] = append(nameColumns[field.Name].([][]int8), candi.v.Interface().([]int8))
			default:
				return nil, merr.WrapErrParameterInvalidMsg("the type(%v) of field(%v) is not supported, use other sdk please", field.DataType, field.Name)
			}

			delete(set, field.Name)
		}
		for _, structField := range sch.GetStructArrayFields() {
			delete(set, structField.GetName())
		}
		// if is not dynamic, but pass more field, will throw err in /internal/distributed/proxy/httpserver/utils.go@checkAndSetData
		if isDynamic {
			m := make(map[string]interface{})
			for name, candi := range set {
				m[name] = candi.v.Interface()
			}
			bs, err := json.Marshal(m)
			if err != nil {
				return nil, merr.WrapErrParameterInvalidErr(err, "failed to marshal dynamic field")
			}
			// The values were checked individually, but this wrapper is what is
			// stored: it adds a level of nesting, and re-encoding can turn two
			// distinct keys into one. Check the bytes that actually land.
			// Gated like the per-value checks above: compatibilityMode restores
			// the previous handling, which stored the wrapper unexamined.
			if !paramtable.Get().HTTPCfg.CompatibilityMode.GetAsBool() {
				if err := checkEngineCompatible(common.MetaFieldName, string(bs)); err != nil {
					return nil, err
				}
			}
			dynamicCol = append(dynamicCol, bs)
		}
	}
	columns := make([]*schemapb.FieldData, 0, len(nameColumns))
	for name, column := range nameColumns {
		validData, hasValidData := validDataMap[name]
		if fieldLen[name] == 0 && name == pkFieldName && isAutoIDPK {
			continue
		}
		if fieldLen[name] == 0 && partialUpdate {
			if hasValidData {
				if len(validData) != rowsLen {
					mlog.Info(context.TODO(), "field len is not equal to rows len",
						mlog.String("fieldName", name),
						mlog.Int("fieldLen", len(validData)),
						mlog.Int("rowsLen", rowsLen))
					return nil, merr.WrapErrParameterInvalidMsg("column %s has length %d, expected %d", name, len(validData), rowsLen)
				}
			} else {
				mlog.Info(context.TODO(), "skip empty field for partial update",
					mlog.String("fieldName", name))
				continue
			}
		}
		if fieldLen[name] != rowsLen && partialUpdate && (!hasValidData || len(validData) != rowsLen) {
			// for partial update, if try to update different field in different rows, return error
			mlog.Info(context.TODO(), "field len is not equal to rows len",
				mlog.String("fieldName", name),
				mlog.Int("fieldLen", fieldLen[name]),
				mlog.Int("rowsLen", rowsLen))
			return nil, merr.WrapErrParameterInvalidMsg("column %s has length %d, expected %d", name, fieldLen[name], rowsLen)
		}

		colData := fieldData[name]
		switch colData.Type {
		case schemapb.DataType_Bool:
			colData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_BoolData{
						BoolData: &schemapb.BoolArray{
							Data: column.([]bool),
						},
					},
				},
			}
		case schemapb.DataType_Int8:
			colData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: convertToIntArray(colData.Type, column),
						},
					},
				},
			}
		case schemapb.DataType_Int16:
			colData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: convertToIntArray(colData.Type, column),
						},
					},
				},
			}
		case schemapb.DataType_Int32:
			colData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: column.([]int32),
						},
					},
				},
			}
		case schemapb.DataType_Int64:
			colData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: column.([]int64),
						},
					},
				},
			}
		case schemapb.DataType_Float:
			colData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_FloatData{
						FloatData: &schemapb.FloatArray{
							Data: column.([]float32),
						},
					},
				},
			}
		case schemapb.DataType_Double:
			colData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_DoubleData{
						DoubleData: &schemapb.DoubleArray{
							Data: column.([]float64),
						},
					},
				},
			}
		case schemapb.DataType_Timestamptz:
			colData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: column.([]string),
						},
					},
				},
			}
		case schemapb.DataType_String, schemapb.DataType_VarChar, schemapb.DataType_Text:
			colData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: column.([]string),
						},
					},
				},
			}
		case schemapb.DataType_Array:
			colData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_ArrayData{
						ArrayData: &schemapb.ArrayArray{
							Data: column.([]*schemapb.ScalarField),
						},
					},
				},
			}
		case schemapb.DataType_JSON:
			colData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_JsonData{
						JsonData: &schemapb.JSONArray{
							Data: column.([][]byte),
						},
					},
				},
			}
		case schemapb.DataType_Geometry:
			colData.Field = &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_GeometryWktData{
						GeometryWktData: &schemapb.GeometryWktArray{
							Data: column.([]string),
						},
					},
				},
			}
		case schemapb.DataType_FloatVector:
			dim := nameDims[name]
			arr, err := convertFloatVectorToArray(column.([][]float32), dim)
			if err != nil {
				return nil, err
			}
			colData.Field = &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{
							Data: arr,
						},
					},
				},
			}
		case schemapb.DataType_BinaryVector:
			dim := nameDims[name]
			arr, err := convertBinaryVectorToArray(column.([][]byte), dim, colData.Type)
			if err != nil {
				return nil, err
			}
			colData.Field = &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_BinaryVector{
						BinaryVector: arr,
					},
				},
			}
		case schemapb.DataType_Float16Vector:
			dim := nameDims[name]
			arr, err := convertBinaryVectorToArray(column.([][]byte), dim, colData.Type)
			if err != nil {
				return nil, err
			}
			colData.Field = &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_Float16Vector{
						Float16Vector: arr,
					},
				},
			}
		case schemapb.DataType_BFloat16Vector:
			dim := nameDims[name]
			arr, err := convertBinaryVectorToArray(column.([][]byte), dim, colData.Type)
			if err != nil {
				return nil, err
			}
			colData.Field = &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_Bfloat16Vector{
						Bfloat16Vector: arr,
					},
				},
			}
		case schemapb.DataType_SparseFloatVector:
			colData.Field = &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: nameDims[name],
					Data: &schemapb.VectorField_SparseFloatVector{
						SparseFloatVector: &schemapb.SparseFloatArray{
							Dim:      nameDims[name],
							Contents: column.([][]byte),
						},
					},
				},
			}
		case schemapb.DataType_Int8Vector:
			dim := nameDims[name]
			arr, err := convertInt8VectorToArray(column.([][]int8), dim)
			if err != nil {
				return nil, err
			}
			colData.Field = &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_Int8Vector{
						Int8Vector: arr,
					},
				},
			}
		default:
			return nil, merr.WrapErrParameterInvalidMsg("the type(%v) of field(%v) is not supported, use other sdk please", colData.Type, name)
		}
		typeutil.SetFieldDataValidData(colData, validDataMap[name])
		columns = append(columns, colData)
	}
	if isDynamic {
		columns = append(columns, &schemapb.FieldData{
			Type:      schemapb.DataType_JSON,
			FieldName: "",
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_JsonData{
						JsonData: &schemapb.JSONArray{
							Data: dynamicCol,
						},
					},
				},
			},
			IsDynamic: true,
		})
	}
	for _, structField := range sch.GetStructArrayFields() {
		validData, hasValidData := validDataMap[structField.GetName()]
		if hasValidData && len(validData) != rowsLen {
			return nil, merr.WrapErrParameterInvalidMsg("struct field %s valid data has length %d, expected %d",
				structField.GetName(), len(validData), rowsLen)
		}
		perRow := make([]structArrayRow, 0, rowsLen)
		for rowIdx, row := range rows {
			val, ok := row[structField.GetName()]
			if hasValidData && !validData[rowIdx] {
				if ok {
					return nil, merr.WrapErrParameterInvalidMsg("row %d struct field %s is null but contains data",
						rowIdx, structField.GetName())
				}
				continue
			}
			if !ok {
				if partialUpdate {
					continue
				}
				return nil, merr.WrapErrParameterInvalidMsg("row %d does not has struct field %s", rowIdx, structField.GetName())
			}
			sr, ok := val.(structArrayRow)
			if !ok {
				return nil, merr.WrapErrParameterInvalidMsg("row %d struct field %s has unexpected payload type %T",
					rowIdx, structField.GetName(), val)
			}
			perRow = append(perRow, sr)
		}
		if len(perRow) == 0 && !hasValidData {
			continue
		}
		var structFieldData *schemapb.FieldData
		var err error
		if hasValidData {
			structFieldData, err = buildNullableStructArrayFieldData(structField, perRow, validData)
		} else {
			structFieldData, err = buildStructArrayFieldData(structField, perRow)
		}
		if err != nil {
			return nil, err
		}
		columns = append(columns, structFieldData)
	}
	return columns, nil
}

// rejectNullCoordinates refuses a null inside any vector of a search request.
//
// A null coordinate decodes to 0 and then passes the dimension check, so the
// search ran against a point the caller never asked for. This is the query side
// of the same rule insert applies to a vector field, and like it is not gated by
// compatibilityMode: a vector is a dense fixed-width array of numbers with no
// per-element validity to record "absent" in, so there are no null coordinates
// to stay compatible with.
func rejectNullCoordinates(vectorStr string, dataType schemapb.DataType) error {
	// same screen as nullElementIn, one level up: skip the row walk entirely
	// for the overwhelming majority of requests that carry no "null" at all
	if !strings.Contains(vectorStr, "null") {
		return nil
	}
	var nullErr error
	gjson.Parse(vectorStr).ForEach(func(row, vector gjson.Result) bool {
		if idx, found := nullElementIn(vector); found {
			nullErr = merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(dataType)], vector.Raw,
				fmt.Sprintf("null at index %d of vector %s; a vector element cannot be null", idx, row.Raw))
			return false
		}
		return true
	})
	return nullErr
}

func serializeFloatVectors(vectorStr string, dataType schemapb.DataType, dimension, bytesLen int64, fpArrayToBytesFunc func([]float32) []byte) ([][]byte, error) {
	if err := rejectNullCoordinates(vectorStr, dataType); err != nil {
		return nil, err
	}

	var fp32Values [][]float32
	err := json.Unmarshal([]byte(vectorStr), &fp32Values)
	if err != nil {
		return nil, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(dataType)], vectorStr, err.Error())
	}
	values := make([][]byte, 0, len(fp32Values))
	for _, vectorArray := range fp32Values {
		if int64(len(vectorArray)) != dimension {
			return nil, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(dataType)], vectorStr,
				fmt.Sprintf("dimension: %d, but length of []float: %d", dimension, len(vectorArray)))
		}
		vectorBytes := fpArrayToBytesFunc(vectorArray)
		values = append(values, vectorBytes)
	}
	return values, nil
}

func serializeByteVectors(vectorStr string, dataType schemapb.DataType, dimension, bytesLen int64) ([][]byte, error) {
	values := make([][]byte, 0)
	err := json.Unmarshal([]byte(vectorStr), &values)
	if err != nil {
		return nil, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(dataType)], vectorStr, err.Error())
	}
	for _, vectorArray := range values {
		if int64(len(vectorArray)) != bytesLen {
			return nil, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(dataType)], string(vectorArray),
				fmt.Sprintf("dimension: %d, bytesLen: %d, but length of []byte: %d", dimension, bytesLen, len(vectorArray)))
		}
	}
	return values, nil
}

// serializeFloatOrByteVectors serializes float32/float16/bfloat16 vectors.
// `[[1, 2, 3], [4.0, 5.0, 6.0]] is float32 vector,
// `["4z1jPgAAgL8=", "gD+AP4A/gD8="]` is float16/bfloat16 vector.
func serializeFloatOrByteVectors(jsonResult gjson.Result, dataType schemapb.DataType, dimension int64, fpArrayToBytesFunc func([]float32) []byte) ([][]byte, error) {
	firstElement := jsonResult.Get("0")

	// Clients may send float32 vector because they are inconvenient of processing float16 or bfloat16.
	// Float32 vector is an array in JSON format, like `[1.0, 2.0, 3.0]`, `[1, 2, 3]`, etc,
	// while float16 or bfloat16 vector is a string in JSON format, like `"4z1jPgAAgL8="`, `"gD+AP4A/gD8="`, etc.
	if firstElement.IsArray() {
		return serializeFloatVectors(jsonResult.Raw, dataType, dimension, dimension*2, fpArrayToBytesFunc)
	} else if firstElement.Type == gjson.String || !firstElement.Exists() {
		// consider corner case: `[]`
		return serializeByteVectors(jsonResult.Raw, dataType, dimension, dimension*2)
	}
	return nil, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(dataType)], jsonResult.Raw, "invalid type")
}

func serializeSparseFloatVectors(vectors []gjson.Result, dataType schemapb.DataType) ([][]byte, error) {
	compatibilityMode := paramtable.Get().HTTPCfg.CompatibilityMode.GetAsBool()
	values := make([][]byte, 0, len(vectors))
	for _, vector := range vectors {
		vectorBytes := []byte(vector.String())
		// "null" is accepted as an empty sparse row, so the search ran against
		// an empty vector instead of reporting the mistake. A real JSON null
		// renders through String() as the empty string, so it is asked of the
		// node type; the literal comparison catches the quoted spelling.
		if !compatibilityMode &&
			(vector.Type == gjson.Null || strings.TrimSpace(string(vectorBytes)) == "null") {
			return nil, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(dataType)], vector.Raw,
				"a sparse vector cannot be null")
		}
		// The float and int8 query paths read the raw element and so refuse the
		// quoted form already; this one reads String() and did not.
		if !compatibilityMode {
			if err := checkVectorSpelling(HTTPRequestData, dataType, vector); err != nil {
				return nil, err
			}
		}
		sparseVector, err := typeutil.CreateSparseFloatRowFromJSON(vectorBytes)
		if err != nil {
			return nil, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(dataType)], vector.String(), err.Error())
		}
		values = append(values, sparseVector)
	}
	return values, nil
}

func serializeInt8Vectors(vectorStr string, dataType schemapb.DataType, dimension int64, int8ArrayToBytesFunc func([]int8) []byte) ([][]byte, error) {
	if err := rejectNullCoordinates(vectorStr, dataType); err != nil {
		return nil, err
	}

	var int8Values [][]int8
	err := json.Unmarshal([]byte(vectorStr), &int8Values)
	if err != nil {
		return nil, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(dataType)], vectorStr, err.Error())
	}
	values := make([][]byte, 0, len(int8Values))
	for _, vectorArray := range int8Values {
		if int64(len(vectorArray)) != dimension {
			return nil, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(dataType)], vectorStr,
				fmt.Sprintf("dimension: %d, but length of []int8: %d", dimension, len(vectorArray)))
		}
		vectorBytes := int8ArrayToBytesFunc(vectorArray)
		values = append(values, vectorBytes)
	}
	return values, nil
}

func convertQueries2Placeholder(body string, dataType schemapb.DataType, dimension int64) (*commonpb.PlaceholderValue, error) {
	var valueType commonpb.PlaceholderType
	var values [][]byte
	var err error
	switch dataType {
	case schemapb.DataType_FloatVector:
		valueType = commonpb.PlaceholderType_FloatVector
		values, err = serializeFloatVectors(gjson.Get(body, HTTPRequestData).Raw, dataType, dimension, dimension*4, typeutil.Float32ArrayToBytes)
	case schemapb.DataType_BinaryVector:
		valueType = commonpb.PlaceholderType_BinaryVector
		values, err = serializeByteVectors(gjson.Get(body, HTTPRequestData).Raw, dataType, dimension, dimension/8)
	case schemapb.DataType_Float16Vector:
		valueType = commonpb.PlaceholderType_Float16Vector
		values, err = serializeFloatOrByteVectors(gjson.Get(body, HTTPRequestData), dataType, dimension, typeutil.Float32ArrayToFloat16Bytes)
	case schemapb.DataType_BFloat16Vector:
		valueType = commonpb.PlaceholderType_BFloat16Vector
		values, err = serializeFloatOrByteVectors(gjson.Get(body, HTTPRequestData), dataType, dimension, typeutil.Float32ArrayToBFloat16Bytes)
	case schemapb.DataType_SparseFloatVector:
		valueType = commonpb.PlaceholderType_SparseFloatVector
		values, err = serializeSparseFloatVectors(gjson.Get(body, HTTPRequestData).Array(), dataType)
	case schemapb.DataType_Int8Vector:
		valueType = commonpb.PlaceholderType_Int8Vector
		values, err = serializeInt8Vectors(gjson.Get(body, HTTPRequestData).Raw, dataType, dimension, typeutil.Int8ArrayToBytes)
	case schemapb.DataType_VarChar:
		valueType = commonpb.PlaceholderType_VarChar
		res := gjson.Get(body, HTTPRequestData).Array()
		values = make([][]byte, 0, len(res))
		compatibilityMode := paramtable.Get().HTTPCfg.CompatibilityMode.GetAsBool()
		for _, v := range res {
			// String() renders whatever it is given rather than returning a
			// string the caller sent, so 1.50 searched for "1.5", null searched
			// for "", and an object searched for its own JSON text.
			//
			// Stricter than insert on purpose: stringFieldValue keeps the
			// literal of a number written into a VarChar field, because there
			// the caller is naming the text to store. A query is naming text to
			// find, and a number there is a mistake worth reporting rather than
			// a search for its own digits.
			if !compatibilityMode && v.Type != gjson.String {
				return nil, merr.WrapErrParameterInvalid(schemapb.DataType_name[int32(dataType)], v.Raw,
					"a text query must be a string")
			}
			values = append(values, []byte(v.String()))
		}
	}
	if err != nil {
		return nil, err
	}
	return &commonpb.PlaceholderValue{
		Tag:    "$0",
		Type:   valueType,
		Values: values,
	}, nil
}

// todo: support [][]byte for BinaryVector
func vectors2PlaceholderGroupBytes(vectors [][]float32) []byte {
	var placeHolderType commonpb.PlaceholderType
	ph := &commonpb.PlaceholderValue{
		Tag:    "$0",
		Values: make([][]byte, 0, len(vectors)),
	}
	if len(vectors) != 0 {
		placeHolderType = commonpb.PlaceholderType_FloatVector

		ph.Type = placeHolderType
		for _, vector := range vectors {
			ph.Values = append(ph.Values, typeutil.Float32ArrayToBytes(vector))
		}
	}
	phg := &commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{
			ph,
		},
	}

	bs, _ := proto.Marshal(phg)
	return bs
}

// --------------------- get/query/search response --------------------- //
func genDynamicFields(fields []string, list []*schemapb.FieldData) []string {
	nonDynamicFieldNames := make(map[string]struct{})
	for _, field := range list {
		if !field.IsDynamic {
			nonDynamicFieldNames[field.FieldName] = struct{}{}
		}
	}
	dynamicFields := []string{}
	for _, fieldName := range fields {
		if _, exist := nonDynamicFieldNames[fieldName]; !exist {
			dynamicFields = append(dynamicFields, fieldName)
		}
	}
	return dynamicFields
}

func fieldDataRowCount(fieldData *schemapb.FieldData) (int64, error) {
	if validData := typeutil.GetFieldDataValidData(fieldData); len(validData) > 0 {
		return int64(len(validData)), nil
	}
	return fieldDataValueCount(fieldData)
}

func fieldDataValueCount(fieldData *schemapb.FieldData) (int64, error) {
	switch fieldData.GetType() {
	case schemapb.DataType_Bool:
		return int64(len(fieldData.GetScalars().GetBoolData().GetData())), nil
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		return int64(len(fieldData.GetScalars().GetIntData().GetData())), nil
	case schemapb.DataType_Int64:
		return int64(len(fieldData.GetScalars().GetLongData().GetData())), nil
	case schemapb.DataType_Float:
		return int64(len(fieldData.GetScalars().GetFloatData().GetData())), nil
	case schemapb.DataType_Double:
		return int64(len(fieldData.GetScalars().GetDoubleData().GetData())), nil
	case schemapb.DataType_Timestamptz:
		if fieldData.GetScalars().GetTimestamptzData() != nil {
			return int64(len(fieldData.GetScalars().GetTimestamptzData().GetData())), nil
		}
		return int64(len(fieldData.GetScalars().GetStringData().GetData())), nil
	case schemapb.DataType_String, schemapb.DataType_VarChar, schemapb.DataType_Text:
		return int64(len(fieldData.GetScalars().GetStringData().GetData())), nil
	case schemapb.DataType_Array:
		return int64(len(fieldData.GetScalars().GetArrayData().GetData())), nil
	case schemapb.DataType_ArrayOfVector:
		return int64(len(fieldData.GetVectors().GetVectorArray().GetData())), nil
	case schemapb.DataType_JSON:
		return int64(len(fieldData.GetScalars().GetJsonData().GetData())), nil
	case schemapb.DataType_Geometry:
		if fieldData.GetScalars().GetGeometryData() != nil {
			return int64(len(fieldData.GetScalars().GetGeometryData().GetData())), nil
		}
		return int64(len(fieldData.GetScalars().GetGeometryWktData().GetData())), nil
	case schemapb.DataType_BinaryVector:
		dim := fieldData.GetVectors().GetDim()
		bytesPerRow := dim / 8
		if bytesPerRow <= 0 {
			return 0, merr.WrapErrParameterInvalidMsg("invalid binary vector dimension %d for field %s", dim, fieldData.GetFieldName())
		}
		return int64(len(fieldData.GetVectors().GetBinaryVector())) / bytesPerRow, nil
	case schemapb.DataType_FloatVector:
		dim := fieldData.GetVectors().GetDim()
		if dim <= 0 {
			return 0, merr.WrapErrParameterInvalidMsg("invalid float vector dimension %d for field %s", dim, fieldData.GetFieldName())
		}
		return int64(len(fieldData.GetVectors().GetFloatVector().GetData())) / dim, nil
	case schemapb.DataType_Float16Vector:
		dim := fieldData.GetVectors().GetDim()
		bytesPerRow := dim * 2
		if bytesPerRow <= 0 {
			return 0, merr.WrapErrParameterInvalidMsg("invalid float16 vector dimension %d for field %s", dim, fieldData.GetFieldName())
		}
		return int64(len(fieldData.GetVectors().GetFloat16Vector())) / bytesPerRow, nil
	case schemapb.DataType_BFloat16Vector:
		dim := fieldData.GetVectors().GetDim()
		bytesPerRow := dim * 2
		if bytesPerRow <= 0 {
			return 0, merr.WrapErrParameterInvalidMsg("invalid bfloat16 vector dimension %d for field %s", dim, fieldData.GetFieldName())
		}
		return int64(len(fieldData.GetVectors().GetBfloat16Vector())) / bytesPerRow, nil
	case schemapb.DataType_SparseFloatVector:
		return int64(len(fieldData.GetVectors().GetSparseFloatVector().GetContents())), nil
	case schemapb.DataType_Int8Vector:
		dim := fieldData.GetVectors().GetDim()
		if dim <= 0 {
			return 0, merr.WrapErrParameterInvalidMsg("invalid int8 vector dimension %d for field %s", dim, fieldData.GetFieldName())
		}
		return int64(len(fieldData.GetVectors().GetInt8Vector())) / dim, nil
	case schemapb.DataType_ArrayOfStruct:
		subs := fieldData.GetStructArrays().GetFields()
		if len(subs) == 0 {
			return 0, nil
		}
		if validData := typeutil.GetFieldDataValidData(subs[0]); len(validData) > 0 {
			return int64(len(validData)), nil
		}
		switch subs[0].GetType() {
		case schemapb.DataType_Array:
			return int64(len(subs[0].GetScalars().GetArrayData().GetData())), nil
		case schemapb.DataType_ArrayOfVector:
			return int64(len(subs[0].GetVectors().GetVectorArray().GetData())), nil
		default:
			return 0, merr.WrapErrParameterInvalidMsg("unsupported struct sub-field type %s for field %s", subs[0].GetType(), fieldData.GetFieldName())
		}
	default:
		return 0, asSafeMessage(merr.WrapErrParameterInvalidMsg("the type(%v) of field(%v) is not supported, use other sdk please", fieldData.GetType(), fieldData.GetFieldName()))
	}
}

type fieldDataRowAccessor struct {
	fieldData      *schemapb.FieldData
	validData      []bool
	compactIndices []int64
}

func newFieldDataRowAccessor(fieldData *schemapb.FieldData) (*fieldDataRowAccessor, error) {
	accessor := &fieldDataRowAccessor{
		fieldData: fieldData,
		validData: typeutil.GetFieldDataValidData(fieldData),
	}
	if len(accessor.validData) == 0 {
		return accessor, nil
	}
	isNullableVector := typeutil.IsCompactNullableVectorFieldData(fieldData)

	compactIndices := make([]int64, len(accessor.validData))
	validCount := int64(0)
	for i, valid := range accessor.validData {
		if valid {
			compactIndices[i] = validCount
			validCount++
		} else {
			compactIndices[i] = -1
		}
	}
	if isNullableVector {
		if err := funcutil.ValidateNullableVectorFieldDataCompact(fieldData, uint64(len(accessor.validData)), true); err != nil {
			return nil, err
		}
		accessor.compactIndices = compactIndices
		return accessor, nil
	}
	if validCount == 0 {
		accessor.compactIndices = compactIndices
		return accessor, nil
	}

	valueCount, err := fieldDataValueCount(fieldData)
	if err != nil {
		return nil, err
	}
	if valueCount == int64(len(accessor.validData)) {
		return accessor, nil
	}
	if valueCount != validCount {
		return nil, merr.WrapErrParameterInvalidMsg("field %s has %d valid rows, but data length is %d", fieldData.GetFieldName(), validCount, valueCount)
	}
	accessor.compactIndices = compactIndices
	return accessor, nil
}

func (accessor *fieldDataRowAccessor) rowIndex(rowIdx int64) (int64, bool, error) {
	if len(accessor.validData) == 0 {
		return rowIdx, true, nil
	}
	if rowIdx >= int64(len(accessor.validData)) {
		return 0, false, merr.WrapErrParameterInvalidMsg("row index %d out of range for field %s valid data length %d", rowIdx, accessor.fieldData.GetFieldName(), len(accessor.validData))
	}
	if !accessor.validData[rowIdx] {
		return 0, false, nil
	}
	if accessor.compactIndices != nil {
		return accessor.compactIndices[rowIdx], true, nil
	}
	return rowIdx, true, nil
}

// safeMessageError marks an error whose message was written for the caller and is
// therefore safe to echo in an HTTP response. Errors are not safe by default: most
// of the ParameterInvalid errors on the response path report a server-side shape
// mismatch (row counts, valid-data bitmap lengths, element counts) whose wording
// leaks internal layout, so classification alone cannot decide what to echo.
type safeMessageError struct{ error }

func (e *safeMessageError) Unwrap() error { return e.error }

// asSafeMessage marks err as caller-facing. Apply it only where the message tells
// the caller what to do about their own request.
func asSafeMessage(err error) error {
	if err == nil {
		return nil
	}
	return &safeMessageError{err}
}

// outputFieldError attributes a response-serialization failure to the output field
// that caused it. The name comes from the caller's own outputFields, so echoing it
// back tells them which column to drop or report without exposing anything internal.
// The wrapped cause is not safe to echo and is only unwrapped for classification.
type outputFieldError struct {
	field string
	inner error
}

func (e *outputFieldError) Error() string {
	return fmt.Sprintf("failed to serialize output field %s: %s", e.field, e.inner.Error())
}

func (e *outputFieldError) Unwrap() error { return e.inner }

func wrapOutputFieldErr(fieldName string, err error) error {
	if err == nil {
		return nil
	}
	return &outputFieldError{field: fieldName, inner: err}
}

// resultStageError names the part of the response build that failed, for faults
// that cannot be attributed to an output field at all. Its detail describes a
// server-side contract (reduce shapes, bucket counts) and stays in the log; the
// stage name is all the caller gets, but it is enough to say which part of the
// response broke when reporting the failure.
type resultStageError struct {
	stage string
	inner error
}

func (e *resultStageError) Error() string {
	return fmt.Sprintf("failed to build %s: %s", e.stage, e.inner.Error())
}

func (e *resultStageError) Unwrap() error { return e.inner }

func wrapResultStageErr(stage string, err error) error {
	if err == nil {
		return nil
	}
	return &resultStageError{stage: stage, inner: err}
}

// resultErrMessage builds the client-facing message for a response-serialization
// failure. Input errors carry guidance the caller can act on (e.g. an output field
// type the REST layer cannot render — "use other sdk please") and are passed through
// whole. A server-side fault only yields the offending output field: the rest of its
// detail names internal structures and row offsets that mean nothing to the caller,
// and stays in the log line the caller of this function already emits.
func resultErrMessage(err error) string {
	base := merr.ErrInvalidSearchResult.Error()
	// Explicitly marked messages are echoed whole; the mark, not the error type,
	// is what makes a message safe.
	var safeErr *safeMessageError
	if errors.As(err, &safeErr) {
		return base + ", error: " + safeErr.Error()
	}
	var fieldErr *outputFieldError
	if errors.As(err, &fieldErr) {
		return base + ", error: failed to serialize output field " + fieldErr.field
	}
	var stageErr *resultStageError
	if errors.As(err, &stageErr) {
		return base + ", error: failed to build " + stageErr.stage
	}
	return base
}

// legacyArrayValue renders one array row in the protobuf wrapper shape that
// proxy.http.legacyArrayResponse restores. The shape is reproduced by handing
// the ScalarField itself to the encoder, which is what the clients this switch
// exists for parse. An Int64 the caller cannot hold natively is the exception:
// its slice has no room for the string form, so the wrapper around it is spelled
// out instead. Everything else, and every row once the caller allows Int64, is
// the message as it stands, byte for byte.
func legacyArrayValue(row *schemapb.ScalarField, enableInt64 bool) any {
	if enableInt64 || !holdsInt64(row) {
		return row
	}

	switch data := row.GetData().(type) {
	case *schemapb.ScalarField_LongData:
		inner := map[string]any{}
		if values := data.LongData.GetData(); len(values) > 0 {
			inner["data"] = formatInt64(values)
		}
		return map[string]any{"Data": map[string]any{"LongData": inner}}
	case *schemapb.ScalarField_ArrayData:
		inner := map[string]any{}
		if elements := data.ArrayData.GetData(); len(elements) > 0 {
			rendered := make([]any, 0, len(elements))
			for _, element := range elements {
				rendered = append(rendered, legacyArrayValue(element, enableInt64))
			}
			inner["data"] = rendered
		}
		if elementType := data.ArrayData.GetElementType(); elementType != schemapb.DataType_None {
			inner["element_type"] = elementType
		}
		return map[string]any{"Data": map[string]any{"ArrayData": inner}}
	default:
		return row
	}
}

// holdsInt64 reports whether a row renders any Int64, directly or nested.
func holdsInt64(row *schemapb.ScalarField) bool {
	switch data := row.GetData().(type) {
	case *schemapb.ScalarField_LongData:
		return true
	case *schemapb.ScalarField_ArrayData:
		for _, element := range data.ArrayData.GetData() {
			if holdsInt64(element) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func nonNilSlice[T any](values []T) []T {
	if values == nil {
		return []T{}
	}
	return values
}

func scalarFieldToRESTAny(field *schemapb.ScalarField, enableInt64 bool) (any, error) {
	// A missing row renders as null and an unset oneof as [], matching what the
	// previous implementation produced. Turning either into a request-level
	// failure would widen this change beyond the serialization format it fixes.
	if field == nil {
		return nil, nil
	}

	switch data := field.GetData().(type) {
	case nil:
		// segcore emits a default-constructed ScalarField for an empty array whose
		// element type it could not determine (Array.h: `default: { // empty array }`).
		return []any{}, nil
	case *schemapb.ScalarField_BoolData:
		return nonNilSlice(data.BoolData.GetData()), nil
	case *schemapb.ScalarField_IntData:
		return nonNilSlice(data.IntData.GetData()), nil
	case *schemapb.ScalarField_LongData:
		values := nonNilSlice(data.LongData.GetData())
		if enableInt64 {
			return values, nil
		}
		return formatInt64(values), nil
	case *schemapb.ScalarField_FloatData:
		return nonNilSlice(data.FloatData.GetData()), nil
	case *schemapb.ScalarField_DoubleData:
		return nonNilSlice(data.DoubleData.GetData()), nil
	case *schemapb.ScalarField_StringData:
		return nonNilSlice(data.StringData.GetData()), nil
	case *schemapb.ScalarField_ArrayData:
		elements := data.ArrayData.GetData()
		values := make([]any, 0, len(elements))
		for _, element := range elements {
			value, err := scalarFieldToRESTAny(element, enableInt64)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		return values, nil
	default:
		return nil, merr.WrapErrServiceInternalMsg("unsupported array row scalar field type %T", data)
	}
}

//nolint:gosec // G602: slice indices are bounded by rowsNum which is derived from the data length
func buildQueryResp(rowsNum int64, needFields []string, fieldDataList []*schemapb.FieldData, ids *schemapb.IDs,
	scores []float32, enableInt64 bool, collectionSchema *schemapb.CollectionSchema,
) ([]map[string]interface{}, error) {
	nativeJSON := paramtable.Get().HTTPCfg.NativeJSONResponse.GetAsBool()
	jsonFieldNames := make(map[string]struct{})
	jsonAllValid := true

	columnNum := len(fieldDataList)
	if rowsNum == int64(0) { // always
		if columnNum > 0 {
			var err error
			rowsNum, err = fieldDataRowCount(fieldDataList[0])
			if err != nil {
				return nil, wrapOutputFieldErr(fieldDataList[0].GetFieldName(), err)
			}
		} else if ids != nil {
			switch ids.GetIdField().(type) {
			case *schemapb.IDs_IntId:
				int64Pks := ids.GetIntId().GetData()
				rowsNum = int64(len(int64Pks))
			case *schemapb.IDs_StrId:
				stringPks := ids.GetStrId().GetData()
				rowsNum = int64(len(stringPks))
			default:
				return nil, asSafeMessage(merr.WrapErrParameterInvalidMsg("the type of primary key(id) is not supported, use other sdk please"))
			}
		}
	}
	if rowsNum == int64(0) {
		return []map[string]interface{}{}, nil
	}
	fieldDataAccessors := make([]*fieldDataRowAccessor, 0, columnNum)
	structArrayAccessors := make([]*structArrayRowAccessor, columnNum)
	for idx, fieldData := range fieldDataList {
		accessor, err := newFieldDataRowAccessor(fieldData)
		if err != nil {
			return nil, wrapOutputFieldErr(fieldData.GetFieldName(), err)
		}
		fieldDataAccessors = append(fieldDataAccessors, accessor)
		if fieldData.GetType() == schemapb.DataType_ArrayOfStruct {
			structAccessor, err := newStructArrayRowAccessor(fieldData, collectionSchema)
			if err != nil {
				return nil, wrapOutputFieldErr(fieldData.GetFieldName(), err)
			}
			structArrayAccessors[idx] = structAccessor
		}
	}
	queryResp := make([]map[string]interface{}, 0, rowsNum)
	dynamicOutputFields := genDynamicFields(needFields, fieldDataList)
	legacyArrayResponse := paramtable.Get().HTTPCfg.LegacyArrayResponse.GetAsBool()

	pkFieldName := DefaultPrimaryFieldName
	if collectionSchema != nil {
		fieldsSchema := collectionSchema.GetFields()
		for _, field := range fieldsSchema {
			if field.GetIsPrimaryKey() {
				pkFieldName = field.GetName()
				break
			}
		}
	}
	for i := int64(0); i < rowsNum; i++ {
		row := map[string]interface{}{}
		if columnNum > 0 {
			for j := 0; j < columnNum; j++ {
				fieldData := fieldDataList[j]
				dataIdx, valid, err := fieldDataAccessors[j].rowIndex(i)
				if err != nil {
					return nil, wrapOutputFieldErr(fieldData.GetFieldName(), err)
				}
				if !valid {
					if !fieldData.GetIsDynamic() {
						row[fieldData.GetFieldName()] = nil
					}
					continue
				}
				switch fieldDataList[j].GetType() {
				case schemapb.DataType_Bool:
					row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetScalars().GetBoolData().GetData()[dataIdx]
				case schemapb.DataType_Int8:
					row[fieldDataList[j].GetFieldName()] = int8(fieldDataList[j].GetScalars().GetIntData().GetData()[dataIdx])
				case schemapb.DataType_Int16:
					row[fieldDataList[j].GetFieldName()] = int16(fieldDataList[j].GetScalars().GetIntData().GetData()[dataIdx])
				case schemapb.DataType_Int32:
					row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetScalars().GetIntData().GetData()[dataIdx]
				case schemapb.DataType_Int64:
					if enableInt64 {
						row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetScalars().GetLongData().GetData()[dataIdx]
					} else {
						row[fieldDataList[j].GetFieldName()] = strconv.FormatInt(fieldDataList[j].GetScalars().GetLongData().GetData()[dataIdx], 10)
					}
				case schemapb.DataType_Float:
					row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetScalars().GetFloatData().GetData()[dataIdx]
				case schemapb.DataType_Double:
					row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetScalars().GetDoubleData().GetData()[dataIdx]
				case schemapb.DataType_Timestamptz:
					if fieldDataList[j].GetScalars().GetTimestamptzData() != nil {
						row[fieldDataList[j].FieldName] = fieldDataList[j].GetScalars().GetTimestamptzData().GetData()[dataIdx]
					} else {
						row[fieldDataList[j].FieldName] = fieldDataList[j].GetScalars().GetStringData().GetData()[dataIdx]
					}
				case schemapb.DataType_String, schemapb.DataType_VarChar, schemapb.DataType_Text:
					row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetScalars().GetStringData().GetData()[dataIdx]
				case schemapb.DataType_BinaryVector:
					row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetVectors().GetBinaryVector()[dataIdx*(fieldDataList[j].GetVectors().GetDim()/8) : (dataIdx+1)*(fieldDataList[j].GetVectors().GetDim()/8)]
				case schemapb.DataType_FloatVector:
					row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetVectors().GetFloatVector().GetData()[dataIdx*fieldDataList[j].GetVectors().GetDim() : (dataIdx+1)*fieldDataList[j].GetVectors().GetDim()]
				case schemapb.DataType_Float16Vector:
					row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetVectors().GetFloat16Vector()[dataIdx*(fieldDataList[j].GetVectors().GetDim()*2) : (dataIdx+1)*(fieldDataList[j].GetVectors().GetDim()*2)]
				case schemapb.DataType_BFloat16Vector:
					row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetVectors().GetBfloat16Vector()[dataIdx*(fieldDataList[j].GetVectors().GetDim()*2) : (dataIdx+1)*(fieldDataList[j].GetVectors().GetDim()*2)]
				case schemapb.DataType_SparseFloatVector:
					row[fieldDataList[j].GetFieldName()] = typeutil.SparseFloatBytesToMap(fieldDataList[j].GetVectors().GetSparseFloatVector().Contents[dataIdx])
				case schemapb.DataType_Int8Vector:
					row[fieldDataList[j].GetFieldName()] = fieldDataList[j].GetVectors().GetInt8Vector()[dataIdx*fieldDataList[j].GetVectors().GetDim() : (dataIdx+1)*fieldDataList[j].GetVectors().GetDim()]
				case schemapb.DataType_Array:
					arrayRow := fieldDataList[j].GetScalars().GetArrayData().GetData()[dataIdx]
					if legacyArrayResponse {
						// Escape hatch: emit the raw ScalarField so it serializes back into
						// the protobuf wrapper shape clients may have parsed before the fix.
						// The shape is what those clients read; Accept-Type-Allow-Int64 is
						// about what their JSON parser can hold, which the shape does not
						// change, so an Int64 is still rendered the way the header asks.
						row[fieldDataList[j].GetFieldName()] = legacyArrayValue(arrayRow, enableInt64)
						continue
					}
					value, err := scalarFieldToRESTAny(arrayRow, enableInt64)
					if err != nil {
						return nil, wrapOutputFieldErr(fieldData.GetFieldName(), merr.Wrapf(err, "row %d", i))
					}
					row[fieldDataList[j].GetFieldName()] = value
				case schemapb.DataType_JSON:
					data, ok := fieldDataList[j].GetScalars().GetData().(*schemapb.ScalarField_JsonData)
					if ok && !fieldDataList[j].GetIsDynamic() {
						// A JSON field reads back as a string by default, which
						// is why the same value in a dynamic field reads back as a
						// document. proxy.http.nativeJSONResponse returns the
						// document instead; jsonFieldsToStrings below undoes it for
						// the whole response if any row turns out not to hold one.
						raw := data.JsonData.GetData()[dataIdx]
						if nativeJSON {
							row[fieldDataList[j].GetFieldName()] = json.RawMessage(raw)
							jsonFieldNames[fieldDataList[j].GetFieldName()] = struct{}{}
							// json.Valid accepts invalid UTF-8, which the encoder
							// would replace with U+FFFD without saying so
							if !json.Valid(raw) || !utf8.Valid(raw) {
								jsonAllValid = false
							}
						} else {
							row[fieldDataList[j].GetFieldName()] = string(raw)
						}
					} else {
						var dataMap map[string]interface{}

						// Decode with UseNumber so numeric dynamic-field values are kept as
						// json.Number instead of float64. float64 only has a 53-bit mantissa,
						// so integers larger than 2^53 (e.g. 9223372036854775807) silently lose
						// precision when round-tripped through the REST response. json.Number
						// preserves the exact digits and serializes back as the same integer.
						raw := fieldDataList[j].GetScalars().GetJsonData().Data[dataIdx]

						// A Decoder reads one value and ignores whatever follows
						// it, where the Unmarshal this replaced rejected trailing
						// content. A second Decode on the same decoder restores
						// that in the same pass: only whitespace to the end
						// answers io.EOF, anything else answers a value or an
						// error. An earlier version ran json.Valid first, which
						// read every row twice; the ways to avoid the second
						// call are all closed -- Token is not in sonic's
						// Decoder, which this build uses everywhere, More
						// answers false at a closing bracket because it exists
						// to iterate inside one, and Buffered only exposes the
						// window the decoder happened to pre-read, so a second
						// document past four kilobytes of padding sat outside
						// it and was silently dropped.
						decoder := json.NewDecoder(bytes.NewReader(raw))
						decoder.UseNumber()
						err := decoder.Decode(&dataMap)
						if err == nil {
							var trailing interface{}
							if trailingErr := decoder.Decode(&trailing); trailingErr != io.EOF {
								err = merr.WrapErrParameterInvalidMsg(
									"dynamic field does not hold a single JSON document")
							}
						}
						if err != nil {
							mlog.Error(context.TODO(),
								fmt.Sprintf("[BuildQueryResp] Unmarshal error %s", err.Error()))
							// This branch is only entered for the dynamic field, so the
							// name here is always the internal $meta -- never one of the
							// caller's outputFields -- which makes wrapOutputFieldErr's
							// contract not hold and would report a column the caller
							// cannot drop. What it can act on is the cause: the stored
							// bytes are not one JSON document. That describes its own
							// data, not our layout, so echo it and keep the decoder's
							// text (which can quote the bytes and an offset into them)
							// in the log line above.
							return nil, asSafeMessage(merr.WrapErrParameterInvalidMsg(
								"dynamic field does not hold a single JSON document"))
						}

						if containsString(dynamicOutputFields, fieldDataList[j].GetFieldName()) {
							for key, value := range dataMap {
								row[key] = value
							}
						} else {
							for _, dynamicField := range dynamicOutputFields {
								if _, ok := dataMap[dynamicField]; ok {
									row[dynamicField] = dataMap[dynamicField]
								}
							}
						}
					}
				case schemapb.DataType_Geometry:
					if fieldDataList[j].GetScalars().GetGeometryData() != nil {
						row[fieldDataList[j].FieldName] = fieldDataList[j].GetScalars().GetGeometryData().GetData()[dataIdx]
					} else {
						row[fieldDataList[j].FieldName] = fieldDataList[j].GetScalars().GetGeometryWktData().Data[dataIdx]
					}
				case schemapb.DataType_ArrayOfStruct:
					structRow, err := structArrayAccessors[j].row(int(dataIdx), enableInt64)
					if err != nil {
						return nil, wrapOutputFieldErr(fieldData.GetFieldName(), err)
					}
					row[fieldDataList[j].GetFieldName()] = structRow
				default:
					row[fieldDataList[j].GetFieldName()] = ""
				}
			}
		}
		if ids != nil {
			switch ids.GetIdField().(type) {
			case *schemapb.IDs_IntId:
				int64Pks := ids.GetIntId().GetData()
				if enableInt64 {
					row[pkFieldName] = int64Pks[i]
				} else {
					row[pkFieldName] = strconv.FormatInt(int64Pks[i], 10)
				}
			case *schemapb.IDs_StrId:
				stringPks := ids.GetStrId().GetData()
				row[pkFieldName] = stringPks[i]
			default:
				return nil, asSafeMessage(merr.WrapErrParameterInvalidMsg("the type of primary key(id) is not supported, use other sdk please"))
			}
		}
		if scores != nil && int64(len(scores)) > i {
			row[HTTPReturnDistance] = scores[i] // only 8 decimal places
		}
		queryResp = append(queryResp, row)
	}

	if nativeJSON && !jsonAllValid {
		// Rows written before the insert path was fixed can hold bytes that are
		// not a JSON document. Embedding one of those natively makes the whole
		// response fail to marshal, so a single legacy row would break every
		// query that selects it. Degrade the whole response instead of part of
		// it: a caller can handle "always a document" or "always a string", but
		// not one field that is sometimes each.
		mlog.Warn(context.TODO(),
			"a JSON field holds bytes that are not a JSON document, returning JSON fields as strings for this response",
			mlog.Int("rows", len(queryResp)))
		jsonFieldsToStrings(queryResp, jsonFieldNames)
	}

	return queryResp, nil
}

// jsonFieldsToStrings turns the named fields back into their textual form.
func jsonFieldsToStrings(rows []map[string]interface{}, fields map[string]struct{}) {
	for _, row := range rows {
		for name := range fields {
			if raw, ok := row[name].(json.RawMessage); ok {
				row[name] = string(raw)
			}
		}
	}
}

func hasSearchAggregationResult(results *schemapb.SearchResultData) bool {
	return results != nil && (len(results.GetAggTopks()) > 0 || len(results.GetAggBuckets()) > 0)
}

// buildSearchAggregationResp renders the aggregation payload. Every failure below
// it is a server-side reduce contract violation with no output field to blame, so
// it would otherwise reach the caller as a bare "fail to parse search result".
// Naming the stage keeps the response actionable enough to report while the
// contract detail stays in the log.
func buildSearchAggregationResp(results *schemapb.SearchResultData, enableInt64 bool, collectionSchema *schemapb.CollectionSchema) ([]gin.H, error) {
	output, err := buildSearchAggregationRespData(results, enableInt64, collectionSchema)
	if err != nil {
		return nil, wrapResultStageErr("search aggregation result", err)
	}
	return output, nil
}

func buildSearchAggregationRespData(results *schemapb.SearchResultData, enableInt64 bool, collectionSchema *schemapb.CollectionSchema) ([]gin.H, error) {
	if results == nil {
		// The aggregation payload is produced by the server-side reduce, never
		// by the request: a malformed shape is an internal contract violation.
		return nil, merr.WrapErrServiceInternalMsg("search_aggregation result is nil")
	}
	aggTopks := results.GetAggTopks()
	pbBuckets := results.GetAggBuckets()
	if len(aggTopks) == 0 {
		return nil, merr.WrapErrServiceInternalMsg("search_aggregation response missing agg_topks")
	}
	if results.GetNumQueries() <= 0 {
		return nil, merr.WrapErrServiceInternalMsg("search_aggregation response missing nq")
	}
	if len(aggTopks) != int(results.GetNumQueries()) {
		return nil, merr.WrapErrServiceInternalMsg("search_aggregation agg_topks length %d does not match nq %d", len(aggTopks), results.GetNumQueries())
	}

	total := int64(0)
	for _, topk := range aggTopks {
		if topk < 0 {
			return nil, merr.WrapErrServiceInternalMsg("search_aggregation agg_topks cannot contain negative values")
		}
		total += topk
	}
	if total != int64(len(pbBuckets)) {
		return nil, merr.WrapErrServiceInternalMsg("search_aggregation agg_topks sum %d does not match bucket count %d", total, len(pbBuckets))
	}

	output := make([]gin.H, 0, len(aggTopks))
	offset := 0
	for _, topk := range aggTopks {
		buckets := make([]gin.H, 0, int(topk))
		for i := int64(0); i < topk; i++ {
			bucket, err := buildAggBucketResp(pbBuckets[offset], enableInt64, collectionSchema)
			if err != nil {
				return nil, err
			}
			buckets = append(buckets, bucket)
			offset++
		}
		output = append(output, gin.H{"buckets": buckets})
	}
	return output, nil
}

func buildAggBucketResp(pb *schemapb.AggBucket, enableInt64 bool, collectionSchema *schemapb.CollectionSchema) (gin.H, error) {
	if pb == nil {
		return nil, merr.WrapErrServiceInternalMsg("search_aggregation bucket is nil")
	}
	bucket := gin.H{
		"key":       buildAggBucketKeyResp(pb.GetKey(), enableInt64),
		"count":     formatRESTInt64(pb.GetCount(), enableInt64),
		"metrics":   buildAggMetricsResp(pb.GetMetrics(), enableInt64),
		"hits":      buildAggHitsResp(pb.GetHits(), enableInt64, collectionSchema),
		"subGroups": []gin.H{},
	}
	subGroups := make([]gin.H, 0, len(pb.GetSubGroups()))
	for _, sub := range pb.GetSubGroups() {
		subGroup, err := buildAggBucketResp(sub, enableInt64, collectionSchema)
		if err != nil {
			return nil, err
		}
		subGroups = append(subGroups, subGroup)
	}
	bucket["subGroups"] = subGroups
	return bucket, nil
}

func buildAggBucketKeyResp(keys []*schemapb.BucketKeyEntry, enableInt64 bool) []gin.H {
	resp := make([]gin.H, 0, len(keys))
	for _, key := range keys {
		if key == nil {
			resp = append(resp, gin.H{})
			continue
		}
		fieldName := key.GetFieldName()
		if fieldName == "" {
			fieldName = strconv.FormatInt(key.GetFieldId(), 10)
		}
		resp = append(resp, gin.H{
			"fieldName": fieldName,
			"fieldId":   formatRESTInt64(key.GetFieldId(), enableInt64),
			"value":     bucketKeyEntryValueToRESTAny(key, enableInt64),
		})
	}
	return resp
}

func buildAggMetricsResp(metrics map[string]*schemapb.MetricValue, enableInt64 bool) gin.H {
	resp := make(gin.H, len(metrics))
	for alias, metric := range metrics {
		resp[alias] = metricValueToRESTAny(metric, enableInt64)
	}
	return resp
}

func buildAggHitsResp(hits []*schemapb.AggHit, enableInt64 bool, collectionSchema *schemapb.CollectionSchema) []gin.H {
	resp := make([]gin.H, 0, len(hits))
	pkFieldName := getRESTPrimaryFieldName(collectionSchema)
	for _, hit := range hits {
		if hit == nil {
			resp = append(resp, gin.H{})
			continue
		}
		row := gin.H{
			pkFieldName:        aggHitPKToRESTAny(hit, enableInt64),
			HTTPReturnDistance: hit.GetScore(),
		}
		for _, field := range hit.GetFields() {
			if field == nil {
				continue
			}
			fieldName := field.GetFieldName()
			if fieldName == "" {
				fieldName = strconv.FormatInt(field.GetFieldId(), 10)
			}
			row[fieldName] = aggHitFieldValueToRESTAny(field, enableInt64)
		}
		resp = append(resp, row)
	}
	return resp
}

func getRESTPrimaryFieldName(collectionSchema *schemapb.CollectionSchema) string {
	if collectionSchema == nil {
		return DefaultPrimaryFieldName
	}
	for _, field := range collectionSchema.GetFields() {
		if field.GetIsPrimaryKey() {
			return field.GetName()
		}
	}
	return DefaultPrimaryFieldName
}

func formatRESTInt64(v int64, enableInt64 bool) interface{} {
	if enableInt64 {
		return v
	}
	return strconv.FormatInt(v, 10)
}

func metricValueToRESTAny(pb *schemapb.MetricValue, enableInt64 bool) interface{} {
	if pb == nil {
		return nil
	}
	switch v := pb.GetValue().(type) {
	case *schemapb.MetricValue_IntVal:
		return formatRESTInt64(v.IntVal, enableInt64)
	case *schemapb.MetricValue_DoubleVal:
		return v.DoubleVal
	case *schemapb.MetricValue_StringVal:
		return v.StringVal
	case *schemapb.MetricValue_BoolVal:
		return v.BoolVal
	default:
		return nil
	}
}

func bucketKeyEntryValueToRESTAny(pb *schemapb.BucketKeyEntry, enableInt64 bool) interface{} {
	if pb == nil {
		return nil
	}
	switch v := pb.GetValue().(type) {
	case *schemapb.BucketKeyEntry_IntVal:
		return formatRESTInt64(v.IntVal, enableInt64)
	case *schemapb.BucketKeyEntry_StringVal:
		return v.StringVal
	case *schemapb.BucketKeyEntry_BoolVal:
		return v.BoolVal
	default:
		return nil
	}
}

func aggHitPKToRESTAny(pb *schemapb.AggHit, enableInt64 bool) interface{} {
	if pb == nil {
		return nil
	}
	switch v := pb.GetPk().(type) {
	case *schemapb.AggHit_IntPk:
		return formatRESTInt64(v.IntPk, enableInt64)
	case *schemapb.AggHit_StrPk:
		return v.StrPk
	default:
		return nil
	}
}

func aggHitFieldValueToRESTAny(pb *schemapb.AggHitField, enableInt64 bool) interface{} {
	if pb == nil {
		return nil
	}
	switch v := pb.GetValue().(type) {
	case *schemapb.AggHitField_IntVal:
		return formatRESTInt64(v.IntVal, enableInt64)
	case *schemapb.AggHitField_BoolVal:
		return v.BoolVal
	case *schemapb.AggHitField_FloatVal:
		return v.FloatVal
	case *schemapb.AggHitField_DoubleVal:
		return v.DoubleVal
	case *schemapb.AggHitField_StringVal:
		return v.StringVal
	case *schemapb.AggHitField_BytesVal:
		return v.BytesVal
	default:
		return nil
	}
}

func formatInt64(intArray []int64) []string {
	stringArray := make([]string, 0, len(intArray))
	for _, i := range intArray {
		stringArray = append(stringArray, strconv.FormatInt(i, 10))
	}
	return stringArray
}

func CheckLimiter(ctx context.Context, req interface{}, pxy types.ProxyComponent) (any, error) {
	if !paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.GetAsBool() {
		return nil, nil
	}
	// apply limiter for http/http2 server
	limiter, err := pxy.GetRateLimiter()
	if err != nil {
		mlog.Error(ctx, "Get proxy rate limiter for httpV1/V2 server failed", mlog.Err(err))
		return nil, err
	}

	request, ok := req.(proto.Message)
	if !ok {
		return nil, merr.WrapErrParameterInvalidMsg("wrong req format when check limiter")
	}

	metaCache := getProxyMetaCache(pxy)
	dbID, collectionIDToPartIDs, rt, n, err := proxy.GetRequestInfo(ctx, metaCache(), request)
	if err != nil {
		return nil, err
	}
	err = limiter.Check(dbID, collectionIDToPartIDs, rt, n)
	nodeID := strconv.FormatInt(paramtable.GetNodeID(), 10)
	metrics.ProxyRateLimitReqCount.WithLabelValues(nodeID, rt.String(), metrics.TotalLabel).Inc()
	if err != nil {
		metrics.ProxyRateLimitReqCount.WithLabelValues(nodeID, rt.String(), metrics.FailLabel).Inc()
		return proxy.GetFailedResponse(req, err), err
	}
	metrics.ProxyRateLimitReqCount.WithLabelValues(nodeID, rt.String(), metrics.SuccessLabel).Inc()
	return nil, nil
}

func convertConsistencyLevel(reqConsistencyLevel string) (commonpb.ConsistencyLevel, bool, error) {
	if reqConsistencyLevel != "" {
		level, ok := commonpb.ConsistencyLevel_value[reqConsistencyLevel]
		if !ok {
			return 0, false, merr.WrapErrParameterInvalidMsg("parameter:'%s' is incorrect, please check it", reqConsistencyLevel)
		}
		return commonpb.ConsistencyLevel(level), false, nil
	}
	// ConsistencyLevel_Bounded default in PyMilvus
	return commonpb.ConsistencyLevel_Bounded, true, nil
}

func convertDefaultValue(value interface{}, dataType schemapb.DataType) (*schemapb.ValueField, error) {
	if value == nil {
		return nil, nil
	}
	switch dataType {
	case schemapb.DataType_Bool:
		v, ok := value.(bool)
		if !ok {
			return nil, merr.WrapErrParameterInvalidMsg(`cannot use "%v"(type: %T) as bool default value`, value, value)
		}
		data := &schemapb.ValueField{
			Data: &schemapb.ValueField_BoolData{
				BoolData: v,
			},
		}
		return data, nil

	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		// all passed number is float64 type
		v, ok := value.(float64)
		if !ok {
			return nil, merr.WrapErrParameterInvalidMsg(`cannot use ""%v"(type: %T) as int default value`, value, value)
		}
		data := &schemapb.ValueField{
			Data: &schemapb.ValueField_IntData{
				IntData: int32(v),
			},
		}
		return data, nil

	case schemapb.DataType_Int64:
		v, ok := value.(float64)
		if !ok {
			return nil, merr.WrapErrParameterInvalidMsg(`cannot use "%v"(type: %T) as long default value`, value, value)
		}
		data := &schemapb.ValueField{
			Data: &schemapb.ValueField_LongData{
				LongData: int64(v),
			},
		}
		return data, nil

	case schemapb.DataType_Float:
		v, ok := value.(float64)
		if !ok {
			return nil, merr.WrapErrParameterInvalidMsg(`cannot use "%v"(type: %T) as float default value`, value, value)
		}
		data := &schemapb.ValueField{
			Data: &schemapb.ValueField_FloatData{
				FloatData: float32(v),
			},
		}
		return data, nil

	case schemapb.DataType_Double:
		v, ok := value.(float64)
		if !ok {
			return nil, merr.WrapErrParameterInvalidMsg(`cannot use "%v"(type: %T) as float default value`, value, value)
		}
		data := &schemapb.ValueField{
			Data: &schemapb.ValueField_DoubleData{
				DoubleData: v,
			},
		}
		return data, nil

	case schemapb.DataType_Timestamptz:
		v, ok := value.(string)
		if !ok {
			return nil, merr.WrapErrParameterInvalid("string", value, "Wrong defaultValue type")
		}
		data := &schemapb.ValueField{
			Data: &schemapb.ValueField_StringData{
				StringData: v,
			},
		}
		return data, nil

	case schemapb.DataType_Geometry:
		v, ok := value.(string)
		if !ok {
			return nil, merr.WrapErrParameterInvalidMsg(`cannot use "%v"(type: %T) as geometry default value`, value, value)
		}
		data := &schemapb.ValueField{
			Data: &schemapb.ValueField_StringData{
				StringData: v,
			},
		}
		return data, nil

	case schemapb.DataType_String, schemapb.DataType_VarChar:
		v, ok := value.(string)
		if !ok {
			return nil, merr.WrapErrParameterInvalidMsg(`cannot use "%v"(type: %T) as string default value`, value, value)
		}
		data := &schemapb.ValueField{
			Data: &schemapb.ValueField_StringData{
				StringData: v,
			},
		}
		return data, nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg("Unexpected default value type: %s", dataType.String())
	}
}

func convertToExtraParams(indexParam IndexParam) ([]*commonpb.KeyValuePair, error) {
	var params []*commonpb.KeyValuePair
	if indexParam.IndexType != "" {
		params = append(params, &commonpb.KeyValuePair{Key: common.IndexTypeKey, Value: indexParam.IndexType})
	}
	if indexParam.IndexType == "" {
		for key, value := range indexParam.Params {
			if key == common.IndexTypeKey {
				params = append(params, &commonpb.KeyValuePair{Key: common.IndexTypeKey, Value: fmt.Sprintf("%v", value)})
				break
			}
		}
	}
	if indexParam.MetricType != "" {
		params = append(params, &commonpb.KeyValuePair{Key: common.MetricTypeKey, Value: indexParam.MetricType})
	}
	if len(indexParam.Params) != 0 {
		v, err := json.Marshal(indexParam.Params)
		if err != nil {
			return nil, err
		}
		params = append(params, &commonpb.KeyValuePair{Key: common.ParamsKey, Value: string(v)})
	}
	return params, nil
}

func getElementTypeParams(param interface{}) (string, error) {
	if str, ok := param.(string); ok {
		return str, nil
	}

	jsonBytes, err := json.Marshal(param)
	if err != nil {
		return "", err
	}
	return string(jsonBytes), nil
}

func MetricsHandlerFunc(c *gin.Context) {
	path := c.Request.URL.Path
	metrics.RestfulFunctionCall.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10), path,
	).Inc()
	if c.Request.ContentLength >= 0 {
		metrics.RestfulReceiveBytes.WithLabelValues(
			strconv.FormatInt(paramtable.GetNodeID(), 10), path,
		).Add(float64(c.Request.ContentLength))
	}
	start := time.Now()

	// Process request
	c.Next()

	latency := time.Since(start)
	metrics.RestfulReqLatency.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10), path,
	).Observe(float64(latency.Milliseconds()))

	// see https://github.com/milvus-io/milvus/issues/35767, counter cannot add negative value
	// when response is not written(say timeout/network broken), panicking may happen if not check
	if size := c.Writer.Size(); size > 0 {
		metrics.RestfulSendBytes.WithLabelValues(
			strconv.FormatInt(paramtable.GetNodeID(), 10), path,
		).Add(float64(c.Writer.Size()))
	}
}

func LoggerHandlerFunc() gin.HandlerFunc {
	notlogged := proxy.Params.ProxyCfg.GinLogSkipPaths.GetAsStrings()
	var skip map[string]struct{}
	if length := len(notlogged); length > 0 {
		skip = make(map[string]struct{}, length)
		for _, p := range notlogged {
			skip[p] = struct{}{}
		}
	}

	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		raw := c.Request.URL.RawQuery

		c.Next()

		if _, ok := skip[path]; ok {
			return
		}

		param := gin.LogFormatterParams{
			Request:      c.Request,
			TimeStamp:    time.Now(),
			ClientIP:     c.ClientIP(),
			Method:       c.Request.Method,
			StatusCode:   c.Writer.Status(),
			ErrorMessage: c.Errors.ByType(gin.ErrorTypePrivate).String(),
			BodySize:     c.Writer.Size(),
		}
		param.Latency = param.TimeStamp.Sub(start)
		if param.Latency > time.Minute {
			param.Latency = param.Latency.Truncate(time.Second)
		}
		if raw != "" {
			path = path + "?" + raw
		}
		param.Path = path

		traceID, _ := c.Get("traceID")
		if traceID == nil {
			traceID = ""
		}

		accesslog.SetHTTPParams(c, &param)
		fmt.Fprintf(gin.DefaultWriter, "[%v] [GIN] [%s] [traceID=%s] [code=%3d] [latency=%v] [client=%s] [method=%s] [error=%s]\n",
			param.TimeStamp.Format("2006/01/02 15:04:05.000 Z07:00"),
			param.Path,
			traceID,
			param.StatusCode,
			param.Latency,
			param.ClientIP,
			param.Method,
			param.ErrorMessage,
		)
	}
}

func RequestHandlerFunc(c *gin.Context) {
	_, err := strconv.ParseBool(c.Request.Header.Get(mhttp.HTTPHeaderAllowInt64))
	if err != nil {
		if paramtable.Get().HTTPCfg.AcceptTypeAllowInt64.GetAsBool() {
			c.Request.Header.Set(mhttp.HTTPHeaderAllowInt64, "true")
		} else {
			c.Request.Header.Set(mhttp.HTTPHeaderAllowInt64, "false")
		}
	}
	c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
	c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
	c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With, "+HTTPHeaderIdempotencyKey)
	c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, HEAD, POST, PUT, DELETE, OPTIONS, PATCH")
	c.Writer.Header().Set("X-Content-Type-Options", "nosniff") // Prevents MIME sniffing

	enableHSTS := paramtable.Get().HTTPCfg.EnableHSTS.GetAsBool()
	if enableHSTS {
		maxAge := paramtable.Get().HTTPCfg.HSTSMaxAge.GetValue()
		hstsValue := fmt.Sprintf("max-age=%s", maxAge)
		includeSubDomains := paramtable.Get().HTTPCfg.HSTSIncludeSubDomains.GetAsBool()
		if includeSubDomains {
			hstsValue += "; includeSubDomains"
		}
		c.Writer.Header().Set("Strict-Transport-Security", hstsValue)
	}

	if c.Request.Method == "OPTIONS" {
		c.AbortWithStatus(204)
		return
	}
	c.Next()
}

// templateValueFromJSON converts one expression template parameter from the
// literal the caller wrote.
//
// The values used to arrive already decoded into interface{}, which meant every
// number had been through float64: a filter parameter of 9007199254740993
// silently matched 9007199254740992, while the same value written as a literal
// in the filter string matched correctly. Reading the raw token keeps integers
// exact.
//
// The previous version also panicked on anything it did not expect, so a null,
// an empty array or an object in exprParams reached the recovery handler and
// the caller got a 500.
func templateValueFromJSON(name string, value gjson.Result, depth, maxDepth int) (*schemapb.TemplateValue, error) {
	switch value.Type {
	case gjson.True, gjson.False:
		return &schemapb.TemplateValue{Val: &schemapb.TemplateValue_BoolVal{BoolVal: value.Bool()}}, nil

	case gjson.String:
		return &schemapb.TemplateValue{Val: &schemapb.TemplateValue_StringVal{StringVal: value.Str}}, nil

	case gjson.Number:
		if parsed, ok := parseJSONInteger(value.Raw, 64); ok {
			return &schemapb.TemplateValue{Val: &schemapb.TemplateValue_Int64Val{Int64Val: parsed}}, nil
		}
		// A whole number sitting among the 64-bit integers must not become a
		// float: comparing 9223372036854775809 as a double matches ...808
		// instead, so the filter silently returns the wrong rows. Asked of the
		// value rather than the spelling, so 9223372036854775809.0 and ...809e0
		// are refused too, while 1e20 and 1e300 pass as the doubles they are --
		// see wholeNumberInExactIntegerRange for the bounds and what they cost.
		if err := checkTemplateIntegerRange(name, value, depth, maxDepth); err != nil {
			return nil, err
		}
		floating, err := strconv.ParseFloat(value.Raw, 64)
		if err != nil || math.IsInf(floating, 0) {
			return nil, merr.WrapErrParameterInvalidMsg(
				"expression template parameter %s has an unrepresentable number %s", name, value.Raw)
		}
		return &schemapb.TemplateValue{Val: &schemapb.TemplateValue_FloatVal{FloatVal: floating}}, nil

	case gjson.JSON:
		if value.IsArray() {
			array, err := templateArrayFromJSON(name, value, depth, maxDepth)
			if err != nil {
				return nil, err
			}
			return &schemapb.TemplateValue{Val: &schemapb.TemplateValue_ArrayVal{ArrayVal: array}}, nil
		}
		return nil, merr.WrapErrParameterInvalidMsg(
			"expression template parameter %s must be a bool, number, string or array", name)

	default:
		// gjson.Null, which also covers an absent member
		return nil, merr.WrapErrParameterInvalidMsg(
			"expression template parameter %s must not be null", name)
	}
}

// templateElementType reports the element type an array member contributes. A
// mixed array falls back to JSON, matching what the typed-array branches below
// can hold.
func templateElementType(value gjson.Result) schemapb.DataType {
	switch value.Type {
	case gjson.True, gjson.False:
		return schemapb.DataType_Bool
	case gjson.String:
		return schemapb.DataType_String
	case gjson.Number:
		if _, ok := parseJSONInteger(value.Raw, 64); ok {
			return schemapb.DataType_Int64
		}
		return schemapb.DataType_Float
	case gjson.JSON:
		if value.IsArray() {
			return schemapb.DataType_Array
		}
		return schemapb.DataType_JSON
	default:
		return schemapb.DataType_JSON
	}
}

// templateArrayFromJSON converts an array parameter. An empty array used to
// index element zero and panic.
// checkTemplateIntegerRange rejects a whole-number literal that cannot survive
// the trip to a double, anywhere inside an expression template parameter.
//
// Such a literal is only ever carried as a double, which the planner compares
// against exact integers, so a value the conversion rounds silently matches a
// neighboring row instead of nothing.
//
// The question is asked of the value, not of the spelling. An earlier version
// skipped anything containing "." or "e" on the grounds that the caller had
// asked for a double, but that made the same number behave two ways:
// 9223372036854775809 was refused while 9223372036854775809.0 was accepted and
// then matched the row holding 9223372036854775808. A whole number written with
// a zero fraction is still a whole number.
//
// The version after that refused every whole number outside int64, which also
// refused values that are honest doubles: 1e20 is far past any 64-bit integer
// and can only mean the double, so refusing it protected nothing. The refusal
// is now scoped to the range where it protects something -- see
// wholeNumberInExactIntegerRange, which also states what the scoping costs.
func checkTemplateIntegerRange(name string, value gjson.Result, depth, maxDepth int) error {
	switch {
	case value.IsArray(), value.IsObject():
		if depth >= maxDepth {
			return templateDepthExceeded(name, maxDepth)
		}
		var err error
		value.ForEach(func(_, element gjson.Result) bool {
			err = checkTemplateIntegerRange(name, element, depth+1, maxDepth)
			return err == nil
		})
		return err

	case value.Type == gjson.Number:
		if _, ok := parseJSONInteger(value.Raw, 64); ok {
			return nil
		}
		if !wholeNumberInExactIntegerRange(value.Raw) {
			return nil
		}
		return merr.WrapErrParameterInvalidMsg(
			"expression template parameter %s has a whole number %s that can only be carried as a double, "+
				"where it can no longer be told apart from neighboring 64-bit integers", name, value.Raw)
	}
	return nil
}

// wholeNumberInExactIntegerRange reports whether a number literal that is not
// an int64 is a whole number that lands, as a double, among the values the
// engine holds as exact 64-bit integers.
//
// What this rests on: parseJSONInteger reads the literal exactly, in every
// notation, so every whole number an int64 can hold has already been taken
// before this is asked. A whole number arriving here therefore has a magnitude
// of at least 2^63, and the only question left is whether it is still close
// enough to the 64-bit integers to be confused with one. The magnitude is read
// from the double, which is what makes the bounds exact: a literal that rounds
// onto 2^63 or 2^64 answers with that value and is caught, however it was
// spelled.
//
// What it costs, stated plainly. Telling a whole number the double carries
// exactly from one it rounds needs exact arithmetic on the literal, which is
// precisely what this no longer does -- an arbitrary-precision expansion turns
// a dozen bytes such as 1e-1000000 into a million-digit rational, and a body
// full of them into minutes of CPU and gigabytes of allocation. So the whole
// window is refused rather than only its unsafe half, and the values that pay
// for it are the ones a double happens to carry exactly: 2^63 and 2^64
// themselves, and the one round magnitude between them, 1e19.
//
// Where that leaves the caller, measured against the filter text rather than
// assumed. Written as an integer, 9223372036854775809 is refused there too --
// the parser reads it with ParseInt and reports an overflow -- so the two
// paths now agree, and this closes a hole where the template path was the more
// permissive of the two while quietly answering with the wrong rows. Written
// with a point or an exponent the parser takes the same magnitude as a double
// and accepts it, so the paths part company there and this one is the stricter:
// that acceptance is the ambiguity being refused here, not a workaround to
// point the caller at.
//
// Below 2^63 every whole number is an int64, and above 2^64 both sides of the
// comparison are doubles that went through the same rounding, so nothing
// outside the window changes.
func wholeNumberInExactIntegerRange(raw string) bool {
	floating, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsInf(floating, 0) {
		// unrepresentable as a double at all; the caller reports that instead
		return false
	}
	magnitude := math.Abs(floating)
	if magnitude < math.Ldexp(1, 63) || magnitude > math.Ldexp(1, 64) {
		return false
	}
	return isWholeNumberLiteral(raw)
}

// isWholeNumberLiteral reports whether a JSON number denotes a whole number,
// whatever notation it is written in: 5, 5.0, 5e0, 500e-2 and 1e300 all do,
// while 1.5 and 1e-3 do not.
//
// The literal is read rather than evaluated: the exponent moves the decimal
// point through the mantissa's digits, and whatever lands to the right of it
// must be zeros. Nothing is computed, so the work is one pass over the literal
// and a hostile exponent buys the sender nothing.
func isWholeNumberLiteral(raw string) bool {
	mantissa := raw
	exponent := 0
	if at := strings.IndexAny(raw, "eE"); at >= 0 {
		mantissa = raw[:at]
		parsed, err := strconv.Atoi(raw[at+1:])
		if err != nil {
			// Too many exponent digits to hold in an int, so only the sign
			// matters: every digit ends up left of the point, or the value
			// ends up strictly between zero and one. A zero coefficient
			// cannot reach here -- parseJSONInteger reads it as the integer 0.
			return !strings.HasPrefix(raw[at+1:], "-")
		}
		exponent = parsed
	}
	mantissa = strings.TrimPrefix(mantissa, "-")

	integer, fraction := mantissa, ""
	if at := strings.IndexByte(mantissa, '.'); at >= 0 {
		integer, fraction = mantissa[:at], mantissa[at+1:]
	}
	digits := integer + fraction
	point := len(integer) + exponent
	if point >= len(digits) {
		return true
	}
	if point < 0 {
		point = 0
	}
	for _, digit := range []byte(digits[point:]) {
		if digit != '0' {
			return false
		}
	}
	return true
}

// templateDepthExceeded is the one answer every walk gives past the bound, so
// the caller sees the same refusal wherever the depth is discovered.
func templateDepthExceeded(name string, maxDepth int) error {
	return merr.WrapErrParameterInvalidMsg(
		"expression template parameter %s exceeds the maximum nesting depth %d", name, maxDepth)
}

func templateArrayFromJSON(name string, value gjson.Result, depth, maxDepth int) (*schemapb.TemplateArrayValue, error) {
	if depth >= maxDepth {
		return nil, templateDepthExceeded(name, maxDepth)
	}
	elements := value.Array()
	if len(elements) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg(
			"expression template parameter %s must not be an empty array", name)
	}

	dtype := templateElementType(elements[0])
	for _, element := range elements {
		if element.Type == gjson.Null {
			return nil, merr.WrapErrParameterInvalidMsg(
				"expression template parameter %s must not contain null", name)
		}
		// Check every element before the array's type is inferred, and look
		// inside the ones that nest. An integer that does not fit an int64 is
		// classified as a float, so in a mixed array it reaches the JSON branch
		// and is compared as a double: 9223372036854775809 matched ...808. The
		// same holds one level down, where [[9223372036854775809], "x"] made the
		// array mixed and carried the integer along unchecked.
		if err := checkTemplateIntegerRange(name, element, depth+1, maxDepth); err != nil {
			return nil, err
		}
		if templateElementType(element) != dtype {
			dtype = schemapb.DataType_JSON
		}
	}

	switch dtype {
	case schemapb.DataType_Bool:
		result := make([]bool, len(elements))
		for i, element := range elements {
			result[i] = element.Bool()
		}
		return &schemapb.TemplateArrayValue{
			Data: &schemapb.TemplateArrayValue_BoolData{BoolData: &schemapb.BoolArray{Data: result}},
		}, nil

	case schemapb.DataType_String:
		result := make([]string, len(elements))
		for i, element := range elements {
			result[i] = element.Str
		}
		return &schemapb.TemplateArrayValue{
			Data: &schemapb.TemplateArrayValue_StringData{StringData: &schemapb.StringArray{Data: result}},
		}, nil

	case schemapb.DataType_Int64:
		result := make([]int64, len(elements))
		for i, element := range elements {
			parsed, ok := parseJSONInteger(element.Raw, 64)
			if !ok {
				return nil, merr.WrapErrParameterInvalidMsg(
					"expression template parameter %s has an integer %s outside the 64-bit range",
					name, element.Raw)
			}
			result[i] = parsed
		}
		return &schemapb.TemplateArrayValue{
			Data: &schemapb.TemplateArrayValue_LongData{LongData: &schemapb.LongArray{Data: result}},
		}, nil

	case schemapb.DataType_Float:
		result := make([]float64, len(elements))
		for i, element := range elements {
			floating, err := strconv.ParseFloat(element.Raw, 64)
			if err != nil || math.IsInf(floating, 0) {
				return nil, merr.WrapErrParameterInvalidMsg(
					"expression template parameter %s has an unrepresentable number %s", name, element.Raw)
			}
			result[i] = floating
		}
		return &schemapb.TemplateArrayValue{
			Data: &schemapb.TemplateArrayValue_DoubleData{DoubleData: &schemapb.DoubleArray{Data: result}},
		}, nil

	case schemapb.DataType_Array:
		result := make([]*schemapb.TemplateArrayValue, len(elements))
		for i, element := range elements {
			nested, err := templateArrayFromJSON(name, element, depth+1, maxDepth)
			if err != nil {
				return nil, err
			}
			result[i] = nested
		}
		return &schemapb.TemplateArrayValue{
			Data: &schemapb.TemplateArrayValue_ArrayData{ArrayData: &schemapb.TemplateArrayValueArray{Data: result}},
		}, nil

	default:
		// Mixed element types travel as raw JSON documents.
		result := make([][]byte, len(elements))
		for i, element := range elements {
			result[i] = []byte(element.Raw)
		}
		return &schemapb.TemplateArrayValue{
			Data: &schemapb.TemplateArrayValue_JsonData{JsonData: &schemapb.JSONArray{Data: result}},
		}, nil
	}
}

// maxExprParamsDepthCeiling bounds the configurable bound. The recursion the
// setting protects is the reason it exists; a configuration cannot be allowed
// to configure the protection away.
const maxExprParamsDepthCeiling = 1024

// generateExpressionTemplate converts every expression template parameter,
// reporting the first one that cannot be represented instead of panicking.
//
// The walk is recursive over the caller's nesting, so the depth is bounded --
// by proxy.http.maxExprParamsDepth, read once per request and clamped to a
// ceiling the configuration cannot raise. Arrays and objects both count. Not
// gated by compatibilityMode: this is a resource bound, not a value rule, and
// the previous handling it would restore is a stack overflow.
func generateExpressionTemplate(params map[string]json.RawMessage) (map[string]*schemapb.TemplateValue, error) {
	maxDepth := paramtable.Get().HTTPCfg.MaxExprParamsDepth.GetAsInt()
	if maxDepth > maxExprParamsDepthCeiling {
		maxDepth = maxExprParamsDepthCeiling
	}
	if maxDepth < 1 {
		// zero or negative cannot mean "most permissive"; a bound set below
		// the smallest usable value falls back to the smallest usable value
		maxDepth = 1
	}
	expressionTemplate := make(map[string]*schemapb.TemplateValue, len(params))
	for name, raw := range params {
		value, err := templateValueFromJSON(name, gjson.ParseBytes(raw), 0, maxDepth)
		if err != nil {
			return nil, err
		}
		expressionTemplate[name] = value
	}
	return expressionTemplate, nil
}

func WrapErrorToResponse(err error) *milvuspb.BoolResponse {
	return &milvuspb.BoolResponse{
		Status: merr.Status(err),
	}
}

// searchParamsRootContainAny asks only the root of searchParams, not the
// nested params object: the nested copies are inert (the proxy reads only
// standalone pairs), so the questions "is grouping enabled" and "does a
// spelling conflict" must be asked of the keys that actually act.
func searchParamsRootContainAny(reqSearchParams map[string]interface{}, keys ...string) bool {
	for _, key := range keys {
		if _, ok := reqSearchParams[key]; ok {
			return true
		}
	}
	return false
}

func searchParamsContainAny(reqSearchParams map[string]interface{}, keys ...string) bool {
	for _, key := range keys {
		if _, ok := reqSearchParams[key]; ok {
			return true
		}
	}

	params, ok := reqSearchParams[Params]
	if !ok {
		return false
	}
	paramsMap, ok := params.(map[string]interface{})
	if !ok {
		return false
	}
	for _, key := range keys {
		if _, ok := paramsMap[key]; ok {
			return true
		}
	}
	return false
}

// after 2.5.2, all parameters of search_params can be written into one layer
// no more parameters will be written searchParams.params
// to ensure compatibility and milvus can still get a json format parameter
// try to write all the parameters under searchParams into searchParams.Params
func generateSearchParams(reqSearchParams map[string]interface{}) ([]*commonpb.KeyValuePair, error) {
	var searchParams []*commonpb.KeyValuePair
	var params interface{}
	if val, ok := reqSearchParams[Params]; ok {
		params = val
	}

	paramsMap := make(map[string]interface{})
	if params != nil {
		var ok bool
		if paramsMap, ok = params.(map[string]interface{}); !ok {
			return nil, merr.WrapErrParameterInvalidMsg("searchParams.params must be a dict")
		}
	}

	deepEqual := func(value1, value2 interface{}) bool {
		// try to handle 10.0==10
		switch v1 := value1.(type) {
		case float64:
			if v2, ok := value2.(int); ok {
				return v1 == float64(v2)
			}
		case int:
			if v2, ok := value2.(float64); ok {
				return float64(v1) == v2
			}
		}
		return reflect.DeepEqual(value1, value2)
	}

	for key, value := range reqSearchParams {
		if val, ok := paramsMap[key]; ok {
			if !deepEqual(val, value) {
				return nil, merr.WrapErrParameterInvalidMsg("ambiguous parameter: %s, in search_param: %v, in search_param.params: %v", key, value, val)
			}
		} else if key != Params {
			paramsMap[key] = value
		}
	}

	bs, _ := json.Marshal(paramsMap)
	searchParams = append(searchParams, &commonpb.KeyValuePair{Key: Params, Value: string(bs)})

	for key, value := range reqSearchParams {
		if key != Params {
			// for compatibility
			if key == "ignoreGrowing" {
				key = common.IgnoreGrowing
			}
			searchParams = append(searchParams, &commonpb.KeyValuePair{Key: key, Value: fmt.Sprintf("%v", value)})
		}
	}
	// need to exposure ParamRoundDecimal in req?
	searchParams = append(searchParams, &commonpb.KeyValuePair{Key: ParamRoundDecimal, Value: "-1"})
	return searchParams, nil
}

func convertSearchAggregationReq(req *SearchAggregationReq) (*commonpb.SearchAggregationSpec, error) {
	if req == nil {
		return nil, nil
	}
	if len(req.Fields) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("searchAggregation.fields must be non-empty")
	}
	fields := make([]string, 0, len(req.Fields))
	for _, field := range req.Fields {
		field = strings.TrimSpace(field)
		if field == "" {
			return nil, merr.WrapErrParameterInvalidMsg("searchAggregation.fields must contain non-empty field names")
		}
		fields = append(fields, field)
	}
	if req.Size <= 0 {
		return nil, merr.WrapErrParameterInvalidMsg("searchAggregation.size must be positive")
	}
	if req.SearchSize < 0 {
		return nil, merr.WrapErrParameterInvalidMsg("searchAggregation.searchSize must be non-negative")
	}
	if req.SearchSize > 0 && req.SearchSize < req.Size {
		return nil, merr.WrapErrParameterInvalidMsg("searchAggregation.searchSize must be greater than or equal to size")
	}

	spec := &commonpb.SearchAggregationSpec{
		Fields:     fields,
		Size:       req.Size,
		SearchSize: req.SearchSize,
	}

	if len(req.Metrics) > 0 {
		spec.Metrics = make(map[string]*commonpb.MetricAggSpec, len(req.Metrics))
	}
	for alias, metric := range req.Metrics {
		alias = strings.TrimSpace(alias)
		op := strings.TrimSpace(metric.Op)
		fieldName := strings.TrimSpace(metric.FieldName)
		if alias == "" {
			return nil, merr.WrapErrParameterInvalidMsg("searchAggregation.metrics alias must be non-empty")
		}
		if op == "" {
			return nil, merr.WrapErrParameterInvalidMsg("searchAggregation.metrics.%s.op must be non-empty", alias)
		}
		if fieldName == "" {
			return nil, merr.WrapErrParameterInvalidMsg("searchAggregation.metrics.%s.fieldName must be non-empty", alias)
		}
		spec.Metrics[alias] = &commonpb.MetricAggSpec{Op: op, FieldName: fieldName}
	}

	for _, order := range req.Order {
		key := strings.TrimSpace(order.Key)
		direction := strings.TrimSpace(order.Direction)
		if key == "" {
			return nil, merr.WrapErrParameterInvalidMsg("searchAggregation.order key must be non-empty")
		}
		if direction == "" {
			return nil, merr.WrapErrParameterInvalidMsg("searchAggregation.order direction must be non-empty")
		}
		spec.Order = append(spec.Order, &commonpb.OrderSpec{Key: key, Direction: direction})
	}

	if req.TopHits != nil {
		topHits, err := convertTopHitsReq(req.TopHits)
		if err != nil {
			return nil, err
		}
		spec.TopHits = topHits
	}

	if req.SubAggregation != nil {
		sub, err := convertSearchAggregationReq(req.SubAggregation)
		if err != nil {
			return nil, err
		}
		spec.SubAggregation = sub
	}

	return spec, nil
}

func convertTopHitsReq(req *TopHitsReq) (*commonpb.TopHitsSpec, error) {
	if req.Size <= 0 {
		return nil, merr.WrapErrParameterInvalidMsg("searchAggregation.topHits.size must be positive")
	}
	spec := &commonpb.TopHitsSpec{Size: req.Size}
	for _, sort := range req.Sort {
		fieldName := strings.TrimSpace(sort.FieldName)
		direction := strings.TrimSpace(sort.Direction)
		if fieldName == "" {
			return nil, merr.WrapErrParameterInvalidMsg("searchAggregation.topHits.sort fieldName must be non-empty")
		}
		if direction == "" {
			return nil, merr.WrapErrParameterInvalidMsg("searchAggregation.topHits.sort direction must be non-empty")
		}
		spec.Sort = append(spec.Sort, &commonpb.SortSpec{FieldName: fieldName, Direction: direction})
	}
	return spec, nil
}

func genFunctionSchema(ctx context.Context, function *FunctionSchema) (*schemapb.FunctionSchema, error) {
	functionTypeValue, ok := schemapb.FunctionType_value[function.FunctionType]
	if !ok {
		mlog.Warn(ctx, "function's data type is invalid(case sensitive).", mlog.Any("function.DataType", function.FunctionType), mlog.Any("function", function))
		return nil, merr.WrapErrParameterInvalidMsg("Unsupported function type: %s", function.FunctionType)
	}
	functionType := schemapb.FunctionType(functionTypeValue)
	description := function.Description
	params := []*commonpb.KeyValuePair{}
	for key, value := range function.Params {
		if reflect.TypeOf(value).Kind() == reflect.Slice || reflect.TypeOf(value).Kind() == reflect.Map {
			bs, err := json.Marshal(value)
			if err != nil {
				return nil, merr.WrapErrParameterInvalidMsg("Marshal function params fail, please check it!")
			}
			params = append(params, &commonpb.KeyValuePair{Key: key, Value: string(bs)})
		} else {
			params = append(params, &commonpb.KeyValuePair{Key: key, Value: fmt.Sprintf("%v", value)})
		}
	}
	return &schemapb.FunctionSchema{
		Name:             function.FunctionName,
		Description:      description,
		Type:             functionType,
		InputFieldNames:  function.InputFieldNames,
		OutputFieldNames: function.OutputFieldNames,
		Params:           params,
	}, nil
}

func genFunctionScore(ctx context.Context, functionScore *FunctionScore) (*schemapb.FunctionScore, error) {
	fScore := schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{},
		Params:    []*commonpb.KeyValuePair{},
	}
	for _, function := range functionScore.Functions {
		f, err := genFunctionSchema(ctx, &function)
		if err != nil {
			return nil, err
		}
		fScore.Functions = append(fScore.Functions, f)
	}
	for key, value := range functionScore.Params {
		fScore.Params = append(fScore.Params, &commonpb.KeyValuePair{Key: key, Value: fmt.Sprintf("%v", value)})
	}
	return &fScore, nil
}

func genFunctionChains(chains []FunctionChainReq) ([]*schemapb.FunctionChain, error) {
	result := make([]*schemapb.FunctionChain, 0, len(chains))
	for i, chainReq := range chains {
		chainPB, err := genFunctionChain(chainReq)
		if err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("functionChains[%d]: %v", i, err)
		}
		result = append(result, chainPB)
	}
	return result, nil
}

func genFunctionChain(req FunctionChainReq) (*schemapb.FunctionChain, error) {
	stage, err := genFunctionChainStage(req.Stage)
	if err != nil {
		return nil, err
	}

	ops := make([]*schemapb.FunctionChainOp, 0, len(req.Ops))
	for i, opReq := range req.Ops {
		opPB, err := genFunctionChainOp(opReq)
		if err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("ops[%d]: %v", i, err)
		}
		ops = append(ops, opPB)
	}

	return &schemapb.FunctionChain{
		Name:  strings.TrimSpace(req.Name),
		Stage: stage,
		Ops:   ops,
	}, nil
}

func genFunctionChainStage(stageName string) (schemapb.FunctionChainStage, error) {
	stageName = strings.TrimSpace(stageName)
	stageValue, ok := schemapb.FunctionChainStage_value[stageName]
	if !ok {
		return schemapb.FunctionChainStage_FunctionChainStageUnspecified, merr.WrapErrParameterInvalidMsg("unsupported function chain stage: %s", stageName)
	}
	stage := schemapb.FunctionChainStage(stageValue)
	if _, err := chain.ProtoStageToReprStage(stage); err != nil {
		return schemapb.FunctionChainStage_FunctionChainStageUnspecified, err
	}
	return stage, nil
}

func genFunctionChainOp(req FunctionChainOpReq) (*schemapb.FunctionChainOp, error) {
	opName := strings.TrimSpace(req.Op)
	if opName == "" {
		return nil, merr.WrapErrParameterInvalidMsg("op name is empty")
	}

	paramMap, err := genFunctionParamMap(req.Params)
	if err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("params: %v", err)
	}

	var exprPB *schemapb.FunctionChainExpr
	if req.Expr != nil {
		exprPB, err = genFunctionChainExpr(*req.Expr)
		if err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("expr: %v", err)
		}
	}

	return &schemapb.FunctionChainOp{
		Op:      opName,
		Expr:    exprPB,
		Inputs:  trimStringList(req.Inputs),
		Outputs: trimStringList(req.Outputs),
		Params:  paramMap,
	}, nil
}

func genFunctionChainExpr(req FunctionChainExprReq) (*schemapb.FunctionChainExpr, error) {
	name := strings.TrimSpace(req.Name)
	if name == "" {
		return nil, merr.WrapErrParameterInvalidMsg("expr name is empty")
	}

	args := make([]*schemapb.FunctionChainExprArg, 0, len(req.Args))
	for i, argReq := range req.Args {
		argPB, err := genFunctionChainExprArg(argReq)
		if err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("args[%d]: %v", i, err)
		}
		args = append(args, argPB)
	}

	params, err := genFunctionParamMap(req.Params)
	if err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("params: %v", err)
	}

	return &schemapb.FunctionChainExpr{
		Name:   name,
		Args:   args,
		Params: params,
	}, nil
}

func genFunctionChainExprArg(req FunctionChainExprArgReq) (*schemapb.FunctionChainExprArg, error) {
	hasColumn := req.Column != nil
	hasLiteral := req.Literal != nil
	if hasColumn == hasLiteral {
		return nil, merr.WrapErrParameterInvalidMsg("exactly one of column or literal is required")
	}
	if hasColumn {
		name := strings.TrimSpace(*req.Column)
		if name == "" {
			return nil, merr.WrapErrParameterInvalidMsg("column name is empty")
		}
		return &schemapb.FunctionChainExprArg{
			Arg: &schemapb.FunctionChainExprArg_Column{
				Column: &schemapb.FunctionChainColumnArg{Name: name},
			},
		}, nil
	}

	literal, err := genFunctionParamValue(req.Literal)
	if err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("literal: %v", err)
	}
	return &schemapb.FunctionChainExprArg{
		Arg: &schemapb.FunctionChainExprArg_Literal{Literal: literal},
	}, nil
}

func genFunctionParamMap(params map[string]interface{}) (map[string]*schemapb.FunctionParamValue, error) {
	result := make(map[string]*schemapb.FunctionParamValue, len(params))
	for key, value := range params {
		paramName := strings.TrimSpace(key)
		if paramName == "" {
			return nil, merr.WrapErrParameterInvalidMsg("param name is empty")
		}
		paramValue, err := genFunctionParamValue(value)
		if err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("param %q: %v", key, err)
		}
		result[paramName] = paramValue
	}
	return result, nil
}

func genFunctionParamValue(value interface{}) (*schemapb.FunctionParamValue, error) {
	switch v := value.(type) {
	case nil:
		return nil, merr.WrapErrParameterInvalidMsg("function param value is nil")
	case bool:
		return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_BoolValue{BoolValue: v}}, nil
	case float64:
		if math.Trunc(v) == v && v >= math.MinInt64 && v <= math.MaxInt64 {
			return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: int64(v)}}, nil
		}
		return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_DoubleValue{DoubleValue: v}}, nil
	case string:
		return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_StringValue{StringValue: v}}, nil
	case []interface{}:
		values := make([]*schemapb.FunctionParamValue, 0, len(v))
		for i, item := range v {
			converted, err := genFunctionParamValue(item)
			if err != nil {
				return nil, merr.WrapErrParameterInvalidMsg("array[%d]: %v", i, err)
			}
			values = append(values, converted)
		}
		return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_ArrayValue{ArrayValue: &schemapb.FunctionParamArray{Values: values}}}, nil
	case map[string]interface{}:
		fields := make(map[string]*schemapb.FunctionParamValue, len(v))
		for key, item := range v {
			fieldName := strings.TrimSpace(key)
			if fieldName == "" {
				return nil, merr.WrapErrParameterInvalidMsg("object field name is empty")
			}
			converted, err := genFunctionParamValue(item)
			if err != nil {
				return nil, merr.WrapErrParameterInvalidMsg("object field %q: %v", key, err)
			}
			fields[fieldName] = converted
		}
		return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_ObjectValue{ObjectValue: &schemapb.FunctionParamObject{Fields: fields}}}, nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg("unsupported function param value type %T", value)
	}
}

func trimStringList(values []string) []string {
	trimmed := make([]string, len(values))
	for i, value := range values {
		trimmed[i] = strings.TrimSpace(value)
	}
	return trimmed
}

// IdempotencyKeyHandlerFunc copies the REST Idempotency-Key header into the gRPC
// incoming metadata, so the proxy reads the key from exactly one place regardless of
// whether the request arrived over REST or gRPC. Existing metadata is preserved.
//
// Registered as middleware rather than inside a handler wrapper: which operations are
// idempotent is decided downstream, so every route — v1 and v2 alike — must carry the
// key rather than drop it and force the next adopter to rediscover this hop.
func IdempotencyKeyHandlerFunc(c *gin.Context) {
	key := c.Request.Header.Get(HTTPHeaderIdempotencyKey)
	if key == "" {
		c.Next()
		return
	}
	// Validate before the key reaches outgoing metadata. Go accepts header bytes
	// gRPC does not, and this middleware is mounted on the whole engine, so an
	// unchecked key would ride along on every coordinator RPC of any v1 or v2
	// route -- see ValidateIdempotencyKey for what that costs.
	if err := interceptor.ValidateIdempotencyKey(key); err != nil {
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(err),
			HTTPReturnMessage: err.Error(),
		})
		return
	}
	ctx := c.Request.Context()
	md, _ := metadata.FromIncomingContext(ctx)
	md = md.Copy()
	md.Set(util.HeaderIdempotencyKey, key)
	c.Request = c.Request.WithContext(metadata.NewIncomingContext(ctx, md))
	c.Next()
}
