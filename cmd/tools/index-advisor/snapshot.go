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

package main

import (
	"os"
	"slices"
	"sort"
	"strings"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	snapshotFormatVersion = 1
	defaultDatabase       = "default"
)

// snapshotFile is the --snapshot input: for each collection, the schema
// details the expression parser and the rules need, and the existing indexes.
// README.md documents the format. Its keys follow the field names of the
// server's proto (schemapb.FieldSchema). Unknown keys are ignored with a
// warning, so that a key in another interface's naming, such as pymilvus's
// is_primary, is reported.
type snapshotFile struct {
	FormatVersion int                  `json:"format_version"`
	Collections   []collectionSnapshot `json:"collections"`
}

// collectionSnapshot is one collection. Aliases are listed because the access
// log records the name a request used, which may be an alias.
type collectionSnapshot struct {
	DB                 string          `json:"db"`
	Name               string          `json:"name"`
	Aliases            []string        `json:"aliases"`
	EnableDynamicField bool            `json:"enable_dynamic_field"`
	Fields             []fieldSnapshot `json:"fields"`
	Indexes            []indexSnapshot `json:"indexes"`
}

// fieldSnapshot is one schema field. DataType and ElementType are DataType
// names, compared without case or underscores so that both the proto names
// (VarChar, FloatVector) and the pymilvus names (VARCHAR, FLOAT_VECTOR) match.
type fieldSnapshot struct {
	Name         string `json:"name"`
	DataType     string `json:"data_type"`
	ElementType  string `json:"element_type,omitempty"` // for ARRAY fields
	IsPrimaryKey bool   `json:"is_primary_key,omitempty"`
}

// indexSnapshot is one index as DescribeIndex reports it. Of its params,
// json_path locates an index on a JSON path, and json_cast_type, when present,
// tells which filters a JSON index serves. The others are not read and may
// hold any JSON value.
type indexSnapshot struct {
	IndexName string         `json:"index_name"`
	FieldName string         `json:"field_name"`
	IndexType string         `json:"index_type"`
	Params    map[string]any `json:"params,omitempty"`
}

// collectionInfo is a loaded collection: its schema, ready for the expression
// parser, and its existing indexes keyed by the column they cover.
type collectionInfo struct {
	db, name       string
	helper         *typeutil.SchemaHelper
	indexes        map[string][]indexSnapshot // column.String() -> indexes on it, sorted by name
	indexedColumns map[string]column          // column.String() -> the column
}

func newCollectionInfo(db, name string, helper *typeutil.SchemaHelper) *collectionInfo {
	if db == "" {
		db = defaultDatabase
	}
	return &collectionInfo{db: db, name: name, helper: helper, indexes: map[string][]indexSnapshot{}, indexedColumns: map[string]column{}}
}

// addIndex records idx as an index on col, keeping each column's list sorted
// by name.
func (c *collectionInfo) addIndex(col column, idx indexSnapshot) {
	list := append(c.indexes[col.String()], idx)
	sort.Slice(list, func(i, j int) bool { return list[i].IndexName < list[j].IndexName })
	c.indexes[col.String()] = list
	c.indexedColumns[col.String()] = col
}

// indexesOn returns the existing indexes that serve col, sorted by name: those
// on col itself and, for a JSON path, those with json_cast_type JSON on a path
// above it.
func (c *collectionInfo) indexesOn(col column) []indexSnapshot {
	out := append([]indexSnapshot(nil), c.indexes[col.String()]...)
	for key, indexed := range c.indexedColumns {
		if key != col.String() && coversPath(indexed, col) {
			out = append(out, lo.Filter(c.indexes[key], func(idx indexSnapshot, _ int) bool { return isJSONCast(idx) })...)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].IndexName < out[j].IndexName })
	return out
}

// isJSONCast reports whether idx was created with json_cast_type JSON, which
// Milvus uses for every path beneath the indexed one.
func isJSONCast(idx indexSnapshot) bool {
	cast, _ := idx.Params[common.JSONCastTypeKey].(string)
	return strings.EqualFold(strings.TrimSpace(cast), "JSON")
}

// coversPath reports whether path lies beneath prefix in the same JSON field
// with no array position in between, which is where Milvus uses an index with
// json_cast_type JSON on prefix (IsPathPrefix and RelativePathContainsInteger
// in internal/core/src/exec/expression/IndexPathSelection.h).
func coversPath(prefix, path column) bool {
	if prefix.field != path.field || !typeutil.IsJSONType(prefix.dataType) || len(prefix.jsonPath) >= len(path.jsonPath) ||
		!slices.Equal(prefix.jsonPath, path.jsonPath[:len(prefix.jsonPath)]) {
		return false
	}
	return !lo.ContainsBy(path.jsonPath[len(prefix.jsonPath):], isArrayPosition)
}

// snapshotSet holds the loaded collections, reachable by name and by alias.
type snapshotSet struct {
	collections []*collectionInfo          // each collection once, in snapshot order
	byName      map[string]*collectionInfo // collectionKey of a name or alias -> collection
}

func collectionKey(db, name string) string {
	if db == "" {
		db = defaultDatabase
	}
	return db + "." + name
}

func (m *snapshotSet) lookup(db, name string) (*collectionInfo, bool) {
	c, ok := m.byName[collectionKey(db, name)]
	return c, ok
}

// loadSnapshotFile loads the snapshot at path. It also returns warnings about
// the keys it ignored.
func loadSnapshotFile(path string) (*snapshotSet, []string, error) {
	data, err := os.ReadFile(path) // #nosec G304 -- path is a CLI argument
	if err != nil {
		return nil, nil, merr.WrapErrIoFailed(path, err)
	}
	m, warnings, err := parseSnapshot(data)
	if err != nil {
		return nil, nil, merr.Wrapf(err, "snapshot file %s", path)
	}
	return m, prefixAll("snapshot file "+path+": ", warnings), nil
}

func parseSnapshot(data []byte) (*snapshotSet, []string, error) {
	var file snapshotFile
	warnings, err := decodeInput(data, &file)
	if err != nil {
		return nil, nil, merr.WrapErrParameterInvalidErr(err, "invalid snapshot JSON")
	}
	if err := checkFormatVersion(file.FormatVersion, snapshotFormatVersion); err != nil {
		return nil, nil, err
	}
	if len(file.Collections) == 0 {
		return nil, nil, merr.WrapErrParameterMissingMsg("snapshot lists no collections")
	}

	set := &snapshotSet{byName: map[string]*collectionInfo{}}
	for i, c := range file.Collections {
		if c.Name == "" {
			return nil, nil, merr.WrapErrParameterMissingMsg("collection %d has no name", i)
		}
		key := collectionKey(c.DB, c.Name)
		info, err := loadCollection(c)
		if err != nil {
			return nil, nil, merr.Wrapf(err, "collection %s", key)
		}
		set.collections = append(set.collections, info)
		for _, name := range append([]string{c.Name}, c.Aliases...) {
			if _, dup := set.byName[collectionKey(c.DB, name)]; dup {
				return nil, nil, merr.WrapErrParameterInvalidMsg("collection or alias %s listed twice", collectionKey(c.DB, name))
			}
			set.byName[collectionKey(c.DB, name)] = info
		}
	}
	return set, warnings, nil
}

func loadCollection(c collectionSnapshot) (*collectionInfo, error) {
	schema, err := buildSchema(c)
	if err != nil {
		return nil, err
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		return nil, merr.Wrap(err, "invalid schema")
	}

	info := newCollectionInfo(c.DB, c.Name, helper)
	for i, idx := range c.Indexes {
		for _, f := range []struct{ key, value string }{{"index_name", idx.IndexName}, {"field_name", idx.FieldName}, {"index_type", idx.IndexType}} {
			if f.value == "" {
				return nil, merr.WrapErrParameterMissingMsg("index %d has no %s", i, f.key)
			}
		}
		col, err := indexColumn(helper, idx)
		if err != nil {
			return nil, merr.Wrapf(err, "index %s", idx.IndexName)
		}
		info.addIndex(col, idx)
	}
	return info, nil
}

// buildSchema turns the snapshot fields into the schema the expression parser
// resolves names against. Field IDs are assigned in order; nothing outside
// this tool sees them. When dynamic fields are enabled, the hidden $meta field
// is added, so that dynamic keys such as color resolve to $meta["color"].
func buildSchema(c collectionSnapshot) (*schemapb.CollectionSchema, error) {
	if len(c.Fields) == 0 {
		return nil, merr.WrapErrParameterMissingMsg("no fields")
	}
	schema := &schemapb.CollectionSchema{Name: c.Name, EnableDynamicField: c.EnableDynamicField}
	seen := map[string]bool{}
	primaryKeys := 0
	for i, f := range c.Fields {
		switch {
		case f.Name == "":
			return nil, merr.WrapErrParameterMissingMsg("field %d has no name", i)
		case f.Name == common.MetaFieldName:
			return nil, merr.WrapErrParameterInvalidMsg("field %s is implied by enable_dynamic_field and must not be listed", common.MetaFieldName)
		case seen[f.Name]:
			return nil, merr.WrapErrParameterInvalidMsg("field %s listed twice", f.Name)
		}
		seen[f.Name] = true
		dataType, err := parseDataType(f.DataType)
		if err != nil {
			return nil, merr.Wrapf(err, "field %s", f.Name)
		}
		field := &schemapb.FieldSchema{
			FieldID:      common.StartOfUserFieldID + int64(i),
			Name:         f.Name,
			DataType:     dataType,
			IsPrimaryKey: f.IsPrimaryKey,
		}
		// The parser accepts text_match only on a field with enable_match, which
		// the snapshot does not carry. Every VARCHAR field is marked with it, so
		// that a text_match filter parses and the other conditions of its
		// expression are analyzed. Whether the request succeeded is taken from
		// its logged status.
		if dataType == schemapb.DataType_VarChar {
			field.TypeParams = []*commonpb.KeyValuePair{{Key: "enable_match", Value: "true"}}
		}
		switch {
		case typeutil.IsArrayType(dataType) || dataType == schemapb.DataType_ArrayOfVector:
			if field.ElementType, err = parseDataType(f.ElementType); err != nil {
				return nil, merr.Wrapf(err, "element_type of field %s", f.Name)
			}
		case f.ElementType != "":
			return nil, merr.WrapErrParameterInvalidMsg("field %s has element_type but is not an array", f.Name)
		}
		if f.IsPrimaryKey {
			primaryKeys++
		}
		schema.Fields = append(schema.Fields, field)
	}
	if primaryKeys > 1 {
		return nil, merr.WrapErrParameterInvalidMsg("%d fields are marked is_primary_key", primaryKeys)
	}
	if c.EnableDynamicField {
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			FieldID:   common.StartOfUserFieldID + int64(len(c.Fields)),
			Name:      common.MetaFieldName,
			DataType:  schemapb.DataType_JSON,
			IsDynamic: true,
		})
	}
	return schema, nil
}

// dataTypes maps each DataType name, lowercased and without underscores, to
// its value.
var dataTypes = func() map[string]schemapb.DataType {
	m := make(map[string]schemapb.DataType, len(schemapb.DataType_value))
	for name, v := range schemapb.DataType_value {
		if schemapb.DataType(v) != schemapb.DataType_None {
			m[normalizeTypeName(name)] = schemapb.DataType(v)
		}
	}
	return m
}()

func normalizeTypeName(name string) string {
	return strings.ToLower(strings.ReplaceAll(name, "_", ""))
}

func parseDataType(name string) (schemapb.DataType, error) {
	if name == "" {
		return schemapb.DataType_None, merr.WrapErrParameterMissingMsg("data type is empty")
	}
	t, ok := dataTypes[normalizeTypeName(name)]
	if !ok {
		return schemapb.DataType_None, merr.WrapErrParameterInvalidMsg("unknown data type %q", name)
	}
	return t, nil
}

// resolveIdentifier parses a field name or JSON path (price, meta["brand"],
// color) with Milvus's identifier parser into the column it names.
func resolveIdentifier(helper *typeutil.SchemaHelper, identifier string) (column, error) {
	var info *planpb.ColumnInfo
	err := planparserv2.ParseIdentifier(helper, identifier, func(e *planpb.Expr) error {
		info = e.GetColumnExpr().GetInfo()
		return nil
	})
	if err != nil {
		return column{}, err
	}
	return (&exprWalker{helper: helper}).column(info), nil
}

// indexColumn resolves the column an index covers, so it can be compared with
// predicate columns. DescribeIndex reports a JSON-path index under field
// "meta" with json_path `meta["brand"]`, and a dynamic-field index under its
// top-level key, such as "color", with json_path only when the index was
// created with one. Parsing json_path when present, and the field name
// otherwise, resolves all of these and plain fields alike.
func indexColumn(helper *typeutil.SchemaHelper, idx indexSnapshot) (column, error) {
	identifier := idx.FieldName
	if v, ok := idx.Params[common.JSONPathKey]; ok {
		path, isString := v.(string)
		if !isString {
			return column{}, merr.WrapErrParameterInvalidMsg("params.%s is not a string", common.JSONPathKey)
		}
		if path != "" {
			identifier = path
		}
	}
	return resolveIdentifier(helper, identifier)
}
