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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func loadFixtureSnapshot(t *testing.T) *snapshotSet {
	m, warnings, err := loadSnapshotFile("testdata/snapshot.json")
	require.NoError(t, err)
	require.Empty(t, warnings)
	return m
}

func mustColumn(t *testing.T, info *collectionInfo, identifier string) column {
	col, err := resolveIdentifier(info.helper, identifier)
	require.NoError(t, err, identifier)
	return col
}

func indexNames(list []indexSnapshot) []string {
	var names []string
	for _, idx := range list {
		names = append(names, idx.IndexName+":"+idx.IndexType)
	}
	return names
}

// The fixture was written by the suggested script in README.md against Milvus
// v3.0.3, so it uses the pymilvus type names.
func TestLoadSnapshotFixture(t *testing.T) {
	m := loadFixtureSnapshot(t)

	products, ok := m.lookup("", "products")
	require.True(t, ok, "an empty database name means default")
	assert.Equal(t, "default", products.db)
	assert.Equal(t, []string{"city:INVERTED"}, indexNames(products.indexesOn(mustColumn(t, products, "city"))))
	assert.Empty(t, products.indexesOn(mustColumn(t, products, "price")))

	// enable_dynamic_field adds $meta, so dynamic keys resolve.
	color := mustColumn(t, products, "color")
	assert.True(t, color.isDynamic)
	assert.Equal(t, `$meta["color"]`, color.String())

	// Each way DescribeIndex reports an index resolves to the column it covers.
	probe, ok := m.lookup("default", "idx_probe")
	require.True(t, ok)
	assert.Equal(t, []string{"dyn_color:INVERTED"}, indexNames(probe.indexesOn(mustColumn(t, probe, "color"))))
	assert.Equal(t, []string{"meta_brand:INVERTED"}, indexNames(probe.indexesOn(mustColumn(t, probe, `meta["brand"]`))))
	assert.Empty(t, probe.indexesOn(mustColumn(t, probe, `meta["other"]`)))
	assert.Equal(t, []string{"name_ngram:NGRAM"}, indexNames(probe.indexesOn(mustColumn(t, probe, "name"))))
	assert.Equal(t, []string{"price_sort:STL_SORT"}, indexNames(probe.indexesOn(mustColumn(t, probe, "price"))))

	_, ok = m.lookup("other_db", "products")
	assert.False(t, ok)
}

func TestBuildSchema(t *testing.T) {
	schema, err := buildSchema(collectionSnapshot{
		Name:               "c",
		EnableDynamicField: true,
		Fields: []fieldSnapshot{
			{Name: "id", DataType: "Int64", IsPrimaryKey: true},
			{Name: "v", DataType: "FloatVector"},
			{Name: "tags", DataType: "ARRAY", ElementType: "varchar"},
			{Name: "ts", DataType: "TIMESTAMPTZ"},
			{Name: "s", DataType: "sparse_float_vector"},
		},
	})
	require.NoError(t, err)
	got := lo.Map(schema.GetFields(), func(f *schemapb.FieldSchema, _ int) string {
		return fmt.Sprintf("%d %s %s %s %t %t", f.GetFieldID(), f.GetName(), f.GetDataType(), f.GetElementType(), f.GetIsPrimaryKey(), f.GetIsDynamic())
	})
	assert.Equal(t, []string{
		"100 id Int64 None true false",
		"101 v FloatVector None false false",
		"102 tags Array VarChar false false",
		"103 ts Timestamptz None false false",
		"104 s SparseFloatVector None false false",
		"105 $meta JSON None false true",
	}, got, "type names match without case or underscores, and $meta follows the listed fields")

	static, err := buildSchema(collectionSnapshot{Name: "c", Fields: []fieldSnapshot{{Name: "id", DataType: "Int64"}}})
	require.NoError(t, err)
	assert.Len(t, static.GetFields(), 1, "no $meta without enable_dynamic_field")
}

func TestParseSnapshotErrors(t *testing.T) {
	collection := func(body string) string {
		return `{"format_version": 1, "collections": [{"name": "c", ` + body + `}]}`
	}
	const fields = `"fields": [{"name": "id", "data_type": "Int64", "is_primary_key": true}]`
	cases := map[string]struct{ data, want string }{
		"not json":           {`{`, "invalid snapshot JSON"},
		"wrong value type":   {collection(`"fields": [{"name": "id", "data_type": "Int64", "is_primary_key": "yes"}]`), "invalid snapshot JSON"},
		"no version":         {`{"collections": []}`, "format_version is missing"},
		"version":            {`{"format_version": 2, "collections": []}`, "format_version 2"},
		"no collections":     {`{"format_version": 1}`, "no collections"},
		"duplicate":          {`{"format_version": 1, "collections": [{"name": "c", ` + fields + `}, {"db": "default", "name": "c", ` + fields + `}]}`, "listed twice"},
		"no name":            {`{"format_version": 1, "collections": [{` + fields + `}]}`, "collection 0 has no name"},
		"alias taken":        {`{"format_version": 1, "collections": [{"name": "c", "aliases": ["d"], ` + fields + `}, {"name": "d", ` + fields + `}]}`, "collection or alias default.d listed twice"},
		"no fields":          {collection(`"fields": []`), "no fields"},
		"unnamed field":      {collection(`"fields": [{"data_type": "Int64"}]`), "field 0 has no name"},
		"duplicate field":    {collection(`"fields": [{"name": "id", "data_type": "Int64"}, {"name": "id", "data_type": "Int64"}]`), "field id listed twice"},
		"listed $meta":       {collection(`"fields": [{"name": "$meta", "data_type": "JSON"}]`), "implied by enable_dynamic_field"},
		"no type":            {collection(`"fields": [{"name": "id"}]`), "data type is empty"},
		"unknown type":       {collection(`"fields": [{"name": "id", "data_type": "Int128"}]`), `unknown data type "Int128"`},
		"no element type":    {collection(`"fields": [{"name": "a", "data_type": "Array"}]`), "element_type of field a"},
		"stray element type": {collection(`"fields": [{"name": "a", "data_type": "Int64", "element_type": "Int64"}]`), "not an array"},
		"two primary keys":   {collection(`"fields": [{"name": "a", "data_type": "Int64", "is_primary_key": true}, {"name": "b", "data_type": "Int64", "is_primary_key": true}]`), "2 fields are marked"},
		"unknown index":      {collection(fields + `, "indexes": [{"index_name": "i", "field_name": "nope", "index_type": "INVERTED"}]`), "index i"},
		"index without type": {collection(fields + `, "indexes": [{"index_name": "i", "field_name": "id"}]`), "index 0 has no index_type"},
		"json_path number":   {collection(fields + `, "indexes": [{"index_name": "i", "field_name": "id", "index_type": "INVERTED", "params": {"json_path": 1}}]`), "params.json_path is not a string"},
	}
	for name, c := range cases {
		_, _, err := parseSnapshot([]byte(c.data))
		assert.ErrorContains(t, err, c.want, name)
		assert.True(t, errors.Is(err, merr.ErrParameterInvalid) || errors.Is(err, merr.ErrParameterMissing), name)
	}

	_, _, err := loadSnapshotFile(filepath.Join(t.TempDir(), "missing.json"))
	assert.True(t, errors.Is(err, merr.ErrIoFailed))

	bad := filepath.Join(t.TempDir(), "bad.json")
	require.NoError(t, os.WriteFile(bad, []byte(cases["duplicate"].data), 0o600))
	_, _, err = loadSnapshotFile(bad)
	assert.ErrorContains(t, err, "listed twice")
}

func TestIndexesOnJSONCast(t *testing.T) {
	info := newCollectionInfo("default", "t", testSchemaHelper(t))
	info.addIndex(mustColumn(t, info, `meta["a"]`), indexSnapshot{IndexName: "a_json", IndexType: indexInverted, Params: map[string]any{"json_cast_type": " JSON "}})
	info.addIndex(mustColumn(t, info, `meta["a"]`), indexSnapshot{IndexName: "a_str", IndexType: indexInverted, Params: map[string]any{"json_cast_type": "VARCHAR"}})
	assert.Equal(t, []string{"a_json:INVERTED", "a_str:INVERTED"}, indexNames(info.indexesOn(mustColumn(t, info, `meta["a"]`))))
	assert.Equal(t, []string{"a_json:INVERTED"}, indexNames(info.indexesOn(mustColumn(t, info, `meta["a"]["b"]`))), "only the JSON cast serves paths beneath")
	assert.Empty(t, info.indexesOn(mustColumn(t, info, `meta["a"][0]`)), "not across an array position")
	assert.Empty(t, info.indexesOn(mustColumn(t, info, `meta["ab"]`)))
}

// An alias reaches its collection, which is listed once.
func TestParseSnapshotAliases(t *testing.T) {
	m, _, err := parseSnapshot([]byte(`{"format_version": 1, "collections": [{"db": "shop", "name": "orders_v2", "aliases": ["orders"],
		"fields": [{"name": "id", "data_type": "Int64", "is_primary_key": true}]}]}`))
	require.NoError(t, err)
	require.Len(t, m.collections, 1)
	byAlias, ok := m.lookup("shop", "orders")
	require.True(t, ok)
	byName, _ := m.lookup("shop", "orders_v2")
	assert.Same(t, byName, byAlias)
	assert.Equal(t, "orders_v2", byAlias.name)
	_, ok = m.lookup("default", "orders")
	assert.False(t, ok, "an alias belongs to its collection's database")
}

// Keys the snapshot does not define are ignored with a warning, and the rest
// loads as if they were absent.
func TestParseSnapshotIgnoresUnknownKeys(t *testing.T) {
	m, warnings, err := parseSnapshot([]byte(`{
		"format_version": 1, "exported_by": "script",
		"collections": [{
			"Name": "c", "description": "", "enable_dynamic_field": false,
			"fields": [
				{"name": "id", "data_type": "Int64", "is_primary": true, "field_id": 100},
				{"name": "s", "data_type": "VarChar", "params": {"max_length": 64}, "field_id": 101}
			],
			"indexes": [{"index_name": "s", "field_name": "s", "index_type": "NGRAM", "params": {"min_gram": 2, "max_gram": "3"}}]
		}]
	}`))
	require.NoError(t, err)
	assert.Contains(t, warnings, "ignoring unknown key collections[*].fields[*].is_primary")
	assert.Len(t, warnings, 5)

	c, ok := m.lookup("default", "c")
	require.True(t, ok)
	_, err = c.helper.GetPrimaryKeyField()
	assert.Error(t, err, "is_primary is not read as is_primary_key")
	assert.Equal(t, []string{"s:NGRAM"}, indexNames(c.indexesOn(mustColumn(t, c, "s"))), "index params may hold any JSON value")
}
