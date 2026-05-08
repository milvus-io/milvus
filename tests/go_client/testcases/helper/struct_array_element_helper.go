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

package helper

import (
	"fmt"
	"math/rand"

	"github.com/milvus-io/milvus/client/v2/entity"
)

// Mirrors constants from tests/python_client/milvus_client/
// test_milvus_client_struct_array_element_query.py and ..._element_search.py.
const (
	StructAElemPrefix      = "struct_elem"
	StructAElemDim         = 128
	StructAElemCapacity    = 10
	StructAElemSealedNb    = 200 // python uses default_nb=3000; smaller for Go SDK runs
	StructAElemGrowingNb   = 50
	StructAElemMaxStrLen   = 65535
	StructAElemMaxColorLen = 128
)

// COLORS and CATEGORIES match the Python fixtures so element_filter expressions and ground-truth
// comparisons stay identical.
var (
	StructAElemColors     = []string{"Red", "Blue", "Green"}
	StructAElemCategories = []string{"A", "B", "C", "D"}
	StructAElemSizes      = []string{"S", "M", "L", "XL"}
)

// StructAElementSchemaOption controls which sub-fields are present in the canonical structA schema.
// Defaults match the union of sub-fields used by the Python tests so a single helper covers all.
type StructAElementSchemaOption struct {
	Dim              int
	Capacity         int
	IncludeDocInt    bool
	IncludeDocVChar  bool // doc_varchar at the row level
	IncludeStrVal    bool
	IncludeFloatVal  bool
	IncludeCategory  bool
	IncludeSize      bool // adds a "size" VarChar sub-field used by element_search tests
	CollectionName   string
	StructFieldName  string // default "structA"
	NormalVectorName string // default "normal_vector"
}

// DefaultStructAElementSchemaOption returns the union schema (every sub-field present), suitable for
// 90 % of element-query/search tests.
func DefaultStructAElementSchemaOption(name string) StructAElementSchemaOption {
	return StructAElementSchemaOption{
		Dim:              StructAElemDim,
		Capacity:         StructAElemCapacity,
		IncludeDocInt:    true,
		IncludeDocVChar:  true,
		IncludeStrVal:    true,
		IncludeFloatVal:  true,
		IncludeCategory:  true,
		CollectionName:   name,
		StructFieldName:  "structA",
		NormalVectorName: "normal_vector",
	}
}

// CreateStructAElementSchema builds the canonical schema. Returns the entity.Schema and the inner
// StructSchema (the latter is needed by WithStructArrayColumn).
func CreateStructAElementSchema(opt StructAElementSchemaOption) (*entity.Schema, *entity.StructSchema) {
	if opt.Dim == 0 {
		opt.Dim = StructAElemDim
	}
	if opt.Capacity == 0 {
		opt.Capacity = StructAElemCapacity
	}
	if opt.StructFieldName == "" {
		opt.StructFieldName = "structA"
	}
	if opt.NormalVectorName == "" {
		opt.NormalVectorName = "normal_vector"
	}

	structSchema := entity.NewStructSchema().
		WithField(entity.NewField().WithName("embedding").
			WithDataType(entity.FieldTypeFloatVector).WithDim(int64(opt.Dim))).
		WithField(entity.NewField().WithName("int_val").
			WithDataType(entity.FieldTypeInt64))
	if opt.IncludeStrVal {
		structSchema.WithField(entity.NewField().WithName("str_val").
			WithDataType(entity.FieldTypeVarChar).WithMaxLength(StructAElemMaxStrLen))
	}
	if opt.IncludeFloatVal {
		structSchema.WithField(entity.NewField().WithName("float_val").
			WithDataType(entity.FieldTypeFloat))
	}
	structSchema.WithField(entity.NewField().WithName("color").
		WithDataType(entity.FieldTypeVarChar).WithMaxLength(StructAElemMaxColorLen))
	if opt.IncludeCategory {
		structSchema.WithField(entity.NewField().WithName("category").
			WithDataType(entity.FieldTypeVarChar).WithMaxLength(StructAElemMaxColorLen))
	}
	if opt.IncludeSize {
		structSchema.WithField(entity.NewField().WithName("size").
			WithDataType(entity.FieldTypeVarChar).WithMaxLength(StructAElemMaxColorLen))
	}

	schema := entity.NewSchema().WithName(opt.CollectionName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true))
	if opt.IncludeDocInt {
		schema.WithField(entity.NewField().WithName("doc_int").WithDataType(entity.FieldTypeInt64))
	}
	if opt.IncludeDocVChar {
		schema.WithField(entity.NewField().WithName("doc_varchar").
			WithDataType(entity.FieldTypeVarChar).WithMaxLength(256))
	}
	schema.WithField(entity.NewField().WithName(opt.NormalVectorName).
		WithDataType(entity.FieldTypeFloatVector).WithDim(int64(opt.Dim)))
	schema.WithField(entity.NewField().WithName(opt.StructFieldName).
		WithDataType(entity.FieldTypeArray).
		WithElementType(entity.FieldTypeStruct).
		WithMaxCapacity(int64(opt.Capacity)).
		WithStructSchema(structSchema))

	return schema, structSchema
}

// StructAElement represents one struct element in a row. Used both as ground-truth source and
// as input to per-row insert generators.
type StructAElement struct {
	Embedding []float32
	IntVal    int64
	StrVal    string
	FloatVal  float32
	Color     string
	Category  string
	Size      string
}

// StructARow represents one row including doc-level fields. Returned by generators and used by
// ground-truth filters.
type StructARow struct {
	ID           int64
	DocInt       int64
	DocVarChar   string
	NormalVector []float32
	StructA      []StructAElement
}

// StructAElementDataset bundles columns ready for insert plus the structured rows for ground truth.
type StructAElementDataset struct {
	Rows []StructARow
	Opt  StructAElementSchemaOption
}

// SeedVector mirrors python `_seed_vector(seed)` — deterministic uniform random float vector.
// Python's helper also normalises so we keep that for embedding/cosine math parity.
func SeedVector(seed int64, dim int) []float32 {
	r := rand.New(rand.NewSource(seed))
	v := make([]float32, dim)
	var norm float64
	for i := range v {
		v[i] = r.Float32()
		norm += float64(v[i]) * float64(v[i])
	}
	if norm <= 0 {
		return v
	}
	inv := 1.0 / float32sqrt(norm)
	for i := range v {
		v[i] *= inv
	}
	return v
}

func float32sqrt(x float64) float32 {
	// avoid pulling math just for one sqrt at this size
	z := x
	for i := 0; i < 16; i++ {
		z = 0.5 * (z + x/z)
	}
	return float32(z)
}

// GenerateStructAElementData mirrors the deterministic generator used by the python tests:
//   - num_elems = random.Random(i).randint(3, 8)  (python inclusive on both ends)
//   - int_val   = i*100 + j
//   - str_val   = f"row_{i}_elem_{j}"
//   - float_val = i + j*0.1
//   - color     = COLORS[j % 3]
//   - category  = CATEGORIES[(i+j) % 4]
//   - embedding = SeedVector(i*1000 + j)
func GenerateStructAElementData(nb int, startID int64, opt StructAElementSchemaOption) StructAElementDataset {
	if opt.Dim == 0 {
		opt.Dim = StructAElemDim
	}
	rows := make([]StructARow, 0, nb)
	for i := int64(0); i < int64(nb); i++ {
		id := startID + i
		// emulate python random.Random(id).randint(3, 8) using a small PRNG seeded by id
		r := rand.New(rand.NewSource(id))
		numElems := 3 + r.Intn(6) // 3..8 inclusive
		elems := make([]StructAElement, numElems)
		for j := 0; j < numElems; j++ {
			elems[j] = StructAElement{
				Embedding: SeedVector(id*1000+int64(j), opt.Dim),
				IntVal:    id*100 + int64(j),
				StrVal:    fmt.Sprintf("row_%d_elem_%d", id, j),
				FloatVal:  float32(id) + float32(j)*0.1,
				Color:     StructAElemColors[j%3],
				Category:  StructAElemCategories[(int(id)+j)%4],
				Size:      StructAElemSizes[(int(id)+j)%4],
			}
		}
		rows = append(rows, StructARow{
			ID:           id,
			DocInt:       id,
			DocVarChar:   fmt.Sprintf("cat_%d", id%10),
			NormalVector: SeedVector(id+999999, opt.Dim),
			StructA:      elems,
		})
	}
	return StructAElementDataset{Rows: rows, Opt: opt}
}

// ToInsertColumns returns the parallel column slices needed by WithStructArrayColumn etc.
//
//   - ids, normalVectors are always returned
//   - structRows is the row-keyed map[string]any payload to feed WithStructArrayColumn
//   - docInts / docVChars are returned (zero values if not in schema) — caller uses based on opt
func (d StructAElementDataset) ToInsertColumns() (ids []int64, normalVectors [][]float32, docInts []int64, docVChars []string, structRows []map[string]any) {
	ids = make([]int64, len(d.Rows))
	normalVectors = make([][]float32, len(d.Rows))
	docInts = make([]int64, len(d.Rows))
	docVChars = make([]string, len(d.Rows))
	structRows = make([]map[string]any, len(d.Rows))
	for i, r := range d.Rows {
		ids[i] = r.ID
		normalVectors[i] = r.NormalVector
		docInts[i] = r.DocInt
		docVChars[i] = r.DocVarChar
		structRows[i] = elementsToRow(r.StructA, d.Opt)
	}
	return
}

func elementsToRow(elements []StructAElement, opt StructAElementSchemaOption) map[string]any {
	embs := make([][]float32, len(elements))
	intVals := make([]int64, len(elements))
	strVals := make([]string, len(elements))
	floatVals := make([]float32, len(elements))
	colors := make([]string, len(elements))
	cats := make([]string, len(elements))
	sizes := make([]string, len(elements))
	for j, e := range elements {
		embs[j] = e.Embedding
		intVals[j] = e.IntVal
		strVals[j] = e.StrVal
		floatVals[j] = e.FloatVal
		colors[j] = e.Color
		cats[j] = e.Category
		sizes[j] = e.Size
	}
	row := map[string]any{
		"embedding": embs,
		"int_val":   intVals,
		"color":     colors,
	}
	if opt.IncludeStrVal {
		row["str_val"] = strVals
	}
	if opt.IncludeFloatVal {
		row["float_val"] = floatVals
	}
	if opt.IncludeCategory {
		row["category"] = cats
	}
	if opt.IncludeSize {
		row["size"] = sizes
	}
	return row
}

// MakeRow is a row builder used by Python `_make_row(row_id, struct_elements)` controlled-data
// tests. struct_elements only need to set fields the test cares about; missing fields default to
// safe values (color="Red", str_val=auto-generated, embedding=seeded).
func MakeRow(rowID int64, opt StructAElementSchemaOption, structElements []StructAElement) StructARow {
	if opt.Dim == 0 {
		opt.Dim = StructAElemDim
	}
	elems := make([]StructAElement, len(structElements))
	for j, e := range structElements {
		ej := e
		if len(ej.Embedding) == 0 {
			ej.Embedding = SeedVector(rowID*1000+int64(j), opt.Dim)
		}
		if ej.Color == "" {
			ej.Color = "Red"
		}
		if ej.StrVal == "" {
			ej.StrVal = fmt.Sprintf("r%d_e%d", rowID, j)
		}
		elems[j] = ej
	}
	return StructARow{
		ID:           rowID,
		DocInt:       rowID,
		DocVarChar:   fmt.Sprintf("cat_%d", rowID%10),
		NormalVector: SeedVector(rowID+999999, opt.Dim),
		StructA:      elems,
	}
}

// MakeInertRow creates a row that does NOT match common element_filter conditions. Used by the
// python correctness tests as background fill so element_filter results are unambiguous.
func MakeInertRow(rowID int64, opt StructAElementSchemaOption) StructARow {
	if opt.Dim == 0 {
		opt.Dim = StructAElemDim
	}
	return StructARow{
		ID:           rowID,
		DocInt:       9000000 + rowID,
		DocVarChar:   "inert",
		NormalVector: SeedVector(rowID+999999, opt.Dim),
		StructA: []StructAElement{{
			Embedding: SeedVector(rowID*1000, opt.Dim),
			IntVal:    0,
			StrVal:    fmt.Sprintf("inert_%d", rowID),
			Color:     "Inert",
			Category:  "Inert",
			FloatVal:  0,
		}},
	}
}

// RowsToColumns wraps ToInsertColumns for arbitrary StructARow slices that may have been built
// from MakeRow / MakeInertRow rather than the bulk generator.
func RowsToColumns(rows []StructARow, opt StructAElementSchemaOption) (ids []int64, normalVectors [][]float32, docInts []int64, docVChars []string, structRows []map[string]any) {
	d := StructAElementDataset{Rows: rows, Opt: opt}
	return d.ToInsertColumns()
}

// =============================================================================
// Ground truth helpers — port of gt_element_filter_query / gt_match_query / array_contains.
// =============================================================================

// GtElementFilter returns the set of row IDs for which at least one element in StructA satisfies
// elemFilterFn. If docFilterFn is non-nil it must also pass.
func GtElementFilter(data []StructARow, elemFilterFn func(StructAElement) bool, docFilterFn func(StructARow) bool) map[int64]struct{} {
	ids := make(map[int64]struct{})
	for _, row := range data {
		if docFilterFn != nil && !docFilterFn(row) {
			continue
		}
		for _, e := range row.StructA {
			if elemFilterFn(e) {
				ids[row.ID] = struct{}{}
				break
			}
		}
	}
	return ids
}

// GtMatch covers MATCH_ALL / MATCH_ANY / MATCH_LEAST / MATCH_MOST / MATCH_EXACT. threshold is
// only used by the LEAST/MOST/EXACT variants.
func GtMatch(data []StructARow, matchType string, elemFilterFn func(StructAElement) bool, threshold int, docFilterFn func(StructARow) bool) map[int64]struct{} {
	ids := make(map[int64]struct{})
	for _, row := range data {
		if docFilterFn != nil && !docFilterFn(row) {
			continue
		}
		count := 0
		for _, e := range row.StructA {
			if elemFilterFn(e) {
				count++
			}
		}
		total := len(row.StructA)
		var matched bool
		switch matchType {
		case "MATCH_ALL":
			matched = count == total
		case "MATCH_ANY":
			matched = count >= 1
		case "MATCH_LEAST":
			matched = count >= threshold
		case "MATCH_MOST":
			matched = count <= threshold
		case "MATCH_EXACT":
			matched = count == threshold
		}
		if matched {
			ids[row.ID] = struct{}{}
		}
	}
	return ids
}

// GtArrayContains returns IDs whose StructA has at least one element where extractor(elem) ==
// target.
func GtArrayContains[T comparable](data []StructARow, target T, extractor func(StructAElement) T) map[int64]struct{} {
	ids := make(map[int64]struct{})
	for _, row := range data {
		for _, e := range row.StructA {
			if extractor(e) == target {
				ids[row.ID] = struct{}{}
				break
			}
		}
	}
	return ids
}

// GtArrayContainsAll returns IDs whose StructA contains every value in `targets` (each via
// extractor on at least one element).
func GtArrayContainsAll[T comparable](data []StructARow, targets []T, extractor func(StructAElement) T) map[int64]struct{} {
	ids := make(map[int64]struct{})
	for _, row := range data {
		seen := make(map[T]bool, len(targets))
		for _, e := range row.StructA {
			seen[extractor(e)] = true
		}
		all := true
		for _, t := range targets {
			if !seen[t] {
				all = false
				break
			}
		}
		if all {
			ids[row.ID] = struct{}{}
		}
	}
	return ids
}

// GtArrayContainsAny returns IDs whose StructA contains any of the targets.
func GtArrayContainsAny[T comparable](data []StructARow, targets []T, extractor func(StructAElement) T) map[int64]struct{} {
	ids := make(map[int64]struct{})
	want := make(map[T]bool, len(targets))
	for _, t := range targets {
		want[t] = true
	}
	for _, row := range data {
		for _, e := range row.StructA {
			if want[extractor(e)] {
				ids[row.ID] = struct{}{}
				break
			}
		}
	}
	return ids
}

// L2Distance returns the squared L2 distance between two equal-length float32 vectors.
func L2Distance(a, b []float32) float64 {
	var s float64
	for i := range a {
		d := float64(a[i] - b[i])
		s += d * d
	}
	return s
}

// GtElementSearchNoFilter returns the top-K (rowID, bestScore) pairs for an element-level vector
// search with no filter. Each row contributes its best matching element's score (max for COSINE/IP,
// min for L2). Mirrors python `gt_element_search_no_filter`.
func GtElementSearchNoFilter(data []StructARow, queryVector []float32, metric string, limit int) []int64 {
	type rowScore struct {
		id    int64
		score float64
	}
	descending := metric == "COSINE" || metric == "IP"
	scores := make([]rowScore, 0, len(data))
	for _, row := range data {
		var best float64
		hasBest := false
		for _, e := range row.StructA {
			s := scoreFor(queryVector, e.Embedding, metric)
			if !hasBest || (descending && s > best) || (!descending && s < best) {
				best = s
				hasBest = true
			}
		}
		if hasBest {
			scores = append(scores, rowScore{row.ID, best})
		}
	}
	// stable sort by score
	for i := 1; i < len(scores); i++ {
		j := i
		for j > 0 {
			lhs := scores[j-1].score
			rhs := scores[j].score
			if (descending && lhs >= rhs) || (!descending && lhs <= rhs) {
				break
			}
			scores[j-1], scores[j] = scores[j], scores[j-1]
			j--
		}
	}
	if limit > len(scores) {
		limit = len(scores)
	}
	out := make([]int64, limit)
	for i := 0; i < limit; i++ {
		out[i] = scores[i].id
	}
	return out
}

func scoreFor(q, v []float32, metric string) float64 {
	switch metric {
	case "COSINE":
		return float64(CosineSimilarity(q, v))
	case "L2":
		return L2Distance(q, v)
	case "IP":
		var s float64
		for i := range q {
			s += float64(q[i]) * float64(v[i])
		}
		return s
	}
	return 0
}

// IDSetToSorted is a tiny utility to turn the ID maps into deterministic int64 slices for diff
// printing in failed assertions.
func IDSetToSorted(set map[int64]struct{}) []int64 {
	out := make([]int64, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	for i := 1; i < len(out); i++ {
		j := i
		for j > 0 && out[j-1] > out[j] {
			out[j-1], out[j] = out[j], out[j-1]
			j--
		}
	}
	return out
}
