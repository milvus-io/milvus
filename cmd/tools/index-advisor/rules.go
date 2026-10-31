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

// This file holds the rules of defaultRuleEngine, in the order it runs them.

import (
	"fmt"
	"sort"
	"strings"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/pkg/v3/common"
)

// primaryKeyRule stops at the primary key, whose ==, IN and range filters
// segcore serves with its built-in primary-key lookup.
type primaryKeyRule struct{}

func (primaryKeyRule) apply(fc *fieldContext, adv *advice) verdict {
	if !fc.col.isPrimaryKey {
		return proceed
	}
	adv.note("Primary key: Milvus serves ==, IN and range filters on it through its built-in primary-key lookup, and this tool proposes no index for it.")
	return stop
}

// unfilteredColumnRule stops at an indexed column that no analyzed request
// filters on. Such a column is in the report only because of its index.
type unfilteredColumnRule struct{}

func (unfilteredColumnRule) apply(fc *fieldContext, adv *advice) verdict {
	if fc.workload.requests > 0 {
		return proceed
	}
	adv.note("No analyzed request filters on this column, so these logs show no use of its index.")
	return stop
}

// jsonCastTypeRule stops at a JSON path whose json_cast_type the logged
// literals do not determine, since every index on a path needs one.
type jsonCastTypeRule struct{}

func (jsonCastTypeRule) apply(fc *fieldContext, adv *advice) verdict {
	if fc.kind != kindJSONPath || fc.castType != "" {
		return proceed
	}
	adv.note("The json_cast_type cannot be inferred: the literals compared with this path have several types, or there are none (template variables or exists).")
	return stop
}

// indexableOperatorRule stops when no observed operator on the column is one
// a scalar index serves.
type indexableOperatorRule struct{}

func (indexableOperatorRule) apply(fc *fieldContext, adv *advice) verdict {
	if len(fc.workload.servedOps()) > 0 {
		return proceed
	}
	var reasons []string
	for op := range fc.workload.opRequests {
		reasons = append(reasons, unservedOps[op])
	}
	sort.Strings(reasons)
	adv.note("No scalar index serves these predicates: " + strings.Join(reasons, "; ") + ".")
	return stop
}

// baselineAutoIndexRule proposes AUTOINDEX, which lets Milvus choose the index
// type, for every column a scalar index can serve, and notes the columns on
// which Milvus does not accept it.
type baselineAutoIndexRule struct{}

func (baselineAutoIndexRule) apply(fc *fieldContext, adv *advice) verdict {
	resolved := autoIndexResolution(fc.col, fc.castType)
	if resolved == "" {
		if fc.castType != "" {
			adv.note(fmt.Sprintf("Milvus does not accept AUTOINDEX on a JSON path with json_cast_type %s.", fc.castType))
		} else {
			adv.note(fmt.Sprintf("This tool proposes no index for %s columns.", fc.col.typeName()))
		}
		return proceed
	}
	builds := fmt.Sprintf("AUTOINDEX lets Milvus pick the index type; with the default scalarAutoIndex.params.build it builds %s for %s", resolved, fc.col.typeName())
	if fc.kind == kindJSONPath && jsonPathVersionGated(resolved) {
		builds += fmt.Sprintf(", or INVERTED on a cluster whose scalar index engine version is below %d", common.MinScalarIndexVersionForJsonPathMultiType)
	}
	c := newCandidate(fc, indexAuto, roleBaseline, fc.workload.servedOps(), []string{
		fmt.Sprintf("this column is filtered by %s", plural(fc.workload.requests, "analyzed request")),
		builds,
	}, docScalarIndex)
	c.resolvesTo = resolved
	adv.add(c)
	return proceed
}

// alternative is what an evidence rule proposes: an index type, the operators
// and column kinds it is meant for, and the documentation it relies on.
type alternative struct {
	indexType string
	ops       []opClass
	kinds     []columnKind
	source    string
}

// evidenceRule is a rule that proposes an alternative to AUTOINDEX only when
// the access log or --field-stats gives evidence for it.
type evidenceRule interface {
	rule
	alternative() alternative
}

// serves returns the observed operators on fc the alternative is meant for;
// nil when the column kind or Milvus's type checks rule it out, or when the
// column already has such an index.
func (a alternative) serves(fc *fieldContext) []opClass {
	if !lo.Contains(a.kinds, fc.kind) || !indexAccepts(a.indexType, fc.col, fc.castType) {
		return nil
	}
	proposed := candidate{IndexType: a.indexType}
	if fc.kind == kindJSONPath {
		proposed.setParam(common.JSONCastTypeKey, fc.castType)
	}
	if lo.ContainsBy(fc.existing, func(idx indexSnapshot) bool { return sameIndex(idx, proposed) }) {
		return nil
	}
	return lo.Filter(a.ops, func(op opClass, _ int) bool { return fc.workload.opRequests[op] > 0 })
}

// bitmapRule proposes BITMAP for equality, IN and contains filters when
// --field-stats shows fewer than bitmapMaxDistinct distinct values.
type bitmapRule struct{}

func (bitmapRule) alternative() alternative {
	return alternative{
		indexType: indexBitmap,
		ops:       []opClass{opEq, opIn, opContains},
		kinds:     []columnKind{kindInt, kindString, kindArray},
		source:    docBitmap,
	}
}

func (r bitmapRule) apply(fc *fieldContext, adv *advice) verdict {
	alt := r.alternative()
	serves := alt.serves(fc)
	distinct := fc.stats.distinctCount
	switch {
	case len(serves) == 0:
		return proceed
	case lo.ContainsBy(adv.candidates, func(c candidate) bool { return c.resolvesTo == alt.indexType }):
		return proceed // AUTOINDEX already builds BITMAP, as for an array of BOOL
	case distinct == nil:
		adv.note(fmt.Sprintf("BITMAP was not evaluated: it suits fewer than %d distinct values and distinct_count is unknown; provide it with --field-stats.", bitmapMaxDistinct))
		return proceed
	case *distinct >= bitmapMaxDistinct:
		return proceed
	}
	adv.add(newCandidate(fc, alt.indexType, roleAlternative, serves, []string{
		fmt.Sprintf("distinct_count %d is below %d, where BITMAP is documented to be most effective", *distinct, bitmapMaxDistinct),
		fmt.Sprintf("compare with AUTOINDEX: its HYBRID index already switches to a bitmap below bitmap_cardinality_limit (default %d)", hybridBitmapLimit),
	}, alt.source))
	return proceed
}

// ngramRule proposes NGRAM for infix, suffix and pattern LIKE filters when
// every fixed part of every observed LIKE filter is at least min_gram long, so
// all of them can use the index.
type ngramRule struct{}

func (ngramRule) alternative() alternative {
	return alternative{
		indexType: indexNgram,
		ops:       []opClass{opLikeInfix, opLikeSuffix, opLikePattern},
		kinds:     []columnKind{kindString, kindJSONPath},
		source:    docNgram,
	}
}

func (r ngramRule) apply(fc *fieldContext, adv *advice) verdict {
	alt := r.alternative()
	serves := alt.serves(fc)
	w := fc.workload
	switch {
	case len(serves) == 0:
		return proceed
	case w.unknownLiteral || w.minLiteral < 0:
		adv.note("NGRAM was not evaluated: some LIKE literal lengths are unknown (template variables or regex).")
		return proceed
	case w.minLiteral < ngramMinGram:
		adv.note(fmt.Sprintf("NGRAM was not evaluated: a LIKE filter has a fixed part of %d characters, below min_gram %d, so that filter could not use the index.", w.minLiteral, ngramMinGram))
		return proceed
	}
	c := newCandidate(fc, alt.indexType, roleAlternative, serves, []string{
		fmt.Sprintf("infix, suffix or pattern LIKE, which NGRAM is documented for, is used by %s", plural(w.requestsServing(serves), "request")),
		fmt.Sprintf("every fixed part of the observed LIKE filters has at least %d characters (shortest: %d)", ngramMinGram, w.minLiteral),
	}, alt.source)
	c.setParam("min_gram", fmt.Sprint(ngramMinGram))
	c.setParam("max_gram", fmt.Sprint(ngramMaxGram))
	adv.add(c)
	return proceed
}

// jsonArrayRule proposes INVERTED with an array json_cast_type for json_contains
// filters on a JSON path that holds an array. A JSON path index serves such a
// filter only with an array cast, and INVERTED is the only index type that
// accepts one.
type jsonArrayRule struct{}

func (jsonArrayRule) alternative() alternative {
	return alternative{
		indexType: indexInverted,
		ops:       []opClass{opContains},
		kinds:     []columnKind{kindJSONPath},
		source:    docJSONIndex,
	}
}

func (r jsonArrayRule) apply(fc *fieldContext, adv *advice) verdict {
	alt := r.alternative()
	serves := alt.serves(fc)
	if len(serves) == 0 || !isArrayCast(fc.castType) {
		return proceed
	}
	adv.add(newCandidate(fc, alt.indexType, roleAlternative, serves, []string{
		fmt.Sprintf("json_contains on this JSON array is used by %s; a JSON path index serves it only with an array json_cast_type", plural(fc.workload.requestsServing(serves), "request")),
		"INVERTED is the index type Milvus accepts with an array json_cast_type; AUTOINDEX, HYBRID and BITMAP do not",
	}, alt.source))
	return proceed
}

// jsonShreddingRule notes, for a JSON path with candidates or existing indexes,
// that JSON shredding takes precedence: on segments with JSON statistics,
// Milvus filters the path through them before it considers a JSON path index,
// unless the path holds an array position (CanUseJsonStatsAtInit in
// internal/core/src/exec/expression/Expr.h).
type jsonShreddingRule struct{}

func (jsonShreddingRule) apply(fc *fieldContext, adv *advice) verdict {
	if fc.kind == kindJSONPath && !lo.ContainsBy(fc.col.jsonPath, isArrayPosition) && (len(adv.candidates) > 0 || len(fc.existing) > 0) {
		adv.note("Where JSON shredding is enabled (common.enabledJSONShredding and common.usingJSONShreddingForQuery), Milvus filters this path through JSON statistics on segments that have them, before it considers a JSON path index; measure with the deployment's settings.")
	}
	return proceed
}

// existingIndexRule drops each candidate of the same type as an existing index
// on the column, since the review measures that index instead. AUTOINDEX is
// also dropped when it would build that type.
type existingIndexRule struct{}

func (existingIndexRule) apply(fc *fieldContext, adv *advice) verdict {
	adv.candidates = lo.Filter(adv.candidates, func(c candidate, _ int) bool {
		return !lo.ContainsBy(fc.existing, func(idx indexSnapshot) bool { return sameIndex(idx, c) })
	})
	return proceed
}

// sameIndex reports whether idx is of the type c proposes or AUTOINDEX would
// build. On a JSON path, an index with another json_cast_type serves other
// filters, so it differs. An index whose cast the snapshot omits is taken as
// the same.
func sameIndex(idx indexSnapshot, c candidate) bool {
	cast, known := idx.Params[common.JSONCastTypeKey].(string)
	if want := c.Params[common.JSONCastTypeKey]; known && want != "" && !strings.EqualFold(strings.TrimSpace(cast), want) {
		return false
	}
	return strings.EqualFold(idx.IndexType, c.IndexType) || (c.resolvesTo != "" && strings.EqualFold(idx.IndexType, c.resolvesTo))
}

// newCandidate builds a candidate on fc's column. A JSON path gets its
// json_cast_type and json_path. A dynamic-field path is indexed under its
// top-level key, which Milvus resolves to the dynamic field; its json_path is
// written as $meta[...], which parses for any key, nested or not.
func newCandidate(fc *fieldContext, indexType, role string, serves []opClass, evidence []string, source string) candidate {
	c := candidate{
		IndexType:  indexType,
		Role:       role,
		FieldName:  fc.col.field,
		Serves:     lo.Map(serves, func(op opClass, _ int) string { return string(op) }),
		Predicates: fc.workload.requestsServing(serves),
		Evidence:   evidence,
		Source:     source,
	}
	if fc.kind == kindJSONPath {
		c.setParam(common.JSONCastTypeKey, fc.castType)
		c.setParam(common.JSONPathKey, fc.col.String())
		if fc.col.isDynamic {
			c.FieldName = fc.col.jsonPath[0]
		}
	}
	return c
}
