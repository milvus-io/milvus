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
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
)

var (
	validAggregationMetricOps = map[string]struct{}{
		"avg":   {},
		"sum":   {},
		"count": {},
		"min":   {},
		"max":   {},
	}
	validAggregationDirections = map[string]struct{}{
		"asc":  {},
		"desc": {},
	}
	specialAggregationOrderKeys = map[string]struct{}{
		"_count": {},
		"_key":   {},
	}
)

// SearchAggregation describes one level of bucket aggregation for Search.
type SearchAggregation struct {
	fields         []string
	size           int64
	searchSize     int64
	metrics        map[string]aggregationMetric
	order          []aggregationOrder
	topHits        *TopHits
	subAggregation *SearchAggregation
}

type aggregationMetric struct {
	op        string
	fieldName string
}

type aggregationOrder struct {
	key       string
	direction string
}

type aggregationSort struct {
	fieldName string
	direction string
}

// TopHits describes representative hits returned inside each aggregation bucket.
type TopHits struct {
	size int64
	sort []aggregationSort
}

// NewSearchAggregation creates one search aggregation level.
func NewSearchAggregation(fields []string, size int) *SearchAggregation {
	return &SearchAggregation{
		fields:  append([]string(nil), fields...),
		size:    int64(size),
		metrics: make(map[string]aggregationMetric),
	}
}

// WithSearchSize sets the candidate bucket budget for this aggregation level.
func (a *SearchAggregation) WithSearchSize(searchSize int) *SearchAggregation {
	a.searchSize = int64(searchSize)
	return a
}

// WithMetric adds an aggregation metric.
func (a *SearchAggregation) WithMetric(alias, op, fieldName string) *SearchAggregation {
	if a.metrics == nil {
		a.metrics = make(map[string]aggregationMetric)
	}
	a.metrics[alias] = aggregationMetric{op: op, fieldName: fieldName}
	return a
}

// WithOrder appends a bucket ordering criterion.
func (a *SearchAggregation) WithOrder(key, direction string) *SearchAggregation {
	a.order = append(a.order, aggregationOrder{key: key, direction: direction})
	return a
}

// WithTopHits sets the top hits spec for this aggregation level.
func (a *SearchAggregation) WithTopHits(topHits *TopHits) *SearchAggregation {
	a.topHits = topHits
	return a
}

// WithSubAggregation sets the nested child aggregation level.
func (a *SearchAggregation) WithSubAggregation(sub *SearchAggregation) *SearchAggregation {
	a.subAggregation = sub
	return a
}

// NewTopHits creates a top hits spec.
func NewTopHits(size int) *TopHits {
	return &TopHits{size: int64(size)}
}

// WithSort appends a top hits sorting criterion.
func (h *TopHits) WithSort(fieldName, direction string) *TopHits {
	h.sort = append(h.sort, aggregationSort{fieldName: fieldName, direction: direction})
	return h
}

func (a *SearchAggregation) Validate() error {
	_, err := a.protoMessage()
	return err
}

func (h *TopHits) Validate() error {
	_, err := h.protoMessage()
	return err
}

func (a *SearchAggregation) protoMessage() (*commonpb.SearchAggregationSpec, error) {
	if a == nil {
		return nil, errors.New("search_aggregation cannot be nil")
	}
	if len(a.fields) == 0 {
		return nil, errors.New("SearchAggregation.fields must be non-empty")
	}
	fields := make([]string, 0, len(a.fields))
	for _, field := range a.fields {
		field = strings.TrimSpace(field)
		if field == "" {
			return nil, errors.New("SearchAggregation.fields must contain non-empty field names")
		}
		if isAggregationJSONPath(field) {
			return nil, fmt.Errorf("SearchAggregation.fields does not yet support bracketed JSON path expressions: %q", field)
		}
		fields = append(fields, field)
	}
	if a.size <= 0 {
		return nil, errors.New("SearchAggregation.size must be positive")
	}
	if a.searchSize < 0 {
		return nil, errors.New("SearchAggregation.search_size must be non-negative")
	}
	if a.searchSize > 0 && a.searchSize < a.size {
		return nil, errors.New("SearchAggregation.search_size must be greater than or equal to size")
	}

	spec := &commonpb.SearchAggregationSpec{
		Fields:     fields,
		Size:       a.size,
		SearchSize: a.searchSize,
	}

	metricAliases := make(map[string]struct{}, len(a.metrics))
	if len(a.metrics) > 0 {
		spec.Metrics = make(map[string]*commonpb.MetricAggSpec, len(a.metrics))
	}
	for alias, metric := range a.metrics {
		alias = strings.TrimSpace(alias)
		if alias == "" {
			return nil, errors.New("metric alias must be non-empty")
		}
		op := strings.ToLower(strings.TrimSpace(metric.op))
		if _, ok := validAggregationMetricOps[op]; !ok {
			return nil, fmt.Errorf("unsupported metric op %q", metric.op)
		}
		fieldName := strings.TrimSpace(metric.fieldName)
		if fieldName == "" {
			return nil, fmt.Errorf("metric %q field name must be non-empty", alias)
		}
		if isAggregationJSONPath(fieldName) {
			return nil, fmt.Errorf("metric %q field does not yet support bracketed JSON path expressions: %q", alias, fieldName)
		}
		if op != "count" && fieldName == "*" {
			return nil, fmt.Errorf("metric %q field_name '*' is only valid for count op", alias)
		}
		spec.Metrics[alias] = &commonpb.MetricAggSpec{Op: op, FieldName: fieldName}
		metricAliases[alias] = struct{}{}
	}

	for _, order := range a.order {
		key := strings.TrimSpace(order.key)
		if key == "" {
			return nil, errors.New("SearchAggregation.order key must be non-empty")
		}
		if _, ok := metricAliases[key]; !ok {
			if _, ok := specialAggregationOrderKeys[key]; !ok {
				return nil, fmt.Errorf("SearchAggregation.order key %q must be a metric alias or one of [_count _key]", key)
			}
		}
		direction, err := normalizeAggregationDirection(order.direction)
		if err != nil {
			return nil, fmt.Errorf("invalid SearchAggregation.order direction for %q: %w", key, err)
		}
		spec.Order = append(spec.Order, &commonpb.OrderSpec{Key: key, Direction: direction})
	}

	if a.topHits != nil {
		topHits, err := a.topHits.protoMessage()
		if err != nil {
			return nil, err
		}
		spec.TopHits = topHits
	}

	if a.subAggregation != nil {
		sub, err := a.subAggregation.protoMessage()
		if err != nil {
			return nil, err
		}
		spec.SubAggregation = sub
	}

	return spec, nil
}

func (h *TopHits) protoMessage() (*commonpb.TopHitsSpec, error) {
	if h == nil {
		return nil, errors.New("top_hits cannot be nil")
	}
	if h.size <= 0 {
		return nil, errors.New("TopHits.size must be positive")
	}

	spec := &commonpb.TopHitsSpec{Size: h.size}
	for _, sort := range h.sort {
		fieldName := strings.TrimSpace(sort.fieldName)
		if fieldName == "" {
			return nil, errors.New("TopHits.sort field name must be non-empty")
		}
		if isAggregationJSONPath(fieldName) {
			return nil, fmt.Errorf("TopHits.sort does not yet support bracketed JSON path expressions: %q", fieldName)
		}
		direction, err := normalizeAggregationDirection(sort.direction)
		if err != nil {
			return nil, fmt.Errorf("invalid TopHits.sort direction for %q: %w", fieldName, err)
		}
		spec.Sort = append(spec.Sort, &commonpb.SortSpec{FieldName: fieldName, Direction: direction})
	}
	return spec, nil
}

func normalizeAggregationDirection(direction string) (string, error) {
	direction = strings.ToLower(strings.TrimSpace(direction))
	if direction == "" {
		return "", errors.New("direction must be non-empty")
	}
	if _, ok := validAggregationDirections[direction]; !ok {
		return "", fmt.Errorf("direction must be asc or desc, got %q", direction)
	}
	return direction, nil
}

func isAggregationJSONPath(fieldName string) bool {
	return strings.Contains(fieldName, "[") || strings.Contains(fieldName, "]")
}
