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

package metrics

import (
	"maps"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	CollectionLevelMetricsModeFull      = "full"
	CollectionLevelMetricsModeAggregate = "aggregate"
)

var collectionLevelMetricsAggregate atomic.Bool

// SetCollectionLevelMetricsMode configures how metrics carrying collection or
// explicitly identified VChannel labels are recorded. The setting is
// process-wide and is expected to be initialized once, before components start
// writing metrics.
func SetCollectionLevelMetricsMode(mode string) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case CollectionLevelMetricsModeFull:
		collectionLevelMetricsAggregate.Store(false)
	case CollectionLevelMetricsModeAggregate:
		collectionLevelMetricsAggregate.Store(true)
	default:
		panic(merr.WrapErrParameterInvalidMsg(
			"common.metrics.collectionLevelMode must be %q or %q, got %q",
			CollectionLevelMetricsModeFull,
			CollectionLevelMetricsModeAggregate,
			mode,
		))
	}
}

// CollectionLevelMetricsMode returns the active process-wide mode.
func CollectionLevelMetricsMode() string {
	if collectionLevelMetricsAggregate.Load() {
		return CollectionLevelMetricsModeAggregate
	}
	return CollectionLevelMetricsModeFull
}

// IsCollectionLevelMetricsAggregateMode reports whether collection and
// explicitly identified VChannel label values must be collapsed to AllLabel.
func IsCollectionLevelMetricsAggregateMode() bool {
	return collectionLevelMetricsAggregate.Load()
}

type cardinalityMetricLabels struct {
	indexes             []int
	labelNames          map[string]struct{}
	aggregateLabelNames map[string]struct{}
}

func newCollectionMetricLabels(labelNames []string) cardinalityMetricLabels {
	return newCollectionMetricLabelsWithAdditional(labelNames)
}

// newVChannelMetricLabels is intentionally opt-in. channel_name is also used
// by PChannel metrics, so callers must first verify the writer supplies a
// VChannel.
func newVChannelMetricLabels(labelNames []string) cardinalityMetricLabels {
	return newCardinalityMetricLabels(labelNames, []string{channelNameLabelName})
}

func newCollectionVChannelMetricLabels(labelNames []string) cardinalityMetricLabels {
	return newCollectionMetricLabelsWithAdditional(labelNames, channelNameLabelName)
}

func newCollectionMetricLabelsWithAdditional(labelNames []string, additional ...string) cardinalityMetricLabels {
	aggregateLabelNames := slices.Clone(additional)
	for _, labelName := range labelNames {
		if labelName == collectionIDLabelName || labelName == collectionName {
			aggregateLabelNames = append(aggregateLabelNames, labelName)
		}
	}
	if len(aggregateLabelNames) == len(additional) {
		panic(merr.WrapErrServiceInternalMsg(
			"collection metric must contain %q or %q", collectionIDLabelName, collectionName))
	}
	return newCardinalityMetricLabels(labelNames, aggregateLabelNames)
}

func newCardinalityMetricLabels(labelNames, aggregateLabelNames []string) cardinalityMetricLabels {
	labels := cardinalityMetricLabels{
		labelNames:          make(map[string]struct{}, len(labelNames)),
		aggregateLabelNames: make(map[string]struct{}, len(aggregateLabelNames)),
	}
	for _, labelName := range aggregateLabelNames {
		labels.aggregateLabelNames[labelName] = struct{}{}
	}
	for index, labelName := range labelNames {
		labels.labelNames[labelName] = struct{}{}
		if _, ok := labels.aggregateLabelNames[labelName]; ok {
			labels.indexes = append(labels.indexes, index)
		}
	}
	if len(labels.indexes) != len(labels.aggregateLabelNames) {
		panic(merr.WrapErrServiceInternalMsg(
			"cardinality-controlled metric labels %v are not all present in %v",
			aggregateLabelNames,
			labelNames,
		))
	}
	return labels
}

func (l cardinalityMetricLabels) normalizeValues(values []string) []string {
	if !IsCollectionLevelMetricsAggregateMode() {
		return values
	}
	normalized := slices.Clone(values)
	for _, index := range l.indexes {
		if index < len(normalized) {
			normalized[index] = AllLabel
		}
	}
	return normalized
}

func (l cardinalityMetricLabels) normalizeLabels(labels prometheus.Labels) prometheus.Labels {
	if !IsCollectionLevelMetricsAggregateMode() {
		return labels
	}
	normalized := maps.Clone(labels)
	for labelName := range l.aggregateLabelNames {
		if _, ok := normalized[labelName]; ok {
			normalized[labelName] = AllLabel
		}
	}
	return normalized
}

func (l cardinalityMetricLabels) validateValues(values []string) error {
	if len(values) != len(l.labelNames) {
		return merr.WrapErrServiceInternalMsg(
			"inconsistent cardinality-controlled metric label count: expected %d values, got %d",
			len(l.labelNames), len(values))
	}
	return nil
}

func (l cardinalityMetricLabels) validateLabels(labels prometheus.Labels) error {
	if len(labels) != len(l.labelNames) {
		return merr.WrapErrServiceInternalMsg(
			"inconsistent cardinality-controlled metric label count: expected %d labels, got %d",
			len(l.labelNames), len(labels))
	}
	for labelName := range l.labelNames {
		if _, ok := labels[labelName]; !ok {
			return merr.WrapErrServiceInternalMsg(
				"cardinality-controlled metric label %q is missing", labelName)
		}
	}
	return nil
}

func (l cardinalityMetricLabels) scopedDelete(labels prometheus.Labels) bool {
	if !IsCollectionLevelMetricsAggregateMode() {
		return false
	}
	for labelName := range l.aggregateLabelNames {
		if _, ok := labels[labelName]; ok {
			return true
		}
	}
	return false
}

// CardinalityCounterVec collapses configured collection/VChannel labels before
// a series is created. Scoped deletes become no-ops in aggregate mode because
// the series is shared by every original label value with the same remaining
// labels.
type CardinalityCounterVec struct {
	vec    *prometheus.CounterVec
	labels cardinalityMetricLabels
}

func newCollectionCounterVec(opts prometheus.CounterOpts, labelNames []string) *CardinalityCounterVec {
	return newCounterVecWithCardinalityLabels(opts, labelNames, newCollectionMetricLabels(labelNames))
}

func newCounterVecWithCardinalityLabels(
	opts prometheus.CounterOpts,
	labelNames []string,
	labels cardinalityMetricLabels,
) *CardinalityCounterVec {
	return &CardinalityCounterVec{
		vec:    prometheus.NewCounterVec(opts, labelNames),
		labels: labels,
	}
}

func (v *CardinalityCounterVec) Describe(ch chan<- *prometheus.Desc) {
	v.vec.Describe(ch)
}

func (v *CardinalityCounterVec) Collect(ch chan<- prometheus.Metric) {
	v.vec.Collect(ch)
}

func (v *CardinalityCounterVec) Reset() {
	v.vec.Reset()
}

func (v *CardinalityCounterVec) GetMetricWithLabelValues(values ...string) (prometheus.Counter, error) {
	return v.vec.GetMetricWithLabelValues(v.labels.normalizeValues(values)...)
}

func (v *CardinalityCounterVec) WithLabelValues(values ...string) prometheus.Counter {
	metric, err := v.GetMetricWithLabelValues(values...)
	if err != nil {
		panic(err)
	}
	return metric
}

func (v *CardinalityCounterVec) GetMetricWith(labels prometheus.Labels) (prometheus.Counter, error) {
	return v.vec.GetMetricWith(v.labels.normalizeLabels(labels))
}

func (v *CardinalityCounterVec) With(labels prometheus.Labels) prometheus.Counter {
	metric, err := v.GetMetricWith(labels)
	if err != nil {
		panic(err)
	}
	return metric
}

func (v *CardinalityCounterVec) DeleteLabelValues(values ...string) bool {
	if IsCollectionLevelMetricsAggregateMode() {
		return false
	}
	return v.vec.DeleteLabelValues(values...)
}

func (v *CardinalityCounterVec) Delete(labels prometheus.Labels) bool {
	if v.labels.scopedDelete(labels) {
		return false
	}
	return v.vec.Delete(labels)
}

func (v *CardinalityCounterVec) DeletePartialMatch(labels prometheus.Labels) int {
	if v.labels.scopedDelete(labels) {
		return 0
	}
	return v.vec.DeletePartialMatch(labels)
}

// CardinalityHistogramVec is the Histogram counterpart of CardinalityCounterVec.
type CardinalityHistogramVec struct {
	vec    *prometheus.HistogramVec
	labels cardinalityMetricLabels
}

func newCollectionHistogramVec(opts prometheus.HistogramOpts, labelNames []string) *CardinalityHistogramVec {
	return newHistogramVecWithCardinalityLabels(opts, labelNames, newCollectionMetricLabels(labelNames))
}

func newVChannelHistogramVec(opts prometheus.HistogramOpts, labelNames []string) *CardinalityHistogramVec {
	return newHistogramVecWithCardinalityLabels(opts, labelNames, newVChannelMetricLabels(labelNames))
}

func newCollectionVChannelHistogramVec(opts prometheus.HistogramOpts, labelNames []string) *CardinalityHistogramVec {
	return newHistogramVecWithCardinalityLabels(opts, labelNames, newCollectionVChannelMetricLabels(labelNames))
}

func newHistogramVecWithCardinalityLabels(
	opts prometheus.HistogramOpts,
	labelNames []string,
	labels cardinalityMetricLabels,
) *CardinalityHistogramVec {
	return &CardinalityHistogramVec{
		vec:    prometheus.NewHistogramVec(opts, labelNames),
		labels: labels,
	}
}

func (v *CardinalityHistogramVec) Describe(ch chan<- *prometheus.Desc) {
	v.vec.Describe(ch)
}

func (v *CardinalityHistogramVec) Collect(ch chan<- prometheus.Metric) {
	v.vec.Collect(ch)
}

func (v *CardinalityHistogramVec) Reset() {
	v.vec.Reset()
}

func (v *CardinalityHistogramVec) GetMetricWithLabelValues(values ...string) (prometheus.Observer, error) {
	return v.vec.GetMetricWithLabelValues(v.labels.normalizeValues(values)...)
}

func (v *CardinalityHistogramVec) WithLabelValues(values ...string) prometheus.Observer {
	metric, err := v.GetMetricWithLabelValues(values...)
	if err != nil {
		panic(err)
	}
	return metric
}

func (v *CardinalityHistogramVec) GetMetricWith(labels prometheus.Labels) (prometheus.Observer, error) {
	return v.vec.GetMetricWith(v.labels.normalizeLabels(labels))
}

func (v *CardinalityHistogramVec) With(labels prometheus.Labels) prometheus.Observer {
	metric, err := v.GetMetricWith(labels)
	if err != nil {
		panic(err)
	}
	return metric
}

func (v *CardinalityHistogramVec) DeleteLabelValues(values ...string) bool {
	if IsCollectionLevelMetricsAggregateMode() {
		return false
	}
	return v.vec.DeleteLabelValues(values...)
}

func (v *CardinalityHistogramVec) Delete(labels prometheus.Labels) bool {
	if v.labels.scopedDelete(labels) {
		return false
	}
	return v.vec.Delete(labels)
}

func (v *CardinalityHistogramVec) DeletePartialMatch(labels prometheus.Labels) int {
	if v.labels.scopedDelete(labels) {
		return 0
	}
	return v.vec.DeletePartialMatch(labels)
}

type collectionGaugeAggregatePolicy int

const (
	// collectionGaugeAggregateSum is used by gauges whose writers either use
	// Add/Sub or pre-aggregate all collection/VChannel values before calling Set.
	collectionGaugeAggregateSum collectionGaugeAggregatePolicy = iota
	// collectionGaugeAggregateDisabled suppresses a gauge in aggregate mode
	// when no truthful scope-independent aggregation is available.
	collectionGaugeAggregateDisabled
)

type noOpGauge struct {
	prometheus.Gauge
}

func (noOpGauge) Set(float64)       {}
func (noOpGauge) Inc()              {}
func (noOpGauge) Dec()              {}
func (noOpGauge) Add(float64)       {}
func (noOpGauge) Sub(float64)       {}
func (noOpGauge) SetToCurrentTime() {}

var disabledCollectionGauge prometheus.Gauge = noOpGauge{
	Gauge: prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "disabled_collection_metric",
		Help: "internal no-op gauge for disabled collection metrics",
	}),
}

// CardinalityGaugeVec either collapses collection/VChannel labels or suppresses
// writes according to the metric family's explicitly selected aggregate policy.
type CardinalityGaugeVec struct {
	vec    *prometheus.GaugeVec
	labels cardinalityMetricLabels
	policy collectionGaugeAggregatePolicy
}

func newCollectionGaugeVec(
	opts prometheus.GaugeOpts,
	labelNames []string,
	policy collectionGaugeAggregatePolicy,
) *CardinalityGaugeVec {
	return newGaugeVecWithCardinalityLabels(opts, labelNames, policy, newCollectionMetricLabels(labelNames))
}

func newVChannelGaugeVec(
	opts prometheus.GaugeOpts,
	labelNames []string,
	policy collectionGaugeAggregatePolicy,
) *CardinalityGaugeVec {
	return newGaugeVecWithCardinalityLabels(opts, labelNames, policy, newVChannelMetricLabels(labelNames))
}

func newCollectionVChannelGaugeVec(
	opts prometheus.GaugeOpts,
	labelNames []string,
	policy collectionGaugeAggregatePolicy,
) *CardinalityGaugeVec {
	return newGaugeVecWithCardinalityLabels(opts, labelNames, policy, newCollectionVChannelMetricLabels(labelNames))
}

func newGaugeVecWithCardinalityLabels(
	opts prometheus.GaugeOpts,
	labelNames []string,
	policy collectionGaugeAggregatePolicy,
	labels cardinalityMetricLabels,
) *CardinalityGaugeVec {
	return &CardinalityGaugeVec{
		vec:    prometheus.NewGaugeVec(opts, labelNames),
		labels: labels,
		policy: policy,
	}
}

func (v *CardinalityGaugeVec) Describe(ch chan<- *prometheus.Desc) {
	v.vec.Describe(ch)
}

func (v *CardinalityGaugeVec) Collect(ch chan<- prometheus.Metric) {
	v.vec.Collect(ch)
}

func (v *CardinalityGaugeVec) Reset() {
	v.vec.Reset()
}

func (v *CardinalityGaugeVec) aggregateDisabled() bool {
	return IsCollectionLevelMetricsAggregateMode() && v.policy == collectionGaugeAggregateDisabled
}

func (v *CardinalityGaugeVec) GetMetricWithLabelValues(values ...string) (prometheus.Gauge, error) {
	if v.aggregateDisabled() {
		if err := v.labels.validateValues(values); err != nil {
			return nil, err
		}
		return disabledCollectionGauge, nil
	}
	return v.vec.GetMetricWithLabelValues(v.labels.normalizeValues(values)...)
}

func (v *CardinalityGaugeVec) WithLabelValues(values ...string) prometheus.Gauge {
	metric, err := v.GetMetricWithLabelValues(values...)
	if err != nil {
		panic(err)
	}
	return metric
}

func (v *CardinalityGaugeVec) GetMetricWith(labels prometheus.Labels) (prometheus.Gauge, error) {
	if v.aggregateDisabled() {
		if err := v.labels.validateLabels(labels); err != nil {
			return nil, err
		}
		return disabledCollectionGauge, nil
	}
	return v.vec.GetMetricWith(v.labels.normalizeLabels(labels))
}

func (v *CardinalityGaugeVec) With(labels prometheus.Labels) prometheus.Gauge {
	metric, err := v.GetMetricWith(labels)
	if err != nil {
		panic(err)
	}
	return metric
}

func (v *CardinalityGaugeVec) DeleteLabelValues(values ...string) bool {
	if IsCollectionLevelMetricsAggregateMode() {
		return false
	}
	return v.vec.DeleteLabelValues(values...)
}

func (v *CardinalityGaugeVec) Delete(labels prometheus.Labels) bool {
	if v.labels.scopedDelete(labels) {
		return false
	}
	return v.vec.Delete(labels)
}

func (v *CardinalityGaugeVec) DeletePartialMatch(labels prometheus.Labels) int {
	if v.labels.scopedDelete(labels) {
		return 0
	}
	return v.vec.DeletePartialMatch(labels)
}
