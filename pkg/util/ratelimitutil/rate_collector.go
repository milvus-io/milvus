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

package ratelimitutil

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/samber/lo"
)

const (
	DefaultWindow      = 10 * time.Second
	DefaultGranularity = 1 * time.Second
	DefaultAvgDuration = 3 * time.Second
)

// RateCollector helps to collect and calculate values (like throughput, QPS, TPS, etc...),
// It implements a sliding window with custom size and granularity to store values.
type RateCollector struct {
	sync.Mutex

	window              time.Duration
	granularity         time.Duration
	position            int
	values              map[string][]float64
	deprecatedSubLabels []lo.Tuple2[string, string]

	last time.Time
}

// NewRateCollector is shorthand for newRateCollector(window, granularity, time.Now()).
func NewRateCollector(window time.Duration, granularity time.Duration, enableSubLabel bool) (*RateCollector, error) {
	return newRateCollector(window, granularity, time.Now(), enableSubLabel)
}

// newRateCollector returns a new RateCollector with given window and granularity.
func newRateCollector(window time.Duration, granularity time.Duration, now time.Time, enableSubLabel bool) (*RateCollector, error) {
	if window == 0 || granularity == 0 {
		return nil, fmt.Errorf("create RateCollector failed, window or granularity cannot be 0, window = %d, granularity = %d", window, granularity)
	}
	if window < granularity || window%granularity != 0 {
		return nil, fmt.Errorf("create RateCollector failed, window has to be a multiplier of the granularity, window = %d, granularity = %d", window, granularity)
	}
	rc := &RateCollector{
		window:      window,
		granularity: granularity,
		position:    0,
		values:      make(map[string][]float64),
		last:        now,
	}

	if enableSubLabel {
		go rc.cleanDeprecateSubLabels()
	}
	return rc, nil
}

func (r *RateCollector) cleanDeprecateSubLabels() {
	tick := time.NewTicker(r.window * 2)
	defer tick.Stop()
	for range tick.C {
		r.Lock()
		for _, labelInfo := range r.deprecatedSubLabels {
			r.removeSubLabel(labelInfo)
		}
		r.Unlock()
	}
}

func (r *RateCollector) removeSubLabel(labelInfo lo.Tuple2[string, string]) {
	label := labelInfo.A
	subLabel := labelInfo.B
	if subLabel == "" {
		return
	}
	removeKeys := make([]string, 1)
	removeKeys[0] = FormatSubLabel(label, subLabel)

	deleteCollectionSubLabelWithPrefix := func(dbName string) {
		for key := range r.values {
			if strings.HasPrefix(key, FormatSubLabel(label, GetCollectionSubLabel(dbName, ""))) {
				removeKeys = append(removeKeys, key)
			}
		}
	}

	parts := strings.Split(subLabel, ".")
	if strings.HasPrefix(subLabel, GetDBSubLabel("")) {
		dbName := parts[1]
		deleteCollectionSubLabelWithPrefix(dbName)
	}
	for _, key := range removeKeys {
		delete(r.values, key)
	}
}

// Register init values of RateCollector for specified label.
func (r *RateCollector) Register(label string) {
	r.Lock()
	defer r.Unlock()
	if _, ok := r.values[label]; !ok {
		r.values[label] = make([]float64, int(r.window/r.granularity))
	}
}

// Deregister remove values of RateCollector for specified label.
func (r *RateCollector) Deregister(label string) {
	r.Lock()
	defer r.Unlock()
	delete(r.values, label)
}

func GetDBSubLabel(dbName string) string {
	return fmt.Sprintf("db.%s", dbName)
}

func GetCollectionSubLabel(dbName, collectionName string) string {
	return fmt.Sprintf("collection.%s.%s", dbName, collectionName)
}

func FormatSubLabel(label, subLabel string) string {
	return fmt.Sprintf("%s-%s", label, subLabel)
}

func IsSubLabel(label string) bool {
	return strings.Contains(label, "-")
}

func SplitCollectionSubLabel(label string) (mainLabel, database, collection string, ok bool) {
	if !IsSubLabel(label) {
		ok = false
		return
	}
	subMark := strings.Index(label, "-")
	mainLabel = label[:subMark]
	database, collection, ok = GetCollectionFromSubLabel(mainLabel, label)
	return
}

func GetDBFromSubLabel(label, fullLabel string) (string, bool) {
	if !strings.HasPrefix(fullLabel, FormatSubLabel(label, GetDBSubLabel(""))) {
		return "", false
	}
	return fullLabel[len(FormatSubLabel(label, GetDBSubLabel(""))):], true
}

func GetCollectionFromSubLabel(label, fullLabel string) (string, string, bool) {
	if !strings.HasPrefix(fullLabel, FormatSubLabel(label, "")) {
		return "", "", false
	}
	subLabels := strings.Split(fullLabel[len(FormatSubLabel(label, "")):], ".")
	if len(subLabels) != 3 || subLabels[0] != "collection" {
		return "", "", false
	}

	return subLabels[1], subLabels[2], true
}

func (r *RateCollector) DeregisterSubLabel(label, subLabel string) {
	r.Lock()
	defer r.Unlock()
	r.deprecatedSubLabels = append(r.deprecatedSubLabels, lo.Tuple2[string, string]{
		A: label,
		B: subLabel,
	})
}

// Add is shorthand for add(label, value, time.Now()).
func (r *RateCollector) Add(label string, value float64, subLabels ...string) {
	r.add(label, value, time.Now(), subLabels...)
}

// add increases the current value of specified label.
func (r *RateCollector) add(label string, value float64, now time.Time, subLabels ...string) {
	r.Lock()
	defer r.Unlock()
	r.update(now)
	if _, ok := r.values[label]; ok {
		r.values[label][r.position] += value
		for _, subLabel := range subLabels {
			r.unsafeAddForSubLabels(label, subLabel, value)
		}
	}
}

func (r *RateCollector) unsafeAddForSubLabels(label, subLabel string, value float64) {
	if subLabel == "" {
		return
	}
	sub := FormatSubLabel(label, subLabel)
	if _, ok := r.values[sub]; ok {
		r.values[sub][r.position] += value
		return
	}
	r.values[sub] = make([]float64, int(r.window/r.granularity))
	r.values[sub][r.position] = value
}

// Max is shorthand for max(label, time.Now()).
func (r *RateCollector) Max(label string, now time.Time) (float64, error) {
	return r.max(label, time.Now())
}

// max returns the maximal value in the window of specified label.
func (r *RateCollector) max(label string, now time.Time) (float64, error) {
	r.Lock()
	defer r.Unlock()
	r.update(now)
	if _, ok := r.values[label]; ok {
		max := float64(0)
		for _, v := range r.values[label] {
			if v > max {
				max = v
			}
		}
		return max, nil
	}
	return 0, fmt.Errorf("RateColletor didn't register for label %s", label)
}

// Min is shorthand for min(label, time.Now()).
func (r *RateCollector) Min(label string, now time.Time) (float64, error) {
	return r.min(label, time.Now())
}

// min returns the minimal value in the window of specified label.
func (r *RateCollector) min(label string, now time.Time) (float64, error) {
	r.Lock()
	defer r.Unlock()
	r.update(now)
	if _, ok := r.values[label]; ok {
		min := math.MaxFloat64
		for _, v := range r.values[label] {
			if v < min {
				min = v
			}
		}
		return min, nil
	}
	return 0, fmt.Errorf("RateColletor didn't register for label %s", label)
}

// Rate is shorthand for rate(label, duration, time.Now()).
func (r *RateCollector) Rate(label string, duration time.Duration) (float64, error) {
	return r.rate(label, duration, time.Now())
}

func (r *RateCollector) RateSubLabel(label string, duration time.Duration) (map[string]float64, error) {
	subLabelPrefix := FormatSubLabel(label, "")
	subLabels := make(map[string]float64)
	r.Lock()
	for s := range r.values {
		if strings.HasPrefix(s, subLabelPrefix) {
			subLabels[s] = 0
		}
	}
	r.Unlock()
	for s := range subLabels {
		v, err := r.rate(s, duration, time.Now())
		if err != nil {
			return nil, err
		}
		subLabels[s] = v
	}
	return subLabels, nil
}

// rate returns the latest mean value of the specified duration.
func (r *RateCollector) rate(label string, duration time.Duration, now time.Time) (float64, error) {
	if duration > r.window {
		duration = r.window
	}
	size := int(duration / r.granularity)
	if size <= 0 {
		return 0, nil
	}

	r.Lock()
	defer r.Unlock()
	r.update(now)
	if _, ok := r.values[label]; ok {
		total := float64(0)
		tmp := r.position
		for i := 0; i < size; i++ {
			total += r.values[label][tmp]
			tmp--
			if tmp < 0 {
				tmp = int(r.window/r.granularity) - 1
			}
		}
		return total / float64(size), nil
	}
	return 0, fmt.Errorf("RateColletor didn't register for label %s", label)
}

// update would update position and clear old values.
func (r *RateCollector) update(now time.Time) {
	duration := now.Sub(r.last)
	if duration/r.granularity == 0 {
		return
	}

	shift := func() {
		if r.position = r.position + 1; r.position >= int(r.window/r.granularity) {
			r.position = 0
		}
		for rt := range r.values {
			r.values[rt][r.position] = 0
		}
		r.last = r.last.Add(r.granularity)
	}

	for i := 0; i < int(duration/r.granularity); i++ {
		shift()
	}
}

// nolint
// print is a helper function to print values.
func (r *RateCollector) print() {
	for k, v := range r.values {
		fmt.Printf("========== %s ==========\n", k)
		fmt.Println(v)
		fmt.Println("position=", r.position)
	}
}
