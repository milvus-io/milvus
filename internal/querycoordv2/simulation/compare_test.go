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

//go:build test

package simulation

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSimCompareReports prints a side-by-side table of two report files:
//
//	MILVUS_SIM_COMPARE=base.jsonl,candidate.jsonl go test -run TestSimCompareReports ...
//
// Repeated runs of one scenario in a file are averaged.
func TestSimCompareReports(t *testing.T) {
	spec := os.Getenv("MILVUS_SIM_COMPARE")
	if spec == "" {
		t.Skip("set MILVUS_SIM_COMPARE=base.jsonl,candidate.jsonl")
	}
	files := strings.Split(spec, ",")
	require.Len(t, files, 2)
	base, candidate := readReports(t, files[0]), readReports(t, files[1])

	scenarios := make([]string, 0, len(base))
	for name := range base {
		scenarios = append(scenarios, name)
	}
	sort.Strings(scenarios)
	var b strings.Builder
	fmt.Fprintf(&b, "\n%-34s %-28s %12s %12s %9s\n", "scenario", "metric", files[0], files[1], "change")
	for _, name := range scenarios {
		metrics := make([]string, 0, len(base[name]))
		for k := range base[name] {
			metrics = append(metrics, k)
		}
		sort.Strings(metrics)
		for _, k := range metrics {
			before, after := base[name][k], candidate[name][k]
			change := "-"
			if before != 0 {
				change = fmt.Sprintf("%+.0f%%", (after-before)/before*100)
			}
			fmt.Fprintf(&b, "%-34s %-28s %12.3f %12.3f %9s\n", name, k, before, after, change)
		}
	}
	t.Log(b.String())
}

func readReports(t *testing.T, path string) map[string]map[string]float64 {
	f, err := os.Open(path) //nolint:gosec // report path from MILVUS_SIM_COMPARE
	require.NoError(t, err)
	defer f.Close()
	sums := map[string]map[string]float64{}
	counts := map[string]float64{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var res Result
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &res))
		if sums[res.Scenario] == nil {
			sums[res.Scenario] = map[string]float64{}
		}
		for k, v := range res.Metrics {
			sums[res.Scenario][k] += v
		}
		counts[res.Scenario]++
	}
	for name, metrics := range sums {
		for k := range metrics {
			metrics[k] /= counts[name]
		}
	}
	return sums
}
