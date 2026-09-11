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

package featureusage

import (
	"bufio"
	"context"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/featureusage"
)

// surfacePath is the checked-in list of everything the report can emit. The
// same file backs internal/featureusage's TestReportSurfaceIsStable, so the
// two cannot drift apart.
const surfacePath = "../../../internal/featureusage/testdata/report_surface.golden"

// notDrivable are the counters no request can move in this environment, each
// with the reason. They are the only permitted gaps: anything else missing
// from the report at the end of the run fails the coverage check.
//
// Keep this list short and justified. A counter parked here is a counter the
// test team cannot accept, so it should either be driven or deleted.
var notDrivable = map[string]string{
	"mixcoord\tcompaction=_other": "the fold slot for an unrecognized CompactionType; no request can produce one",
	"mixcoord\tcompaction=PartitionKeySortCompaction": "no DataCoord path constructs this type today: " +
		"CompactionTriggerType.GetCompactionType emits only Level0Delete, Mix, Clustering, Sort and BumpSchemaVersion, " +
		"and a partition-key collection is sorted as a plain SortCompaction",
	"mixcoord\tcompaction=ClusteringPartitionKeySortCompaction": "same as PartitionKeySortCompaction: declared in the " +
		"proto and handled defensively downstream, but nothing in this tree produces it",
}

// TestCoverage is the acceptance gate: after every other test method has run,
// every counter the report can emit must have a non-zero value somewhere in
// the report, or be listed in notDrivable with a reason.
//
// It runs last because testify executes suite methods in name order and this
// method's name sorts after every other Test... method in the package. The
// check reads the same golden file the unit test pins, so adding a counter
// without exercising it fails here.
func (s *Suite) TestZZCoverage() {
	ctx := context.Background()
	// reportRaw, not report: an earlier test kills a QueryNode on purpose and
	// its session lingers, so the report legitimately still lists it as
	// unreachable when this runs.
	report := s.reportRaw(ctx)

	hit := map[string]bool{}
	for _, node := range report.GetNodes() {
		for _, e := range node.GetEntries() {
			if e.GetGroup() == featureusage.GroupRequest && e.GetValue() > 0 {
				hit[node.GetRole()+"\t"+e.GetName()] = true
			}
		}
	}

	var missing []string
	for _, counter := range readSurfaceCounters(s.T()) {
		if hit[counter] {
			continue
		}
		if _, excused := notDrivable[counter]; excused {
			continue
		}
		missing = append(missing, counter)
	}
	sort.Strings(missing)

	if len(missing) > 0 {
		s.T().Errorf("%d counters were never exercised end to end:\n  %s\n"+
			"Every counter in %s must be driven by a test in this package, "+
			"or listed in notDrivable with the reason it cannot be.",
			len(missing), strings.Join(missing, "\n  "), surfacePath)
	}

	// The excuses have to stay honest: a counter listed as undrivable that a
	// test did move is a stale excuse.
	for counter, reason := range notDrivable {
		s.Falsef(hit[counter], "%s is listed as not drivable (%q) but a test moved it", counter, reason)
	}

	// Groups are covered the same way: every group in the surface file must
	// appear in the report at least once.
	present := map[string]bool{}
	for _, node := range report.GetNodes() {
		for _, e := range node.GetEntries() {
			present[e.GetGroup()] = true
		}
	}
	for _, g := range featureusage.AllGroups() {
		s.Truef(present[g], "group %q never appeared in the report", g)
	}
}

// readSurfaceCounters returns "role\tname" for every counter line in the
// golden surface file.
func readSurfaceCounters(t *testing.T) []string {
	t.Helper()
	f, err := os.Open(surfacePath)
	if err != nil {
		t.Fatalf("cannot read the report surface file: %v", err)
	}
	defer f.Close()

	var out []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "counter\t") {
			continue
		}
		parts := strings.SplitN(line, "\t", 3)
		if len(parts) != 3 {
			t.Fatalf("malformed surface line %q", line)
		}
		out = append(out, parts[1]+"\t"+parts[2])
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(out) == 0 {
		t.Fatalf("no counters found in %s", surfacePath)
	}
	return out
}

func TestFeatureUsage(t *testing.T) {
	suite.Run(t, new(Suite))
}
