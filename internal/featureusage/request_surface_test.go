// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package featureusage

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

const requestSurfaceFile = "testdata/request_surface.golden"

// requestScope keeps the walk inside Milvus's own proto packages; well-known
// types such as google.protobuf.* carry no feature.
const requestScope = "milvus.proto."

// requestPlumbing are messages every request embeds for routing and tracing.
// MsgBase's MsgType enum grows with each internal message type, which says
// nothing about user features.
var requestPlumbing = map[protoreflect.FullName]bool{
	"milvus.proto.common.MsgBase": true,
}

// Request counters are added by hand, so a feature that adds a request field
// or an enum value reaches users whether or not anybody counts it, and
// TestReportSurfaceIsStable cannot notice a counter that was never written.
// This test pins everything a client request or a query plan can carry to a
// checked-in file. A milvus-proto bump or a plan.proto change that adds a
// field or an enum value fails here, and whoever makes it has to look at the
// new names and decide whether one of them needs a counter. Deciding "no"
// costs one rerun:
//
//	go test ./internal/featureusage/ -update-surface
func TestRequestSurfaceIsStable(t *testing.T) {
	got := renderRequestSurface()

	if *updateSurface {
		require.NoError(t, os.MkdirAll(filepath.Dir(requestSurfaceFile), 0o755))
		require.NoError(t, os.WriteFile(requestSurfaceFile, []byte(got), 0o600))
		t.Log("request surface file rewritten")
		return
	}

	wantBytes, err := os.ReadFile(requestSurfaceFile)
	require.NoError(t, err, "golden request surface file missing; regenerate with -update-surface")

	removed, added := diffLines(string(wantBytes), got)
	if len(removed) == 0 && len(added) == 0 {
		return
	}
	t.Errorf("the fields or enum values a request can carry changed.\n"+
		"new:\n  %s\n"+
		"gone:\n  %s\n"+
		"If a new name is a user-facing feature, add a counter for it (see \"New request fields are pinned too\" "+
		"in docs/design-docs/design_docs/20260902-feature-usage-reporting.md).\n"+
		"Then rerun with -update-surface and commit %s.",
		strings.Join(added, "\n  "), strings.Join(removed, "\n  "), requestSurfaceFile)
}

// renderRequestSurface walks the input message of every MilvusService RPC and
// the query plan, recursing into nested messages, and lists every field and
// every enum value once, sorted.
func renderRequestSurface() string {
	lines := make(map[string]struct{})
	seen := make(map[protoreflect.FullName]bool)

	walkEnum := func(e protoreflect.EnumDescriptor) {
		if seen[e.FullName()] || !strings.HasPrefix(string(e.FullName()), requestScope) {
			return
		}
		seen[e.FullName()] = true
		values := e.Values()
		for i := 0; i < values.Len(); i++ {
			// A value's own FullName is scoped to the enum's parent, which
			// hides the enum; spell it out.
			lines["enum\t"+string(e.FullName())+"."+string(values.Get(i).Name())] = struct{}{}
		}
	}

	var walk func(m protoreflect.MessageDescriptor)
	walk = func(m protoreflect.MessageDescriptor) {
		if seen[m.FullName()] || requestPlumbing[m.FullName()] ||
			!strings.HasPrefix(string(m.FullName()), requestScope) {
			return
		}
		seen[m.FullName()] = true
		fields := m.Fields()
		for i := 0; i < fields.Len(); i++ {
			f := fields.Get(i)
			// A map field is listed once; its synthetic key/value entry is not.
			if !m.IsMapEntry() {
				lines["field\t"+string(f.FullName())] = struct{}{}
			}
			if e := f.Enum(); e != nil {
				walkEnum(e)
			}
			if sub := f.Message(); sub != nil {
				walk(sub)
			}
		}
	}

	methods := milvuspb.File_milvus_proto.Services().ByName("MilvusService").Methods()
	for i := 0; i < methods.Len(); i++ {
		walk(methods.Get(i).Input())
	}
	walk((&planpb.PlanNode{}).ProtoReflect().Descriptor())

	sorted := make([]string, 0, len(lines))
	for line := range lines {
		sorted = append(sorted, line)
	}
	sort.Strings(sorted)

	var b strings.Builder
	b.WriteString("# Every field and enum value a client request or a query plan can carry.\n")
	b.WriteString("# Generated by TestRequestSurfaceIsStable -update-surface. Do not edit by hand.\n")
	for _, line := range sorted {
		b.WriteString(line)
		b.WriteByte('\n')
	}
	return b.String()
}
