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

package funcutil

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
)

const milvusServiceName protoreflect.Name = "MilvusService"

var rbacAnnotationAllowlist = map[protoreflect.Name]string{
	// AllocTimestamp only allocates a logical timestamp and does not access user data.
	"AllocTimestamp": "logical timestamp allocation without object access",
	// CalcDistance is a deprecated endpoint that returns service-unavailable before object access.
	"CalcDistance": "deprecated endpoint rejected before object access",
	// CheckHealth is the unauthenticated readiness probe used by clients and orchestration.
	"CheckHealth": "health probe",
	// ComputePhraseMatchSlop runs text analysis from request literals without object lookup.
	"ComputePhraseMatchSlop": "stateless text-analysis helper",
	// Connect is the handshake that establishes client connection metadata before RBAC context exists.
	"Connect": "client handshake",
	// CreateReplicateStream is the CDC data-replication stream entrypoint.
	"CreateReplicateStream": "CDC replication stream",
	// DescribePrewarmTask only polls the state of an asynchronous prewarm job.
	"DescribePrewarmTask": "async prewarm status polling",
	// DescribeSegmentIndexData is an unimplemented feder endpoint that returns service-unavailable.
	"DescribeSegmentIndexData": "unimplemented feder endpoint",
	// Dummy is a legacy test/debug shim that dispatches to already-typed request handlers.
	"Dummy": "legacy debug shim",
	// TODO(milvus-io/milvus#52238): DumpMessages needs CDC/salvage WAL authorization.
	"DumpMessages": "temporary CDC/salvage authorization gap",
	// GetCompactionState only polls the state of a previously-started compaction job.
	"GetCompactionState": "async compaction status polling",
	// GetCompactionStateWithPlans only polls detailed state for a previously-started compaction job.
	"GetCompactionStateWithPlans": "async compaction plan status polling",
	// GetComponentStates reports component liveness and does not access user objects.
	"GetComponentStates": "component liveness diagnostics",
	// GetFlushAllState only polls global flush completion by timestamp.
	"GetFlushAllState": "async global flush status polling",
	// GetImportState is a v1 compatibility wrapper for import progress polling.
	"GetImportState": "legacy import status polling",
	// GetMetrics is a diagnostics endpoint for system metrics.
	"GetMetrics": "system metrics diagnostics",
	// TODO(milvus-proto#652): remove once GetPartitionStatistics has a collection privilege annotation.
	"GetPartitionStatistics": "temporary proto gap for collection statistics",
	// GetPersistentSegmentInfo is a legacy segment diagnostics endpoint.
	"GetPersistentSegmentInfo": "legacy segment diagnostics",
	// GetQuerySegmentInfo is a legacy query-segment diagnostics endpoint.
	"GetQuerySegmentInfo": "legacy query-segment diagnostics",
	// GetRefreshExternalCollectionProgress only polls the state of an external-collection refresh job.
	"GetRefreshExternalCollectionProgress": "async external-collection refresh status polling",
	// GetReplicas is a legacy replica-topology diagnostics endpoint.
	"GetReplicas": "legacy replica diagnostics",
	// GetReplicateInfo reports CDC checkpoint metadata for replication recovery.
	"GetReplicateInfo": "CDC checkpoint diagnostics",
	// GetRestoreSnapshotState only polls the state of a snapshot restore job.
	"GetRestoreSnapshotState": "async snapshot-restore status polling",
	// GetVersion returns build/version metadata and does not access user objects.
	"GetVersion": "build metadata",
	// HasCollection is an existence probe kept public for SDK compatibility.
	"HasCollection": "collection existence probe",
	// ListDatabases is result-filtered by rootcoord using the caller roles.
	"ListDatabases": "result-filtered by caller grants",
	// ListImportTasks is a v1 compatibility wrapper for import task listing.
	"ListImportTasks": "legacy import task listing",
	// ListIndexedSegment is an unimplemented feder endpoint that returns service-unavailable.
	"ListIndexedSegment": "unimplemented feder endpoint",
	// ListRefreshExternalCollectionJobs lists external-collection refresh jobs, optionally scoped by collection.
	"ListRefreshExternalCollectionJobs": "external-collection refresh job listing",
	// RegisterLink only reports proxy deployment metadata for client link registration.
	"RegisterLink": "client link registration",
	// ReplicateMessage is the CDC message-replication endpoint.
	"ReplicateMessage": "CDC message replication",
	// TODO(milvus-io/milvus#52237): RunAnalyzer collection-scoped path needs RBAC coverage.
	"RunAnalyzer": "temporary collection-scoped analyzer authorization gap",
	// ShowCollections is result-filtered by rootcoord using the caller roles.
	"ShowCollections": "result-filtered by caller grants",
}

func TestRBACAnnotationCoverage(t *testing.T) {
	service := milvuspb.File_milvus_proto.Services().ByName(milvusServiceName)
	if service == nil {
		t.Fatal("MilvusService descriptor not found")
	}

	requireAllowlistComments(t)

	seenMethods := make(map[protoreflect.Name]struct{})
	var missing []string
	var unallowlistedStreams []string
	var stale []string
	var undocumented []string
	methods := service.Methods()
	for i := 0; i < methods.Len(); i++ {
		method := methods.Get(i)
		methodName := method.Name()
		seenMethods[methodName] = struct{}{}

		reason, allowlisted := rbacAnnotationAllowlist[methodName]
		if allowlisted && (strings.TrimSpace(reason) == "" || strings.Contains(reason, "\n")) {
			undocumented = append(undocumented, string(methodName))
		}

		hasAnnotation := hasPrivilegeExtObj(method.Input())
		switch {
		case isStreamMethod(method) && !allowlisted:
			unallowlistedStreams = append(unallowlistedStreams, fmt.Sprintf("%s (%s)", methodName, method.Input().FullName()))
		case hasAnnotation && allowlisted && !isStreamMethod(method):
			stale = append(stale, fmt.Sprintf("%s now has privilege_ext_obj; remove it from rbacAnnotationAllowlist", methodName))
		case !hasAnnotation && !allowlisted:
			missing = append(missing, fmt.Sprintf("%s (%s)", methodName, method.Input().FullName()))
		}
	}

	for methodName := range rbacAnnotationAllowlist {
		if _, ok := seenMethods[methodName]; !ok {
			stale = append(stale, fmt.Sprintf("%s is not a MilvusService RPC; remove it from rbacAnnotationAllowlist", methodName))
		}
	}

	sort.Strings(undocumented)
	sort.Strings(unallowlistedStreams)
	sort.Strings(stale)
	sort.Strings(missing)

	requireNoRBACAnnotationCoverageErrors(t, "allowlist entries without one-line justification", undocumented)
	requireNoRBACAnnotationCoverageErrors(t, "MilvusService stream RPCs without explicit allowlist entry; the proxy RBAC interceptor is unary-only", unallowlistedStreams)
	requireNoRBACAnnotationCoverageErrors(t, "stale allowlist entries", stale)
	requireNoRBACAnnotationCoverageErrors(t, "MilvusService RPC request types missing privilege_ext_obj annotation and allowlist entry", missing)
}

func hasPrivilegeExtObj(message protoreflect.MessageDescriptor) bool {
	return proto.HasExtension(message.Options(), commonpb.E_PrivilegeExtObj)
}

func isStreamMethod(method protoreflect.MethodDescriptor) bool {
	return method.IsStreamingClient() || method.IsStreamingServer()
}

func requireNoRBACAnnotationCoverageErrors(t *testing.T, title string, entries []string) {
	t.Helper()
	if len(entries) == 0 {
		return
	}
	t.Fatalf("%s:\n%s", title, strings.Join(entries, "\n"))
}

func requireAllowlistComments(t *testing.T) {
	t.Helper()

	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to locate current test file")
	}
	source, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("failed to read current test file: %v", err)
	}

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filename, source, parser.ParseComments)
	if err != nil {
		t.Fatalf("failed to parse current test file: %v", err)
	}

	allowlist := findAllowlistLiteral(file)
	if allowlist == nil {
		t.Fatal("rbacAnnotationAllowlist declaration not found")
	}

	lines := strings.Split(string(source), "\n")
	var missingComments []string
	for _, elt := range allowlist.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		methodName, ok := allowlistMethodName(kv.Key)
		if !ok {
			continue
		}
		line := fset.Position(kv.Pos()).Line
		if line < 2 {
			missingComments = append(missingComments, methodName)
			continue
		}

		comment := strings.TrimSpace(lines[line-2])
		if !strings.HasPrefix(comment, "// ") || !strings.Contains(comment, methodName) {
			missingComments = append(missingComments, methodName)
		}
	}

	sort.Strings(missingComments)
	requireNoRBACAnnotationCoverageErrors(t, "allowlist entries without an adjacent one-line source comment mentioning the RPC", missingComments)
}

func findAllowlistLiteral(file *ast.File) *ast.CompositeLit {
	for _, decl := range file.Decls {
		genDecl, ok := decl.(*ast.GenDecl)
		if !ok || genDecl.Tok != token.VAR {
			continue
		}
		for _, spec := range genDecl.Specs {
			valueSpec, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, name := range valueSpec.Names {
				if name.Name != "rbacAnnotationAllowlist" || len(valueSpec.Values) <= i {
					continue
				}
				allowlist, _ := valueSpec.Values[i].(*ast.CompositeLit)
				return allowlist
			}
		}
	}
	return nil
}

func allowlistMethodName(expr ast.Expr) (string, bool) {
	lit, ok := expr.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	methodName, err := strconv.Unquote(lit.Value)
	if err != nil {
		return "", false
	}
	return methodName, true
}
