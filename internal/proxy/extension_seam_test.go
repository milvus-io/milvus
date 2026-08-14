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

package proxy

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// dmlBlockingExtension records the operation names it sees and refuses every write.
type dmlBlockingExtension struct {
	extension.NoopProxyExtension
	seen []string
}

func (d *dmlBlockingExtension) InterceptDML(ctx context.Context, op string, req proto.Message) *commonpb.Status {
	d.seen = append(d.seen, op)
	return merr.Status(merr.WrapErrServiceInternal("write is not served"))
}

type testProvider struct{ caps extension.Capabilities }

func (testProvider) Name() string                           { return "test" }
func (testProvider) Requires() []extension.CapabilityID     { return nil }
func (p testProvider) Capabilities() extension.Capabilities { return p.caps }

func installBlockingExtension(t *testing.T) *dmlBlockingExtension {
	t.Helper()
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)

	ext := &dmlBlockingExtension{}
	assert.NoError(t, extension.SetProvider(testProvider{
		caps: extension.Capabilities{ProxyExt: ext},
	}))
	return ext
}

func TestProxyExtensionFallsBackToNoop(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)

	assert.Nil(t, interceptDML(context.Background(), "Insert", &milvuspb.InsertRequest{}), "the seam must be transparent when no provider is installed")
}

func TestInterceptDMLConsultsInstalledExtension(t *testing.T) {
	ext := installBlockingExtension(t)

	st := interceptDML(context.Background(), "Insert", &milvuspb.InsertRequest{})
	assert.NotNil(t, st, "the seam must surface the status when the extension refuses the write")
	assert.Equal(t, []string{"Insert"}, ext.seen, "the seam must pass the operation name through unchanged")
}

// Every write-path RPC consults the seam, not just Insert. Flush and FlushAll
// are in the table although they move no rows: they seal segments and force
// them out, which only means something on an instance that accepted the writes
// behind them. Each case pins its own call site, so a refactor that drops one
// handler's call fails that handler's case by name.
func TestEveryWritePathIsGuardedByTheDMLSeam(t *testing.T) {
	status := func(s *commonpb.Status) *commonpb.Status { return s }
	cases := []struct {
		op   string
		call func(node *Proxy, ctx context.Context) *commonpb.Status
	}{
		{"Insert", func(node *Proxy, ctx context.Context) *commonpb.Status {
			resp, err := node.Insert(ctx, &milvuspb.InsertRequest{DbName: "db", CollectionName: "coll"})
			assert.NoError(t, err)
			return status(resp.GetStatus())
		}},
		{"Delete", func(node *Proxy, ctx context.Context) *commonpb.Status {
			resp, err := node.Delete(ctx, &milvuspb.DeleteRequest{DbName: "db", CollectionName: "coll"})
			assert.NoError(t, err)
			return status(resp.GetStatus())
		}},
		{"Upsert", func(node *Proxy, ctx context.Context) *commonpb.Status {
			resp, err := node.Upsert(ctx, &milvuspb.UpsertRequest{DbName: "db", CollectionName: "coll"})
			assert.NoError(t, err)
			return status(resp.GetStatus())
		}},
		{"Flush", func(node *Proxy, ctx context.Context) *commonpb.Status {
			resp, err := node.Flush(ctx, &milvuspb.FlushRequest{DbName: "db", CollectionNames: []string{"coll"}})
			assert.NoError(t, err)
			return status(resp.GetStatus())
		}},
		{"FlushAll", func(node *Proxy, ctx context.Context) *commonpb.Status {
			resp, err := node.FlushAll(ctx, &milvuspb.FlushAllRequest{})
			assert.NoError(t, err)
			return status(resp.GetStatus())
		}},
	}
	for _, tc := range cases {
		t.Run(tc.op, func(t *testing.T) {
			ext := installBlockingExtension(t)
			node := &Proxy{}
			node.UpdateStateCode(commonpb.StateCode_Healthy)

			st := tc.call(node, context.Background())

			assert.NotNil(t, st)
			assert.NotEqual(t, int32(0), st.GetCode(), "a refused write must not return a success status")
			assert.Equal(t, []string{tc.op}, ext.seen,
				"%s must go through the DML seam; this fails if a refactor moves the call site", tc.op)
		})
	}
}

type stubVerifier struct {
	user     string
	err      error
	external bool
}

func (s stubVerifier) Verify(string) (string, error)         { return s.user, s.err }
func (s stubVerifier) RequireAPIKeyOnExternalListener() bool { return s.external }

func TestVerifyAPIKeyUsesInstalledVerifier(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)

	assert.NoError(t, extension.SetProvider(testProvider{
		caps: extension.Capabilities{APIKey: stubVerifier{user: "alice"}},
	}))

	user, err := VerifyAPIKey("tok")
	assert.NoError(t, err)
	assert.Equal(t, "alice", user, "the installed verifier must decide the username")
}

func TestVerifyAPIKeyReportsVerifierRejection(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)

	assert.NoError(t, extension.SetProvider(testProvider{
		caps: extension.Capabilities{APIKey: stubVerifier{err: errors.New("nope")}},
	}))

	_, err := VerifyAPIKey("tok")
	assert.Error(t, err, "a rejected token must not authenticate")
}

func TestVerifyAPIKeyFallsBackToNativeHookWithNoProviderInstalled(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)

	// InitOnceHook must run before SetMockAPIHook: its first run unconditionally
	// (re)installs the default hook, which would otherwise clobber the mock
	// installed below.
	hookutil.InitOnceHook()
	hookutil.SetMockAPIHook("native-user", nil)
	t.Cleanup(func() { hookutil.SetMockAPIHook("", nil) })

	user, err := VerifyAPIKey("tok")
	assert.NoError(t, err)
	assert.Equal(t, "native-user", user,
		"with no verifier installed VerifyAPIKey must still reach the native hook path, so a stock binary is unchanged")
}

func TestVerifyAPIKeyRejectsEmptyUsernameFromInstalledVerifier(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)

	assert.NoError(t, extension.SetProvider(testProvider{
		caps: extension.Capabilities{APIKey: stubVerifier{user: ""}},
	}))

	_, err := VerifyAPIKey("tok")
	assert.Error(t, err, "an installed verifier that returns no username must not authenticate the request")
}

func TestExternalListenerPolicyDefaultsToOpen(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)

	assert.False(t, ExternalListenerRequiresAPIKey(),
		"with no verifier installed the external listener keeps accepting passwords")
}

func TestExternalListenerPolicyFollowsVerifier(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)

	assert.NoError(t, extension.SetProvider(testProvider{
		caps: extension.Capabilities{APIKey: stubVerifier{external: true}},
	}))
	assert.True(t, ExternalListenerRequiresAPIKey())
}

// recordingAdmissionChecker records how many times each check method ran and
// which CoordClient it received, and returns err from both. The call counts
// double as the zero-call proof for the idempotent-retry short-circuit: a
// call site that skips admission for an already-existing target must never
// move these off zero.
type recordingAdmissionChecker struct {
	collectionCalls int
	databaseCalls   int
	coordSeen       extension.CoordClient
	err             error
}

func (r *recordingAdmissionChecker) CheckCreateCollection(ctx context.Context, coord extension.CoordClient) error {
	r.collectionCalls++
	r.coordSeen = coord
	return r.err
}

func (r *recordingAdmissionChecker) CheckCreateDatabase(ctx context.Context, coord extension.CoordClient) error {
	r.databaseCalls++
	r.coordSeen = coord
	return r.err
}

func TestCheckCreateCollectionAdmissionNoOpWithNoProvider(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)

	// mocks.NewMockMixCoordClient registers no expectations here, so calling
	// any of its methods fails the test immediately (mockery routes an
	// unmatched call through m.Test(t)). Reaching the assertion below without
	// failure, plus the explicit Calls check, is the zero-call proof.
	mockCoord := mocks.NewMockMixCoordClient(t)

	err := checkCreateCollectionAdmission(context.Background(), mockCoord)
	assert.NoError(t, err)
	assert.Empty(t, mockCoord.Calls, "with no provider installed checkCreateCollectionAdmission must not touch coord at all")
}

func TestCheckCreateCollectionAdmissionPassesCoordThroughToChecker(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)

	mockCoord := mocks.NewMockMixCoordClient(t)
	wantResp := &milvuspb.ListDatabasesResponse{DbNames: []string{"probe-db"}}
	mockCoord.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(wantResp, nil).Once()

	checker := &recordingAdmissionChecker{}
	assert.NoError(t, extension.SetProvider(testProvider{caps: extension.Capabilities{Admission: checker}}))

	err := checkCreateCollectionAdmission(context.Background(), mockCoord)
	assert.NoError(t, err)
	assert.Equal(t, 1, checker.collectionCalls, "the installed checker must be consulted")

	if assert.NotNil(t, checker.coordSeen, "the checker must receive a non-nil CoordClient") {
		gotResp, err := checker.coordSeen.ListDatabases(context.Background(), &milvuspb.ListDatabasesRequest{})
		assert.NoError(t, err)
		assert.Same(t, wantResp, gotResp,
			"the CoordClient handed to the checker must forward calls to the underlying mixCoord, proving the adapter is not a stub")
	}
}

func TestCheckCreateCollectionAdmissionErrorPropagatesUnchanged(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)

	sentinel := errors.New("quota exhausted for this instance")
	checker := &recordingAdmissionChecker{err: sentinel}
	assert.NoError(t, extension.SetProvider(testProvider{caps: extension.Capabilities{Admission: checker}}))

	mockCoord := mocks.NewMockMixCoordClient(t)
	err := checkCreateCollectionAdmission(context.Background(), mockCoord)
	assert.Same(t, sentinel, err, "the checker's error must reach the caller unchanged, not rewrapped into a different error")
}

// newAdmissionTestCreateCollectionTask builds a createCollectionTask whose
// schema passes every validation step ahead of the admission short-circuit,
// so PreExecute reaches the code under test.
func newAdmissionTestCreateCollectionTask(t *testing.T, collectionName string) *createCollectionTask {
	t.Helper()
	fieldName2Type := map[string]schemapb.DataType{
		"int64": schemapb.DataType_Int64,
		"fvec":  schemapb.DataType_FloatVector,
	}
	schema := constructCollectionSchemaByDataType(collectionName, fieldName2Type, "int64", false)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	ctx := context.Background()
	return &createCollectionTask{
		Condition: NewTaskCondition(ctx),
		CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
			Base:           &commonpb.MsgBase{},
			CollectionName: collectionName,
			Schema:         marshaledSchema,
			ShardsNum:      1,
		},
		ctx:      ctx,
		mixCoord: mocks.NewMockMixCoordClient(t),
	}
}

// TestCheckCreateCollectionAdmissionSkipsExistenceLookupWhenCheckerAdmits pins
// the reordered contract's main payoff: when admission admits -- the common
// case under capacity, and the only case with no provider installed -- the
// existence lookup (and the coordinator round trip it can cost on a cache
// miss) never runs at all. The recorder on globalMetaCache makes "never
// consulted" an assertion about a call count, not an inference from the
// return value.
func TestCheckCreateCollectionAdmissionSkipsExistenceLookupWhenCheckerAdmits(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	oldCache := globalMetaCache
	t.Cleanup(func() { globalMetaCache = oldCache })

	// No GetCollectionID expectation is registered: an unexpected call here
	// fails the test on its own, before the explicit assertion below even
	// runs.
	cache := NewMockCache(t)
	globalMetaCache = cache

	checker := &recordingAdmissionChecker{err: nil}
	assert.NoError(t, extension.SetProvider(testProvider{caps: extension.Capabilities{Admission: checker}}))

	task := newAdmissionTestCreateCollectionTask(t, "brand_new_coll")
	err := task.PreExecute(task.ctx)

	assert.NoError(t, err)
	assert.Equal(t, 1, checker.collectionCalls, "admission is always consulted first, regardless of existence")
	cache.AssertNumberOfCalls(t, "GetCollectionID", 0)
}

// TestCheckCreateCollectionAdmissionAdmitsRetryWhenCollectionAlreadyExists
// pins the other half of the reordered contract: when admission would
// reject, the existence lookup runs, and finding the collection already
// counted turns the rejection into a nil so a retry still reaches
// rootcoord's own idempotent answer instead of seeing ResourceExhausted.
func TestCheckCreateCollectionAdmissionAdmitsRetryWhenCollectionAlreadyExists(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	oldCache := globalMetaCache
	t.Cleanup(func() { globalMetaCache = oldCache })

	collectionName := "already_exists_coll"
	cache := NewMockCache(t)
	cache.On("GetCollectionID", mock.Anything, "", collectionName).Return(UniqueID(1001), nil)
	globalMetaCache = cache

	checker := &recordingAdmissionChecker{err: errors.New("quota exhausted")}
	assert.NoError(t, extension.SetProvider(testProvider{caps: extension.Capabilities{Admission: checker}}))

	task := newAdmissionTestCreateCollectionTask(t, collectionName)
	err := task.PreExecute(task.ctx)

	assert.NoError(t, err, "an idempotent re-create of an existing collection must not be blocked by admission")
	assert.Equal(t, 1, checker.collectionCalls, "admission is consulted first, even though it ends up overridden")
	cache.AssertNumberOfCalls(t, "GetCollectionID", 1)
}

// TestCheckCreateCollectionAdmissionRejectsWhenCollectionIsGenuinelyNew
// confirms the reorder does not weaken the rejection itself: when admission
// would reject and the collection genuinely does not exist, the rejection
// still surfaces unchanged.
func TestCheckCreateCollectionAdmissionRejectsWhenCollectionIsGenuinelyNew(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	oldCache := globalMetaCache
	t.Cleanup(func() { globalMetaCache = oldCache })

	collectionName := "brand_new_coll"
	cache := NewMockCache(t)
	cache.On("GetCollectionID", mock.Anything, "", collectionName).Return(UniqueID(0), merr.WrapErrCollectionNotFound(collectionName))
	globalMetaCache = cache

	sentinel := errors.New("quota exhausted for this instance")
	checker := &recordingAdmissionChecker{err: sentinel}
	assert.NoError(t, extension.SetProvider(testProvider{caps: extension.Capabilities{Admission: checker}}))

	task := newAdmissionTestCreateCollectionTask(t, collectionName)
	err := task.PreExecute(task.ctx)

	assert.Equal(t, 1, checker.collectionCalls, "admission must be consulted for a genuinely new collection")
	cache.AssertNumberOfCalls(t, "GetCollectionID", 1)
	assert.Same(t, sentinel, err, "the checker's error must surface from PreExecute unchanged")
}

// newAdmissionTestCreateDatabaseTask builds a createDatabaseTask whose name
// passes validation ahead of the admission short-circuit, so PreExecute
// reaches the code under test.
func newAdmissionTestCreateDatabaseTask(t *testing.T, dbName string) *createDatabaseTask {
	t.Helper()
	ctx := context.Background()
	return &createDatabaseTask{
		Condition: NewTaskCondition(ctx),
		CreateDatabaseRequest: &milvuspb.CreateDatabaseRequest{
			DbName: dbName,
		},
		ctx:      ctx,
		mixCoord: mocks.NewMockMixCoordClient(t),
	}
}

func TestCheckCreateDatabaseAdmissionSkippedWhenDatabaseAlreadyExists(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	oldCache := globalMetaCache
	t.Cleanup(func() { globalMetaCache = oldCache })

	dbName := "already_exists_db"
	cache := NewMockCache(t)
	cache.On("HasDatabase", mock.Anything, dbName).Return(true)
	globalMetaCache = cache

	checker := &recordingAdmissionChecker{err: errors.New("quota exhausted")}
	assert.NoError(t, extension.SetProvider(testProvider{caps: extension.Capabilities{Admission: checker}}))

	task := newAdmissionTestCreateDatabaseTask(t, dbName)
	err := task.PreExecute(task.ctx)

	assert.NoError(t, err, "an idempotent re-create of an existing database must not be blocked by admission")
	assert.Equal(t, 0, checker.databaseCalls, "admission must not be consulted once the database is already known to exist")
}

func TestCheckCreateDatabaseAdmissionConsultedWhenDatabaseIsNew(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	oldCache := globalMetaCache
	t.Cleanup(func() { globalMetaCache = oldCache })

	dbName := "brand_new_db"
	cache := NewMockCache(t)
	cache.On("HasDatabase", mock.Anything, dbName).Return(false)
	globalMetaCache = cache

	sentinel := errors.New("quota exhausted for this instance")
	checker := &recordingAdmissionChecker{err: sentinel}
	assert.NoError(t, extension.SetProvider(testProvider{caps: extension.Capabilities{Admission: checker}}))

	task := newAdmissionTestCreateDatabaseTask(t, dbName)
	err := task.PreExecute(task.ctx)

	assert.Equal(t, 1, checker.databaseCalls, "admission must be consulted for a genuinely new database")
	assert.Same(t, sentinel, err, "the checker's error must surface from PreExecute unchanged")
}

// TestCheckCreateDatabaseAdmissionSkipsExistenceLookupWithNoProvider pins the
// property the round-5 fix restores, now through admissionChecker() and the
// direct caps.Admission.CheckCreateDatabase call in task_database.go: with no
// provider installed, createDatabaseTask.PreExecute must reach exactly the
// statements it reached before the admission seam existed, touching neither
// the metadata cache nor the coordinator. Both recorders (globalMetaCache and
// the mixCoord mock) make "never consulted" an assertion about a call count,
// the same shape as
// TestCheckCreateCollectionAdmissionSkipsExistenceLookupWhenCheckerAdmits.
func TestCheckCreateDatabaseAdmissionSkipsExistenceLookupWithNoProvider(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	oldCache := globalMetaCache
	t.Cleanup(func() { globalMetaCache = oldCache })

	// No HasDatabase expectation is registered: an unexpected call fails the
	// test on its own, before the explicit assertion below even runs.
	cache := NewMockCache(t)
	globalMetaCache = cache

	task := newAdmissionTestCreateDatabaseTask(t, "brand_new_db")
	err := task.PreExecute(task.ctx)

	assert.NoError(t, err)
	cache.AssertNumberOfCalls(t, "HasDatabase", 0)
	mockCoord, ok := task.mixCoord.(*mocks.MockMixCoordClient)
	if assert.True(t, ok, "test helper must build mixCoord as a MockMixCoordClient") {
		assert.Empty(t, mockCoord.Calls, "with no provider installed PreExecute must not touch coord at all")
	}
}

// TestCheckCreateDatabaseAdmissionPassesCoordThroughAtPreExecute proves the
// CoordClient the installed checker receives, when reached through
// createDatabaseTask.PreExecute, forwards to the real mixCoord. This is the
// production-path replacement for the coverage a standalone
// checkCreateDatabaseAdmission wrapper used to provide before task_database.go
// started calling admissionChecker() and CheckCreateDatabase directly.
func TestCheckCreateDatabaseAdmissionPassesCoordThroughAtPreExecute(t *testing.T) {
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	oldCache := globalMetaCache
	t.Cleanup(func() { globalMetaCache = oldCache })

	dbName := "brand_new_db_passthrough"
	cache := NewMockCache(t)
	cache.On("HasDatabase", mock.Anything, dbName).Return(false)
	globalMetaCache = cache

	checker := &recordingAdmissionChecker{}
	assert.NoError(t, extension.SetProvider(testProvider{caps: extension.Capabilities{Admission: checker}}))

	task := newAdmissionTestCreateDatabaseTask(t, dbName)
	mockCoord := task.mixCoord.(*mocks.MockMixCoordClient)
	wantResp := &milvuspb.ShowCollectionsResponse{CollectionNames: []string{"probe-coll"}}
	mockCoord.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(wantResp, nil).Once()

	err := task.PreExecute(task.ctx)
	assert.NoError(t, err)

	if assert.NotNil(t, checker.coordSeen, "the checker must receive a non-nil CoordClient") {
		gotResp, err := checker.coordSeen.ShowCollections(context.Background(), &milvuspb.ShowCollectionsRequest{})
		assert.NoError(t, err)
		assert.Same(t, wantResp, gotResp,
			"the CoordClient handed to the checker via task_database.go must forward calls to the underlying mixCoord")
	}
}
