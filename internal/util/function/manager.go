package function

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/bm25"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var (
	defaultFunctionRunnerManager FunctionRunnerManager = NewFunctionRunnerManager()
	functionRunnerSingleflight   conc.Singleflight[struct{}]
)

var errFunctionRunnerEntryRemoved = errors.New("function runner manager entry was removed")

// LatestFunctionRunnerVersion tells the manager to use the latest version
// snapshot already allocated for the collection. It must not be zero because
// schema version 0 is valid for compatibility paths.
const LatestFunctionRunnerVersion int32 = -1

type functionRunnerState int

const (
	functionRunnerStateInitializing functionRunnerState = iota
	functionRunnerStateReady
	functionRunnerStateFailed
)

type FunctionRunnerManager interface {
	// Alloc records that a lifecycle key is using the schema version
	// and asynchronously tries to initialize function runners for that version.
	// The key identifies an independent lifecycle scope; for example, WAL uses
	// "WAL-"+vchannel while delegator uses "DELEGATOR-"+vchannel.
	// Initialization failures are logged and retried by later Alloc, Update, or
	// Materialize calls instead of failing collection recovery.
	Alloc(collectionID int64, key string, schema *schemapb.CollectionSchema) error

	// Update moves a lifecycle key to a newer schema version and asynchronously
	// initializes any missing function runners required by that version. If the
	// schema no longer has runner-backed functions, Update releases the key.
	// Runners for older versions are kept until no key uses those versions.
	Update(collectionID int64, key string, schema *schemapb.CollectionSchema) error

	// Release removes one lifecycle key. The collection entry and its runners
	// are closed only after all keys are released.
	Release(collectionID int64, key string)

	// EnsureReady synchronously initializes every runner of the schema's
	// version and returns the first initialization error. Complements
	// Alloc/Update (whose initialization is asynchronous and best-effort):
	// callers that need runner readiness as a hard precondition — e.g. the
	// delegator before publishing a ready schema snapshot — call this after
	// Alloc/Update. A schema without runner-backed functions is trivially ready.
	EnsureReady(ctx context.Context, collectionID int64, schema *schemapb.CollectionSchema) error

	// Materialize fills missing function output fields for an insert request.
	// Passing a nil schema uses the latest version snapshot already allocated by
	// Create, recovery, or update; any foreground runner initialization uses the
	// schema saved in that entry. Passing a non-nil schema can initialize the
	// matching version. Initialization or execution failures are returned to the
	// caller.
	Materialize(ctx context.Context, collectionID int64, schema *schemapb.CollectionSchema, body *msgpb.InsertRequest) (bool, error)

	// TryMaterialize is used by compatibility paths for old insert messages. It
	// only uses already cached runners for the given schema version, or the
	// latest allocated version when schemaVersion is LatestFunctionRunnerVersion.
	// It returns ok=false when the caller should build short-lived runners instead.
	TryMaterialize(collectionID int64, schemaVersion int32, body *msgpb.InsertRequest) (bool, bool, error)

	// RunWithRunner runs the callback with the runner that owns the output field.
	// schemaVersion can be LatestFunctionRunnerVersion to use the latest allocated
	// version. The callback is executed synchronously while the manager protects
	// the runner from concurrent close; callers must not retain the runner after
	// the callback.
	RunWithRunner(ctx context.Context, collectionID int64, schemaVersion int32, outputFieldID int64, run func(schemapb.FunctionType, FunctionRunner) error) (bool, error)

	// RunWithAnalyzer runs the callback with the analyzer service associated with
	// a function input field. schemaVersion can be LatestFunctionRunnerVersion to
	// use the latest allocated version. BM25 runners can serve analyzer requests.
	// The callback is protected from concurrent close and must not retain the analyzer.
	RunWithAnalyzer(ctx context.Context, collectionID int64, schemaVersion int32, fieldID int64, run func(Analyzer) error) (bool, error)

	// Close releases all cached runners managed by this manager.
	Close()
}

type functionRunnerManager struct {
	mu      sync.RWMutex
	entries map[int64]*functionRunnerCollectionEntry
}

// Lock order is functionRunnerManager.mu -> functionRunnerCollectionEntry.mu ->
// functionRunnerEntry.mu. The manager lock serializes get-or-create with
// zero-key removal so a newly allocated lifecycle key cannot reuse an entry that
// is already being removed. Runner Close calls are always done after releasing
// the manager/collection locks, so slow runner teardown does not block collection
// map operations.
type functionRunnerCollectionEntry struct {
	mu             sync.RWMutex
	collectionID   int64
	keyVersions    map[string]int32
	versionRunners map[int32]*functionRunnerVersion
	runners        map[string]*functionRunnerEntry
}

type functionRunnerVersion struct {
	signatures              []string
	outputFieldIDs          []int64
	outputFieldSignatures   map[int64]string
	outputFieldTypes        map[int64]schemapb.FunctionType
	analyzerFieldSignatures map[int64]string
}

type functionRunnerEntry struct {
	mu           sync.RWMutex
	collectionID int64
	signature    string
	schema       *schemapb.CollectionSchema
	function     *schemapb.FunctionSchema
	runner       FunctionRunner
	state        functionRunnerState
	initID       uint64
	closed       bool
}

func newFunctionRunnerCollectionEntry(collectionID int64) *functionRunnerCollectionEntry {
	return &functionRunnerCollectionEntry{
		collectionID:   collectionID,
		keyVersions:    make(map[string]int32),
		versionRunners: make(map[int32]*functionRunnerVersion),
		runners:        make(map[string]*functionRunnerEntry),
	}
}

func (e *functionRunnerCollectionEntry) Alloc(key string, schema *schemapb.CollectionSchema) error {
	return e.allocOrUpdate(key, schema, "initialize")
}

func (e *functionRunnerCollectionEntry) Update(key string, schema *schemapb.CollectionSchema) error {
	return e.allocOrUpdate(key, schema, "update")
}

func (e *functionRunnerCollectionEntry) allocOrUpdate(
	key string,
	schema *schemapb.CollectionSchema,
	operation string,
) error {
	schemaVersion := LatestFunctionRunnerVersion
	if schema != nil {
		schemaVersion = schema.GetVersion()
	}
	warnInitFailure := func(err error) {
		mlog.Warn(context.TODO(), "failed to initialize function runners, will retry on next request",
			mlog.String("operation", operation),
			mlog.Int64("collectionID", e.collectionID),
			mlog.String("key", key),
			mlog.Int32("schemaVersion", schemaVersion),
			mlog.Err(err))
	}
	runnerEntries, err := e.ensureVersion(key, schema)
	if err != nil {
		return err
	}
	if len(runnerEntries) == 0 {
		return nil
	}
	go func() {
		for _, runnerEntry := range runnerEntries {
			if err := runnerEntry.ensureReady(context.Background()); err != nil {
				if !errors.Is(err, errFunctionRunnerEntryRemoved) {
					warnInitFailure(err)
				}
				break
			}
		}
	}()
	return nil
}

func (e *functionRunnerCollectionEntry) Release(key string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.keyVersions[key]; !ok {
		return false
	}
	delete(e.keyVersions, key)
	return len(e.keyVersions) == 0
}

func (e *functionRunnerCollectionEntry) Close() {
	e.mu.Lock()
	e.keyVersions = make(map[string]int32)
	runnerEntries := e.gcLocked()
	e.mu.Unlock()

	closeFunctionRunnerEntries(runnerEntries)
}

func (e *functionRunnerCollectionEntry) GC() {
	e.mu.Lock()
	runnerEntries := e.gcLocked()
	e.mu.Unlock()

	closeFunctionRunnerEntries(runnerEntries)
}

func (e *functionRunnerCollectionEntry) gcLocked() []*functionRunnerEntry {
	if len(e.keyVersions) == 0 {
		runnerEntries := make([]*functionRunnerEntry, 0, len(e.runners))
		for _, runnerEntry := range e.runners {
			runnerEntries = append(runnerEntries, runnerEntry)
		}
		e.keyVersions = make(map[string]int32)
		e.versionRunners = make(map[int32]*functionRunnerVersion)
		e.runners = make(map[string]*functionRunnerEntry)
		return runnerEntries
	}

	activeVersions := make(map[int32]struct{}, len(e.keyVersions))
	for _, version := range e.keyVersions {
		activeVersions[version] = struct{}{}
	}
	for version := range e.versionRunners {
		if _, ok := activeVersions[version]; !ok {
			delete(e.versionRunners, version)
		}
	}

	usedSignatures := make(map[string]struct{})
	for _, versionRunners := range e.versionRunners {
		for _, signature := range versionRunners.signatures {
			usedSignatures[signature] = struct{}{}
		}
	}

	runnerEntries := make([]*functionRunnerEntry, 0)
	for signature, runnerEntry := range e.runners {
		if _, ok := usedSignatures[signature]; ok {
			continue
		}
		delete(e.runners, signature)
		runnerEntries = append(runnerEntries, runnerEntry)
	}
	return runnerEntries
}

func useLatestFunctionRunnerVersion(schemaVersion int32) bool {
	return schemaVersion == LatestFunctionRunnerVersion
}

func (e *functionRunnerCollectionEntry) getVersionRunnerLocked(schemaVersion int32) (*functionRunnerVersion, bool) {
	if !useLatestFunctionRunnerVersion(schemaVersion) {
		versionRunners, ok := e.versionRunners[schemaVersion]
		return versionRunners, ok
	}

	if len(e.versionRunners) == 0 {
		return nil, false
	}
	var latestVersion int32
	first := true
	for version := range e.versionRunners {
		if first || version > latestVersion {
			latestVersion = version
			first = false
		}
	}
	return e.versionRunners[latestVersion], true
}

func (e *functionRunnerCollectionEntry) getVersionRunnerEntriesLocked(schemaVersion int32) ([]*functionRunnerEntry, []int64, bool) {
	versionRunners, ok := e.getVersionRunnerLocked(schemaVersion)
	if !ok {
		return nil, nil, false
	}
	runnerEntries := make([]*functionRunnerEntry, 0, len(versionRunners.signatures))
	for _, signature := range versionRunners.signatures {
		runnerEntry := e.runners[signature]
		if runnerEntry == nil {
			return nil, nil, false
		}
		runnerEntries = append(runnerEntries, runnerEntry)
	}
	return runnerEntries, append([]int64(nil), versionRunners.outputFieldIDs...), true
}

func (e *functionRunnerCollectionEntry) getVersionRunnerEntries(schemaVersion int32) ([]*functionRunnerEntry, []int64, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.getVersionRunnerEntriesLocked(schemaVersion)
}

func runWithRunnerEntries(
	ctx context.Context,
	runnerEntries []*functionRunnerEntry,
	initRunner bool,
	run func([]FunctionRunner) error,
) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if initRunner {
		for _, runnerEntry := range runnerEntries {
			if err := runnerEntry.ensureReady(ctx); err != nil {
				return false, err
			}
		}
	}

	// Hold runner entry read locks while the callback runs to prevent concurrent
	// close. The read locks are shared, so concurrent materialization is still
	// allowed; concrete runners protect their own mutable state.
	runners := make([]FunctionRunner, 0, len(runnerEntries))
	for _, runnerEntry := range runnerEntries {
		runner, unlock, ok, err := runnerEntry.lockRunner(initRunner)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
		runners = append(runners, runner)
		defer unlock()
	}

	return true, run(runners)
}

func newFunctionRunnerEntry(
	collectionID int64,
	signature string,
	schema *schemapb.CollectionSchema,
	fn *schemapb.FunctionSchema,
) *functionRunnerEntry {
	return &functionRunnerEntry{
		collectionID: collectionID,
		signature:    signature,
		schema:       proto.Clone(schema).(*schemapb.CollectionSchema),
		function:     proto.Clone(fn).(*schemapb.FunctionSchema),
		state:        functionRunnerStateFailed,
	}
}

func (e *functionRunnerEntry) IsReady() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.state == functionRunnerStateReady && e.runner != nil
}

func (e *functionRunnerEntry) ensureReady(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if e.IsReady() {
		return nil
	}

	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return errFunctionRunnerEntryRemoved
	}
	if e.state == functionRunnerStateReady && e.runner != nil {
		e.mu.Unlock()
		return nil
	}
	if e.state != functionRunnerStateInitializing {
		e.initID++
		e.state = functionRunnerStateInitializing
	}
	initID := e.initID
	e.mu.Unlock()

	resultCh := functionRunnerSingleflight.DoChan(e.singleflightKey(), func() (struct{}, error) {
		return struct{}{}, e.init(initID)
	})
	select {
	case result := <-resultCh:
		if result.Err != nil {
			return result.Err
		}
	case <-ctx.Done():
		return ctx.Err()
	}

	if !e.IsReady() {
		return errFunctionRunnerEntryRemoved
	}
	return nil
}

func (e *functionRunnerEntry) run(ctx context.Context, run func(FunctionRunner) error) error {
	if err := e.ensureReady(ctx); err != nil {
		return err
	}

	runner, unlock, _, err := e.lockRunner(true)
	if err != nil {
		return err
	}
	defer unlock()

	return run(runner)
}

func (e *functionRunnerEntry) lockRunner(initRunner bool) (FunctionRunner, func(), bool, error) {
	e.mu.RLock()

	if e.closed || e.state != functionRunnerStateReady || e.runner == nil {
		e.mu.RUnlock()
		if initRunner {
			return nil, nil, false, errFunctionRunnerEntryRemoved
		}
		return nil, nil, false, nil
	}
	return e.runner, e.mu.RUnlock, true, nil
}

func (e *functionRunnerEntry) init(initID uint64) error {
	if e.IsReady() {
		return nil
	}

	runner, err := BuildEmbeddingRunner(e.schema, e.function)
	if err == nil && runner == nil {
		err = errors.New("function runner is nil")
	}

	e.mu.Lock()
	if e.closed || e.initID != initID || e.state != functionRunnerStateInitializing {
		e.mu.Unlock()
		if runner != nil {
			runner.Close()
		}
		return errFunctionRunnerEntryRemoved
	}

	var oldRunner FunctionRunner
	if err != nil {
		e.state = functionRunnerStateFailed
	} else {
		oldRunner = e.runner
		e.runner = runner
		e.state = functionRunnerStateReady
	}
	e.mu.Unlock()

	if oldRunner != nil {
		oldRunner.Close()
	}
	if err != nil && runner != nil {
		runner.Close()
	}
	return err
}

func (e *functionRunnerEntry) singleflightKey() string {
	return strconv.FormatInt(e.collectionID, 10) + "/" + e.signature
}

func (e *functionRunnerEntry) Close() {
	e.mu.Lock()
	runner := e.runner
	e.runner = nil
	e.state = functionRunnerStateFailed
	e.closed = true
	e.mu.Unlock()

	if runner != nil {
		runner.Close()
	}
}

func NewFunctionRunnerManager() FunctionRunnerManager {
	return newFunctionRunnerManager()
}

func newFunctionRunnerManager() *functionRunnerManager {
	return &functionRunnerManager{
		entries: make(map[int64]*functionRunnerCollectionEntry),
	}
}

func (m *functionRunnerManager) Alloc(
	collectionID int64,
	key string,
	schema *schemapb.CollectionSchema,
) error {
	if key == "" {
		return merr.WrapErrFunctionFailedMsg("function runner key is empty")
	}
	if schema == nil {
		return merr.WrapErrFunctionFailedMsg("collection schema is nil")
	}
	if !HasEmbeddingFunctions(schema) {
		return nil
	}
	return m.getOrCreateEntry(collectionID).Alloc(key, schema)
}

func (m *functionRunnerManager) Update(
	collectionID int64,
	key string,
	schema *schemapb.CollectionSchema,
) error {
	if key == "" {
		return merr.WrapErrFunctionFailedMsg("function runner key is empty")
	}
	if schema == nil {
		return merr.WrapErrFunctionFailedMsg("collection schema is nil")
	}
	if !HasEmbeddingFunctions(schema) {
		m.Release(collectionID, key)
		return nil
	}
	return m.getOrCreateEntry(collectionID).Update(key, schema)
}

// EnsureReady synchronously initializes the runners of schema's version, see
// functionRunnerCollectionEntry.EnsureReady. A schema without embedding
// functions is trivially ready.
func (m *functionRunnerManager) EnsureReady(
	ctx context.Context,
	collectionID int64,
	schema *schemapb.CollectionSchema,
) error {
	if schema == nil {
		return merr.WrapErrFunctionFailedMsg("collection schema is nil")
	}
	if !HasEmbeddingFunctions(schema) {
		return nil
	}
	entry := m.getEntry(collectionID)
	if entry == nil {
		return merr.WrapErrFunctionFailedMsg("function runners for collection %d are not allocated", collectionID)
	}
	return entry.EnsureReady(ctx, schema.GetVersion())
}

func (e *functionRunnerCollectionEntry) ensureVersion(
	key string,
	schema *schemapb.CollectionSchema,
) ([]*functionRunnerEntry, error) {
	if schema == nil {
		return nil, merr.WrapErrFunctionFailedMsg("collection schema is nil")
	}

	schemaVersion := schema.GetVersion()
	e.mu.Lock()
	e.updateKeyVersionLocked(key, schemaVersion)

	versionRunners, ok := e.versionRunners[schemaVersion]
	if ok {
		runnerEntries := make([]*functionRunnerEntry, 0, len(versionRunners.signatures))
		missingEntry := false
		for _, signature := range versionRunners.signatures {
			runnerEntry := e.runners[signature]
			if runnerEntry == nil {
				missingEntry = true
				break
			}
			if !runnerEntry.IsReady() {
				runnerEntries = append(runnerEntries, runnerEntry)
			}
		}
		if !missingEntry {
			closedRunnerEntries := e.gcLocked()
			e.mu.Unlock()

			closeFunctionRunnerEntries(closedRunnerEntries)
			return runnerEntries, nil
		}
	}
	e.mu.Unlock()

	functions := embeddingFunctions(schema)
	functionsBySignature := make(map[string]*schemapb.FunctionSchema, len(functions))
	signatures := make([]string, 0, len(functions))
	outputFieldIDs := make([]int64, 0, len(functions))
	outputFieldSignatures := make(map[int64]string)
	outputFieldTypes := make(map[int64]schemapb.FunctionType)
	analyzerFieldSignatures := make(map[int64]string)
	for _, fn := range functions {
		functionOutputFieldIDs, err := functionOutputFieldIDs(schema, fn)
		if err != nil {
			return nil, err
		}
		inputFieldIDs, err := functionInputFieldIDs(schema, fn)
		if err != nil {
			return nil, err
		}
		signature, err := embeddingFunctionSignature(schema, fn)
		if err != nil {
			return nil, err
		}
		signatures = append(signatures, signature)
		outputFieldIDs = append(outputFieldIDs, functionOutputFieldIDs...)
		functionsBySignature[signature] = fn
		for _, outputFieldID := range functionOutputFieldIDs {
			outputFieldSignatures[outputFieldID] = signature
			outputFieldTypes[outputFieldID] = fn.GetType()
		}
		if fn.GetType() == schemapb.FunctionType_BM25 && len(inputFieldIDs) > 0 {
			if _, ok := analyzerFieldSignatures[inputFieldIDs[0]]; !ok {
				analyzerFieldSignatures[inputFieldIDs[0]] = signature
			}
		}
	}

	e.mu.Lock()
	e.updateKeyVersionLocked(key, schemaVersion)

	versionRunners, ok = e.versionRunners[schemaVersion]
	if !ok {
		versionRunners = &functionRunnerVersion{
			signatures:              append([]string(nil), signatures...),
			outputFieldIDs:          append([]int64(nil), outputFieldIDs...),
			outputFieldSignatures:   cloneMap(outputFieldSignatures),
			outputFieldTypes:        cloneMap(outputFieldTypes),
			analyzerFieldSignatures: cloneMap(analyzerFieldSignatures),
		}
		for _, signature := range signatures {
			if e.runners[signature] == nil {
				e.runners[signature] = newFunctionRunnerEntry(
					e.collectionID,
					signature,
					schema,
					functionsBySignature[signature],
				)
			}
		}
		e.versionRunners[schemaVersion] = versionRunners
	} else {
		for _, signature := range versionRunners.signatures {
			if _, functionOK := functionsBySignature[signature]; !functionOK {
				e.mu.Unlock()
				return nil, merr.WrapErrFunctionFailedMsg("function runner signature %s not found in schema version %d", signature, schemaVersion)
			}
			if e.runners[signature] == nil {
				e.runners[signature] = newFunctionRunnerEntry(
					e.collectionID,
					signature,
					schema,
					functionsBySignature[signature],
				)
			}
		}
	}

	initRunnerEntries := make([]*functionRunnerEntry, 0, len(versionRunners.signatures))
	for _, signature := range versionRunners.signatures {
		runnerEntry := e.runners[signature]
		if runnerEntry == nil || runnerEntry.IsReady() {
			continue
		}
		initRunnerEntries = append(initRunnerEntries, runnerEntry)
	}
	runnerEntries := e.gcLocked()
	e.mu.Unlock()

	closeFunctionRunnerEntries(runnerEntries)
	return initRunnerEntries, nil
}

func (e *functionRunnerCollectionEntry) updateKeyVersionLocked(
	key string,
	schemaVersion int32,
) {
	if key == "" {
		return
	}

	version, ok := e.keyVersions[key]
	if !ok {
		e.keyVersions[key] = schemaVersion
		return
	}

	if version <= schemaVersion {
		e.keyVersions[key] = schemaVersion
	}
}

// EnsureReady synchronously initializes every runner of the given schema
// version and returns the first initialization error. Unlike allocOrUpdate's
// background warm-up goroutine, this gives callers (the delegator ready-schema
// publish gate) a hard guarantee: when it returns nil, every runner of the
// version serves requests without lazy read-path initialization.
func (e *functionRunnerCollectionEntry) EnsureReady(ctx context.Context, schemaVersion int32) error {
	e.mu.RLock()
	versionRunners, ok := e.getVersionRunnerLocked(schemaVersion)
	if !ok {
		e.mu.RUnlock()
		return merr.WrapErrFunctionFailedMsg("function runners for schema version %d are not allocated", schemaVersion)
	}
	runnerEntries := make([]*functionRunnerEntry, 0, len(versionRunners.signatures))
	for _, signature := range versionRunners.signatures {
		runnerEntry := e.runners[signature]
		if runnerEntry == nil {
			e.mu.RUnlock()
			return merr.WrapErrFunctionFailedMsg("function runner %s for schema version %d is not allocated", signature, schemaVersion)
		}
		runnerEntries = append(runnerEntries, runnerEntry)
	}
	e.mu.RUnlock()

	for _, runnerEntry := range runnerEntries {
		if err := runnerEntry.ensureReady(ctx); err != nil {
			return merr.Wrapf(err, "failed to initialize function runner %s for schema version %d", runnerEntry.signature, schemaVersion)
		}
	}
	return nil
}

func (e *functionRunnerCollectionEntry) getRunnerEntryByOutputField(schemaVersion int32, outputFieldID int64) (*functionRunnerEntry, schemapb.FunctionType, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	versionRunners, ok := e.getVersionRunnerLocked(schemaVersion)
	if !ok {
		return nil, schemapb.FunctionType_Unknown, false
	}
	signature, ok := versionRunners.outputFieldSignatures[outputFieldID]
	if !ok {
		return nil, schemapb.FunctionType_Unknown, false
	}
	runnerEntry := e.runners[signature]
	if runnerEntry == nil {
		return nil, schemapb.FunctionType_Unknown, false
	}
	return runnerEntry, versionRunners.outputFieldTypes[outputFieldID], true
}

func (e *functionRunnerCollectionEntry) getRunnerEntryByAnalyzerField(schemaVersion int32, fieldID int64) (*functionRunnerEntry, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	versionRunners, ok := e.getVersionRunnerLocked(schemaVersion)
	if !ok {
		return nil, false
	}
	signature, ok := versionRunners.analyzerFieldSignatures[fieldID]
	if !ok {
		return nil, false
	}
	runnerEntry := e.runners[signature]
	if runnerEntry == nil {
		return nil, false
	}
	return runnerEntry, true
}

func (e *functionRunnerCollectionEntry) Materialize(
	ctx context.Context,
	schema *schemapb.CollectionSchema,
	body *msgpb.InsertRequest,
) (bool, error) {
	if body == nil {
		return false, merr.WrapErrFunctionFailedMsg("insert request is nil")
	}

	var runnerEntries []*functionRunnerEntry
	var outputFieldIDs []int64
	var ok bool
	if schema != nil {
		if _, err := e.ensureVersion("", schema); err != nil {
			return false, err
		}
		runnerEntries, outputFieldIDs, ok = e.getVersionRunnerEntries(schema.GetVersion())
	} else {
		runnerEntries, outputFieldIDs, ok = e.getVersionRunnerEntries(LatestFunctionRunnerVersion)
	}
	if !ok {
		return false, nil
	}
	if len(outputFieldIDs) == 0 || HasAllFieldDataByID(body.GetFieldsData(), outputFieldIDs) {
		return false, nil
	}

	changed := false
	ok, err := runWithRunnerEntries(ctx, runnerEntries, true, func(runners []FunctionRunner) error {
		var runErr error
		changed, runErr = FillFunctionFields(runners, body)
		return runErr
	})
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	return changed, nil
}

// TryMaterialize is only used by compatibility logic to try materializing old
// insert messages with cached runners.
func (e *functionRunnerCollectionEntry) TryMaterialize(
	schemaVersion int32,
	body *msgpb.InsertRequest,
) (bool, bool, error) {
	if body == nil {
		return false, false, merr.WrapErrFunctionFailedMsg("insert request is nil")
	}

	runnerEntries, outputFieldIDs, ok := e.getVersionRunnerEntries(schemaVersion)
	if !ok {
		return false, false, nil
	}
	if len(outputFieldIDs) == 0 || HasAllFieldDataByID(body.GetFieldsData(), outputFieldIDs) {
		return false, true, nil
	}

	changed := false
	ok, err := runWithRunnerEntries(context.Background(), runnerEntries, false, func(runners []FunctionRunner) error {
		var runErr error
		changed, runErr = FillFunctionFields(runners, body)
		return runErr
	})
	if err != nil {
		return false, true, err
	}
	return changed, ok, nil
}

func (e *functionRunnerCollectionEntry) RunWithRunner(
	ctx context.Context,
	schemaVersion int32,
	outputFieldID int64,
	run func(schemapb.FunctionType, FunctionRunner) error,
) (bool, error) {
	runnerEntry, functionType, ok := e.getRunnerEntryByOutputField(schemaVersion, outputFieldID)
	if !ok {
		return false, nil
	}

	err := runnerEntry.run(ctx, func(runner FunctionRunner) error {
		return run(functionType, runner)
	})
	return true, err
}

func (e *functionRunnerCollectionEntry) RunWithAnalyzer(
	ctx context.Context,
	schemaVersion int32,
	fieldID int64,
	run func(Analyzer) error,
) (bool, error) {
	runnerEntry, ok := e.getRunnerEntryByAnalyzerField(schemaVersion, fieldID)
	if !ok {
		return false, nil
	}
	err := runnerEntry.run(ctx, func(runner FunctionRunner) error {
		analyzer, ok := runner.(Analyzer)
		if !ok {
			return merr.WrapErrFunctionFailedMsg("function runner cannot serve analyzer requests")
		}
		return run(analyzer)
	})
	return true, err
}

func (m *functionRunnerManager) getOrCreateEntry(collectionID int64) *functionRunnerCollectionEntry {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry := m.entries[collectionID]
	if entry == nil {
		entry = newFunctionRunnerCollectionEntry(collectionID)
		m.entries[collectionID] = entry
	}
	return entry
}

func (m *functionRunnerManager) getEntry(collectionID int64) *functionRunnerCollectionEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.entries[collectionID]
}

func (m *functionRunnerManager) Release(collectionID int64, key string) {
	if key == "" {
		return
	}

	m.mu.Lock()
	entry := m.entries[collectionID]
	if entry == nil {
		m.mu.Unlock()
		return
	}
	remove := entry.Release(key)
	if remove {
		delete(m.entries, collectionID)
	}
	m.mu.Unlock()

	if remove {
		entry.Close()
		return
	}
	entry.GC()
}

func (m *functionRunnerManager) Materialize(
	ctx context.Context,
	collectionID int64,
	schema *schemapb.CollectionSchema,
	body *msgpb.InsertRequest,
) (bool, error) {
	if schema != nil && !HasEmbeddingFunctions(schema) {
		return false, nil
	}
	entry := m.getEntry(collectionID)
	if entry == nil {
		if schema == nil {
			return false, nil
		}
		return false, merr.WrapErrFunctionFailedMsg("function runners for collection %d are not allocated", collectionID)
	}
	return entry.Materialize(ctx, schema, body)
}

func (m *functionRunnerManager) TryMaterialize(
	collectionID int64,
	schemaVersion int32,
	body *msgpb.InsertRequest,
) (bool, bool, error) {
	entry := m.getEntry(collectionID)
	if entry == nil {
		return false, false, nil
	}
	return entry.TryMaterialize(schemaVersion, body)
}

func (m *functionRunnerManager) RunWithRunner(
	ctx context.Context,
	collectionID int64,
	schemaVersion int32,
	outputFieldID int64,
	run func(schemapb.FunctionType, FunctionRunner) error,
) (bool, error) {
	entry := m.getEntry(collectionID)
	if entry == nil {
		return false, nil
	}
	return entry.RunWithRunner(ctx, schemaVersion, outputFieldID, run)
}

func (m *functionRunnerManager) RunWithAnalyzer(
	ctx context.Context,
	collectionID int64,
	schemaVersion int32,
	fieldID int64,
	run func(Analyzer) error,
) (bool, error) {
	entry := m.getEntry(collectionID)
	if entry == nil {
		return false, nil
	}
	return entry.RunWithAnalyzer(ctx, schemaVersion, fieldID, run)
}

func (m *functionRunnerManager) Close() {
	m.mu.Lock()
	entries := make([]*functionRunnerCollectionEntry, 0, len(m.entries))
	for collectionID, entry := range m.entries {
		entries = append(entries, entry)
		delete(m.entries, collectionID)
	}
	m.mu.Unlock()

	for _, entry := range entries {
		entry.Close()
	}
}

func BuildEmbeddingRunner(schema *schemapb.CollectionSchema, fn *schemapb.FunctionSchema) (FunctionRunner, error) {
	if schema == nil {
		return nil, merr.WrapErrFunctionFailedMsg("collection schema is nil")
	}
	if fn == nil {
		return nil, merr.WrapErrFunctionFailedMsg("function schema is nil")
	}
	if !IsEmbeddingFunctionType(fn.GetType()) {
		return nil, nil
	}

	schema = proto.Clone(schema).(*schemapb.CollectionSchema)
	fn = proto.Clone(fn).(*schemapb.FunctionSchema)
	return NewFunctionRunner(schema, fn)
}

func BuildEmbeddingRunners(schema *schemapb.CollectionSchema) ([]FunctionRunner, error) {
	if schema == nil {
		return nil, merr.WrapErrFunctionFailedMsg("collection schema is nil")
	}
	if !HasEmbeddingFunctions(schema) {
		return nil, nil
	}

	functions := embeddingFunctions(schema)
	runners := make([]FunctionRunner, 0, len(functions))
	for _, fn := range functions {
		runner, err := BuildEmbeddingRunner(schema, fn)
		if err != nil {
			CloseRunners(runners)
			return nil, err
		}
		if runner != nil {
			runners = append(runners, runner)
		}
	}
	return runners, nil
}

func EmbeddingOutputFieldIDs(schema *schemapb.CollectionSchema) ([]int64, error) {
	if schema == nil {
		return nil, merr.WrapErrFunctionFailedMsg("collection schema is nil")
	}
	if !HasEmbeddingFunctions(schema) {
		return nil, nil
	}

	functions := embeddingFunctions(schema)
	outputFieldIDs := make([]int64, 0, len(functions))
	for _, fn := range functions {
		functionOutputFieldIDs, err := functionOutputFieldIDs(schema, fn)
		if err != nil {
			return nil, err
		}
		outputFieldIDs = append(outputFieldIDs, functionOutputFieldIDs...)
	}
	return outputFieldIDs, nil
}

func EmbeddingFunctionSignature(schema *schemapb.CollectionSchema) (string, error) {
	if schema == nil {
		return "", merr.WrapErrFunctionFailedMsg("collection schema is nil")
	}

	hasher := sha256.New()
	for _, fn := range embeddingFunctions(schema) {
		signature, err := embeddingFunctionSignature(schema, fn)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(hasher, "runner:%s|", signature)
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

// HasEmbeddingFunctions reports whether the schema has functions backed by FunctionRunner.
func HasEmbeddingFunctions(schema *schemapb.CollectionSchema) bool {
	if schema == nil {
		return false
	}
	for _, fn := range schema.GetFunctions() {
		if IsEmbeddingFunctionType(fn.GetType()) {
			return true
		}
	}
	return false
}

func embeddingFunctions(schema *schemapb.CollectionSchema) []*schemapb.FunctionSchema {
	if schema == nil {
		return nil
	}

	functions := lo.Filter(schema.GetFunctions(), func(fn *schemapb.FunctionSchema, _ int) bool {
		return IsEmbeddingFunctionType(fn.GetType())
	})
	sort.Slice(functions, func(i, j int) bool {
		if functions[i].GetId() != functions[j].GetId() {
			return functions[i].GetId() < functions[j].GetId()
		}
		if functions[i].GetName() != functions[j].GetName() {
			return functions[i].GetName() < functions[j].GetName()
		}
		return functions[i].GetType() < functions[j].GetType()
	})
	return functions
}

func functionOutputFieldIDs(schema *schemapb.CollectionSchema, fn *schemapb.FunctionSchema) ([]int64, error) {
	if outputIDs := fn.GetOutputFieldIds(); len(outputIDs) > 0 {
		outputFieldIDs := append([]int64(nil), outputIDs...)
		for _, outputFieldID := range outputFieldIDs {
			if typeutil.GetField(schema, outputFieldID) == nil {
				return nil, merr.WrapErrFunctionFailedMsg("function %s output field %d not found", fn.GetName(), outputFieldID)
			}
		}
		return outputFieldIDs, nil
	}

	outputNames := fn.GetOutputFieldNames()
	if len(outputNames) == 0 {
		return nil, merr.WrapErrFunctionFailedMsg("function %s output fields not found", fn.GetName())
	}

	outputFieldIDs := make([]int64, 0, len(outputNames))
	for _, outputName := range outputNames {
		field := typeutil.GetFieldByName(schema, outputName)
		if field == nil {
			return nil, merr.WrapErrFunctionFailedMsg("function %s output field %s not found", fn.GetName(), outputName)
		}
		outputFieldIDs = append(outputFieldIDs, field.GetFieldID())
	}
	return outputFieldIDs, nil
}

func functionInputFieldIDs(schema *schemapb.CollectionSchema, fn *schemapb.FunctionSchema) ([]int64, error) {
	if inputIDs := fn.GetInputFieldIds(); len(inputIDs) > 0 {
		inputFieldIDs := append([]int64(nil), inputIDs...)
		for _, inputFieldID := range inputFieldIDs {
			if typeutil.GetField(schema, inputFieldID) == nil {
				return nil, merr.WrapErrFunctionFailedMsg("function %s input field %d not found", fn.GetName(), inputFieldID)
			}
		}
		return inputFieldIDs, nil
	}

	inputNames := fn.GetInputFieldNames()
	if len(inputNames) == 0 {
		return nil, merr.WrapErrFunctionFailedMsg("function %s input fields not found", fn.GetName())
	}

	inputFieldIDs := make([]int64, 0, len(inputNames))
	for _, inputName := range inputNames {
		field := typeutil.GetFieldByName(schema, inputName)
		if field == nil {
			return nil, merr.WrapErrFunctionFailedMsg("function %s input field %s not found", fn.GetName(), inputName)
		}
		inputFieldIDs = append(inputFieldIDs, field.GetFieldID())
	}
	return inputFieldIDs, nil
}

func embeddingFunctionSignature(schema *schemapb.CollectionSchema, fn *schemapb.FunctionSchema) (string, error) {
	hasher := sha256.New()
	fmt.Fprintf(hasher, "fn:%d:%d:%s|", fn.GetId(), fn.GetType(), fn.GetName())
	writeInt64s(hasher, "input_ids", fn.GetInputFieldIds())
	writeStrings(hasher, "input_names", fn.GetInputFieldNames())
	writeInt64s(hasher, "output_ids", fn.GetOutputFieldIds())
	writeStrings(hasher, "output_names", fn.GetOutputFieldNames())
	writeKeyValuePairs(hasher, "fn_params", fn.GetParams())

	for _, fieldID := range fn.GetInputFieldIds() {
		field := typeutil.GetField(schema, fieldID)
		if field == nil {
			return "", merr.WrapErrFunctionFailedMsg("function %s input field %d not found", fn.GetName(), fieldID)
		}
		writeFieldSignature(hasher, "input", field)
	}
	for _, fieldName := range fn.GetInputFieldNames() {
		field := typeutil.GetFieldByName(schema, fieldName)
		if field == nil {
			return "", merr.WrapErrFunctionFailedMsg("function %s input field %s not found", fn.GetName(), fieldName)
		}
		writeFieldSignature(hasher, "input_name", field)
	}
	for _, fieldID := range fn.GetOutputFieldIds() {
		field := typeutil.GetField(schema, fieldID)
		if field == nil {
			return "", merr.WrapErrFunctionFailedMsg("function %s output field %d not found", fn.GetName(), fieldID)
		}
		writeFieldSignature(hasher, "output", field)
	}
	for _, fieldName := range fn.GetOutputFieldNames() {
		field := typeutil.GetFieldByName(schema, fieldName)
		if field == nil {
			return "", merr.WrapErrFunctionFailedMsg("function %s output field %s not found", fn.GetName(), fieldName)
		}
		writeFieldSignature(hasher, "output_name", field)
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func writeFieldSignature(hasher hashWriter, prefix string, field *schemapb.FieldSchema) {
	fmt.Fprintf(hasher, "%s:%d:%s:%d:%d:%t:%t:%t:%t|",
		prefix,
		field.GetFieldID(),
		field.GetName(),
		field.GetDataType(),
		field.GetElementType(),
		field.GetIsPrimaryKey(),
		field.GetIsPartitionKey(),
		field.GetIsClusteringKey(),
		field.GetIsFunctionOutput())
	writeKeyValuePairs(hasher, prefix+"_type_params", field.GetTypeParams())
}

func writeKeyValuePairs(hasher hashWriter, prefix string, pairs []*commonpb.KeyValuePair) {
	cloned := append([]*commonpb.KeyValuePair(nil), pairs...)
	sort.Slice(cloned, func(i, j int) bool {
		if cloned[i].GetKey() != cloned[j].GetKey() {
			return cloned[i].GetKey() < cloned[j].GetKey()
		}
		return cloned[i].GetValue() < cloned[j].GetValue()
	})
	for _, pair := range cloned {
		fmt.Fprintf(hasher, "%s:%s=%s|", prefix, pair.GetKey(), pair.GetValue())
	}
}

func writeInt64s(hasher hashWriter, prefix string, values []int64) {
	for idx, value := range values {
		fmt.Fprintf(hasher, "%s:%d=%d|", prefix, idx, value)
	}
}

func writeStrings(hasher hashWriter, prefix string, values []string) {
	for idx, value := range values {
		fmt.Fprintf(hasher, "%s:%d=%s|", prefix, idx, value)
	}
}

type hashWriter interface {
	Write([]byte) (int, error)
}

func cloneMap[K comparable, V any](values map[K]V) map[K]V {
	cloned := make(map[K]V, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

// FillFunctionData fills function output fields before appending an insert message to WAL.
// Passing nil schema uses the latest runner snapshot allocated by collection
// create, recovery, or update.
func FillFunctionData(ctx context.Context, collectionID int64, schema *schemapb.CollectionSchema, body *msgpb.InsertRequest) (bool, error) {
	return defaultFunctionRunnerManager.Materialize(ctx, collectionID, schema, body)
}

// TryMaterialize is used by compatibility logic to try materializing old insert
// messages with cache-managed runners. It returns ok=false when a matching cache
// is absent so the caller can build a short-lived runner for that message.
func TryMaterialize(collectionID int64, schemaVersion int32, body *msgpb.InsertRequest) (changed bool, ok bool, err error) {
	return defaultFunctionRunnerManager.TryMaterialize(collectionID, schemaVersion, body)
}

func AllocFunctionRunners(
	collectionID int64,
	key string,
	schema *schemapb.CollectionSchema,
) error {
	return defaultFunctionRunnerManager.Alloc(collectionID, key, schema)
}

func UpdateFunctionRunners(
	collectionID int64,
	key string,
	schema *schemapb.CollectionSchema,
) error {
	return defaultFunctionRunnerManager.Update(collectionID, key, schema)
}

func ReleaseFunctionRunners(collectionID int64, key string) {
	defaultFunctionRunnerManager.Release(collectionID, key)
}

// EnsureRunnersReady synchronously initializes every function runner of the
// schema's version, returning the first initialization error. Callers use it
// to make runner readiness a precondition (e.g. before publishing a ready
// schema snapshot) instead of relying on lazy read-path initialization.
func EnsureRunnersReady(ctx context.Context, collectionID int64, schema *schemapb.CollectionSchema) error {
	return defaultFunctionRunnerManager.EnsureReady(ctx, collectionID, schema)
}

func RunWithRunner(
	ctx context.Context,
	collectionID int64,
	schemaVersion int32,
	outputFieldID int64,
	run func(schemapb.FunctionType, FunctionRunner) error,
) (bool, error) {
	return defaultFunctionRunnerManager.RunWithRunner(ctx, collectionID, schemaVersion, outputFieldID, run)
}

func RunWithAnalyzer(
	ctx context.Context,
	collectionID int64,
	schemaVersion int32,
	fieldID int64,
	run func(Analyzer) error,
) (bool, error) {
	return defaultFunctionRunnerManager.RunWithAnalyzer(ctx, collectionID, schemaVersion, fieldID, run)
}

func FillFunctionFields(runners []FunctionRunner, body *msgpb.InsertRequest) (bool, error) {
	if body == nil {
		return false, merr.WrapErrFunctionFailedMsg("insert request is nil")
	}

	changed := false
	for _, runner := range runners {
		outputFields := runner.GetOutputFields()
		if len(outputFields) != 1 {
			return false, merr.WrapErrFunctionFailedMsg("function should have exactly one output field, got %d", len(outputFields))
		}
		outputField := outputFields[0]
		if HasFieldData(body.GetFieldsData(), outputField.GetFieldID()) {
			continue
		}

		// A write stamped at an older schema version (accepted by the v<=current write
		// gate) can predate this function's input field: the field was added after the
		// write was built, so its data is simply absent. Materializing anyway fails with
		// "input field not found", and because a lagging vchannel that does not yet carry
		// this function accepts the same write, that failure resurfaces a cross-vchannel
		// partial commit. Skip instead -- a row with no input has no function output, so
		// leave the output unset (segcore backfills null; the external backfill later
		// recomputes it, which for an absent/empty input is still empty). This keeps the
		// pre-WAL transform a total function over any at-or-behind schema version.
		if !hasAllInputFields(runner, body) {
			continue
		}

		output, err := RunFunction(runner, body)
		if err != nil {
			return false, err
		}
		body.FieldsData = append(body.FieldsData, output)
		changed = true
	}

	return changed, nil
}

// hasAllInputFields reports whether every input field this runner consumes is present
// in the insert body. A write that predates a function's input field (accepted behind
// the current schema version by the write gate) lacks that field entirely, which is the
// signal FillFunctionFields uses to skip materialization rather than fail the write.
func hasAllInputFields(runner FunctionRunner, body *msgpb.InsertRequest) bool {
	for _, field := range runner.GetInputFields() {
		if GetFieldData(body.GetFieldsData(), field.GetFieldID()) == nil {
			return false
		}
	}
	return true
}

func IsEmbeddingFunctionType(functionType schemapb.FunctionType) bool {
	switch functionType {
	case schemapb.FunctionType_BM25, schemapb.FunctionType_MinHash:
		return true
	default:
		return false
	}
}

func RunFunction(runner FunctionRunner, body *msgpb.InsertRequest) (*schemapb.FieldData, error) {
	inputIDs := lo.Map(runner.GetInputFields(), func(field *schemapb.FieldSchema, _ int) int64 {
		return field.GetFieldID()
	})
	inputData, err := getStringFieldData(body.GetFieldsData(), inputIDs...)
	if err != nil {
		return nil, err
	}
	output, err := runner.BatchRun(inputData...)
	if err != nil {
		return nil, err
	}
	if len(output) == 0 {
		return nil, merr.WrapErrFunctionFailedMsg("function runner returned empty output")
	}

	outputFields := runner.GetOutputFields()
	if len(outputFields) != 1 {
		return nil, merr.WrapErrFunctionFailedMsg("function should have exactly one output field, got %d", len(outputFields))
	}
	outputField := outputFields[0]

	switch runner.GetSchema().GetType() {
	case schemapb.FunctionType_BM25:
		sparseArray, ok := output[0].(*schemapb.SparseFloatArray)
		if !ok {
			return nil, merr.WrapErrFunctionFailedMsg("BM25 runner returned non sparse-float-vector output")
		}
		return bm25.BuildSparseFieldData(outputField, sparseArray), nil
	case schemapb.FunctionType_MinHash:
		fieldData, ok := output[0].(*schemapb.FieldData)
		if !ok {
			return nil, merr.WrapErrFunctionFailedMsg("MinHash runner returned non field-data output")
		}
		fieldData.Type = outputField.GetDataType()
		fieldData.FieldName = outputField.GetName()
		fieldData.FieldId = outputField.GetFieldID()
		return fieldData, nil
	default:
		return nil, merr.WrapErrFunctionFailedMsg("unsupported embedding function type %s", runner.GetSchema().GetType().String())
	}
}

func HasAllFieldDataByID(fieldsData []*schemapb.FieldData, fieldIDs []int64) bool {
	for _, fieldID := range fieldIDs {
		if !HasFieldData(fieldsData, fieldID) {
			return false
		}
	}
	return true
}

func HasFieldData(fieldsData []*schemapb.FieldData, fieldID int64) bool {
	return GetFieldData(fieldsData, fieldID) != nil
}

func GetFieldData(fieldsData []*schemapb.FieldData, fieldID int64) *schemapb.FieldData {
	for _, fieldData := range fieldsData {
		if fieldData.GetFieldId() == fieldID {
			return fieldData
		}
	}
	return nil
}

func CloseRunners(runners []FunctionRunner) {
	for _, runner := range runners {
		if runner != nil {
			runner.Close()
		}
	}
}

func closeFunctionRunnerEntries(entries []*functionRunnerEntry) {
	for _, entry := range entries {
		if entry != nil {
			entry.Close()
		}
	}
}

func getStringFieldData(fieldsData []*schemapb.FieldData, fieldIDs ...int64) ([]any, error) {
	result := make([]any, 0, len(fieldIDs))
	for _, fieldID := range fieldIDs {
		fieldData := GetFieldData(fieldsData, fieldID)
		if fieldData == nil {
			return nil, merr.WrapErrFunctionFailedMsg("field %d not found", fieldID)
		}
		stringData := fieldData.GetScalars().GetStringData()
		if stringData == nil {
			return nil, merr.WrapErrFunctionFailedMsg("field %d is not string data", fieldID)
		}
		result = append(result, stringData.GetData())
	}
	return result, nil
}
