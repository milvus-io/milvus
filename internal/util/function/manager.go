package function

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/bm25"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var defaultManager FunctionRunnerManager = NewFunctionRunnerManager()

// LatestFunctionRunnerVersion asks Materialize to use the version currently
// registered by the lifecycle key. Zero is a valid schema version.
const LatestFunctionRunnerVersion int32 = -1

var (
	errFunctionRunnerEntryRemoved           = errors.New("function runner manager entry was removed")
	errFunctionRunnerCollectionEntryRemoved = errors.New("function runner manager collection entry was removed")
)

type FunctionRunnerManager interface {
	// Alloc records that a lifecycle key is using the schema version
	// and asynchronously tries to initialize function runners for that version.
	// The key identifies an independent lifecycle scope; for example, WAL uses
	// "WAL-"+vchannel while delegator uses "DELEGATOR-"+vchannel.
	// Invalid function metadata is returned synchronously. Runner initialization
	// failures are logged and retried by later requests instead of failing
	// collection recovery.
	Alloc(collectionID int64, key string, schema *schemapb.CollectionSchema) error

	// Update moves a lifecycle key to a newer schema version and asynchronously
	// initializes any missing function runners required by that version. Schema
	// snapshots without runner-backed functions are still retained so analyzer
	// execution can be resolved entirely by lifecycle key.
	// Invalid function metadata is returned synchronously. Runners for older
	// versions are kept until no key uses those versions.
	Update(collectionID int64, key string, schema *schemapb.CollectionSchema) error

	// Release removes one lifecycle key. The collection entry and its runners
	// are closed only after all keys are released.
	Release(collectionID int64, key string)

	// Materialize fills missing function output fields for a WAL insert request.
	// The lifecycle key selects the managed schema snapshot. Passing
	// LatestFunctionRunnerVersion uses the version currently registered by the key;
	// an explicit schemaVersion verifies that the WAL and manager snapshots match.
	Materialize(ctx context.Context, collectionID int64, key string, schemaVersion int32, body *msgpb.InsertRequest) (bool, error)

	// TryMaterialize is used by compatibility paths for old insert messages. It
	// uses the exact managed schema version when it is still retained. It returns
	// ok=false when the caller should build compatibility runners instead.
	TryMaterialize(ctx context.Context, collectionID int64, schemaVersion int32, body *msgpb.InsertRequest) (bool, bool, error)

	// RunWithRunner runs the callback with the runner that owns the output field.
	// The lifecycle key selects its currently registered schema version. The
	// callback is executed synchronously while the manager protects the runner
	// from concurrent close; callers must not retain the runner after the callback.
	RunWithRunner(ctx context.Context, collectionID int64, key string, outputFieldID int64, run func(FunctionRunner) error) (bool, error)

	// RunWithAnalyzer runs the callback with the analyzer service associated with
	// a field in the lifecycle key's current schema. BM25 runners are reused for
	// their input fields; other analyzer-enabled fields use a short-lived analyzer.
	// The callback is protected from concurrent close and must not retain the analyzer.
	RunWithAnalyzer(ctx context.Context, collectionID int64, key string, fieldID int64, run func(Analyzer) error) (bool, error)

	// Close releases all cached runners managed by this manager.
	Close()
}

type functionRunnerManager struct {
	mu      sync.RWMutex
	entries map[int64]*functionRunnerCollectionEntry
}

// Lock order is functionRunnerManager.mu -> functionRunnerCollectionEntry.mu ->
// functionRunnerEntry.mu. The manager lock protects collection entry publication
// and removal; a closed collection entry rejects callers that obtained it before
// removal. Runner Close calls are always done after releasing the manager and
// collection locks.
type functionRunnerCollectionEntry struct {
	mu             sync.RWMutex
	collectionID   int64
	keyVersions    map[string]int32
	versionRunners map[int32]*functionRunnerVersion
	runners        map[string]*functionRunnerEntry
	closed         bool
}

type functionRunnerVersion struct {
	schema                  *schemapb.CollectionSchema
	signatures              []string
	outputFieldIDs          []int64
	outputFieldSignatures   map[int64]string
	analyzerFieldSignatures map[int64]string
}

type functionRunnerEntry struct {
	mu       sync.RWMutex
	schema   *schemapb.CollectionSchema
	function *schemapb.FunctionSchema
	runner   FunctionRunner
	init     *functionRunnerInit
	closed   bool
}

type functionRunnerInit struct {
	done chan struct{}
	err  error
}

func newFunctionRunnerCollectionEntry(collectionID int64) *functionRunnerCollectionEntry {
	return &functionRunnerCollectionEntry{
		collectionID:   collectionID,
		keyVersions:    make(map[string]int32),
		versionRunners: make(map[int32]*functionRunnerVersion),
		runners:        make(map[string]*functionRunnerEntry),
	}
}

func (e *functionRunnerCollectionEntry) allocOrUpdate(
	key string,
	schema *schemapb.CollectionSchema,
	versionRunners *functionRunnerVersion,
	functionsBySignature map[string]*schemapb.FunctionSchema,
	operation string,
) error {
	schemaVersion := schema.GetVersion()
	warnInitFailure := func(err error) {
		mlog.Warn(context.TODO(), "failed to initialize function runners, will retry on next request",
			mlog.String("operation", operation),
			mlog.Int64("collectionID", e.collectionID),
			mlog.String("key", key),
			mlog.Int32("schemaVersion", schemaVersion),
			mlog.Err(err))
	}
	runnerEntries, staleRunnerEntries, err := e.ensureVersion(key, schema, versionRunners, functionsBySignature)
	if err != nil {
		return err
	}
	if len(staleRunnerEntries) > 0 {
		// New lookups can no longer reach these entries. Let existing callbacks
		// release their runner leases without blocking the schema update.
		go closeFunctionRunnerEntries(staleRunnerEntries)
	}
	if len(runnerEntries) == 0 {
		return nil
	}
	go func() {
		for _, runnerEntry := range runnerEntries {
			_, unlock, err := runnerEntry.GetRunner(context.Background())
			if err != nil {
				if !errors.Is(err, errFunctionRunnerEntryRemoved) {
					warnInitFailure(err)
				}
				break
			}
			unlock()
		}
	}()
	return nil
}

func (e *functionRunnerCollectionEntry) Release(key string) ([]*functionRunnerEntry, bool) {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil, false
	}
	if _, ok := e.keyVersions[key]; !ok {
		e.mu.Unlock()
		return nil, false
	}
	delete(e.keyVersions, key)
	remove := len(e.keyVersions) == 0
	if remove {
		e.closed = true
	}
	runnerEntries := e.gcLocked()
	e.mu.Unlock()
	return runnerEntries, remove
}

func (e *functionRunnerCollectionEntry) detachForClose() []*functionRunnerEntry {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.keyVersions = make(map[string]int32)
	runnerEntries := e.gcLocked()
	e.mu.Unlock()
	return runnerEntries
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

func (e *functionRunnerCollectionEntry) getVersionRunnerEntriesLocked(schemaVersion int32) ([]*functionRunnerEntry, []int64, bool, error) {
	versionRunners, ok := e.versionRunners[schemaVersion]
	if !ok {
		return nil, nil, false, nil
	}
	runnerEntries := make([]*functionRunnerEntry, 0, len(versionRunners.signatures))
	for _, signature := range versionRunners.signatures {
		runnerEntry := e.runners[signature]
		if runnerEntry == nil {
			return nil, nil, true, merr.WrapErrServiceInternalMsg("function runner entry not found for schema version %d", schemaVersion)
		}
		runnerEntries = append(runnerEntries, runnerEntry)
	}
	return runnerEntries, append([]int64(nil), versionRunners.outputFieldIDs...), true, nil
}

func (e *functionRunnerCollectionEntry) getVersionRunnerEntries(schemaVersion int32) ([]*functionRunnerEntry, []int64, bool, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.getVersionRunnerEntriesLocked(schemaVersion)
}

func runWithRunnerEntries(
	ctx context.Context,
	runnerEntries []*functionRunnerEntry,
	run func([]FunctionRunner) error,
) error {
	if ctx == nil {
		ctx = context.Background()
	}

	// Initialize and lease each runner in one step. Releasing an initialized
	// runner before reacquiring its lease would leave a window where schema GC
	// could close it and make strict materialization report a false success.
	// The read locks are shared, so concurrent materialization is still allowed;
	// concrete runners protect their own mutable state.
	runners := make([]FunctionRunner, 0, len(runnerEntries))
	for _, runnerEntry := range runnerEntries {
		runner, unlock, err := runnerEntry.GetRunner(ctx)
		if err != nil {
			return err
		}
		runners = append(runners, runner)
		defer unlock()
	}

	return run(runners)
}

func newFunctionRunnerEntry(
	schema *schemapb.CollectionSchema,
	fn *schemapb.FunctionSchema,
) *functionRunnerEntry {
	return &functionRunnerEntry{
		schema:   proto.Clone(schema).(*schemapb.CollectionSchema),
		function: proto.Clone(fn).(*schemapb.FunctionSchema),
	}
}

func (e *functionRunnerEntry) isReady() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.isReadyLocked()
}

func (e *functionRunnerEntry) isReadyLocked() bool {
	return !e.closed && e.runner != nil
}

func (e *functionRunnerEntry) GetRunner(ctx context.Context) (FunctionRunner, func(), error) {
	if err := e.ensureRunner(ctx); err != nil {
		return nil, nil, err
	}

	e.mu.RLock()
	if e.closed || e.runner == nil {
		e.mu.RUnlock()
		return nil, nil, errFunctionRunnerEntryRemoved
	}
	return e.runner, e.mu.RUnlock, nil
}

func (e *functionRunnerEntry) ensureRunner(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if e.isReady() {
		return nil
	}

	init, runInit, err := e.getOrCreateInit()
	if err != nil {
		return err
	}
	if init == nil {
		return nil
	}
	if runInit {
		go e.runInit(init)
	}
	return e.waitInit(ctx, init)
}

func (e *functionRunnerEntry) getOrCreateInit() (*functionRunnerInit, bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil, false, errFunctionRunnerEntryRemoved
	}
	if e.isReadyLocked() {
		return nil, false, nil
	}
	init := e.init
	if init == nil {
		init = &functionRunnerInit{done: make(chan struct{})}
		e.init = init
		return init, true, nil
	}
	return init, false, nil
}

func (e *functionRunnerEntry) waitInit(ctx context.Context, init *functionRunnerInit) error {
	select {
	case <-init.done:
		return init.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *functionRunnerEntry) runInit(init *functionRunnerInit) {
	runner, err := BuildEmbeddingRunner(e.schema, e.function)
	if err == nil && runner == nil {
		err = errors.New("function runner is nil")
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	defer close(init.done)

	e.init = nil
	if err != nil {
		if runner != nil {
			runner.Close()
		}
		init.err = err
		return
	}
	if e.closed {
		init.err = errFunctionRunnerEntryRemoved
		runner.Close()
		return
	}

	e.runner = runner
	init.err = nil
}

func (e *functionRunnerEntry) Close() {
	e.mu.Lock()
	runner := e.runner
	e.runner = nil
	e.closed = true
	e.mu.Unlock()

	if runner != nil {
		runner.Close()
	}
}

func NewFunctionRunnerManager() FunctionRunnerManager {
	return newFunctionRunnerManager()
}

// GetManager returns the process-wide function runner manager.
func GetManager() FunctionRunnerManager {
	return defaultManager
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
	versionRunners, functionsBySignature, err := buildFunctionRunnerVersion(schema)
	if err != nil {
		return err
	}
	return m.allocOrUpdate(collectionID, key, schema, versionRunners, functionsBySignature, "initialize")
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
	if entry := m.getEntry(collectionID); entry != nil {
		entry.mu.RLock()
		version, ok := entry.keyVersions[key]
		entry.mu.RUnlock()
		if ok && version >= schema.GetVersion() {
			return nil
		}
	}
	versionRunners, functionsBySignature, err := buildFunctionRunnerVersion(schema)
	if err != nil {
		return err
	}
	return m.allocOrUpdate(collectionID, key, schema, versionRunners, functionsBySignature, "update")
}

func (m *functionRunnerManager) allocOrUpdate(
	collectionID int64,
	key string,
	schema *schemapb.CollectionSchema,
	versionRunners *functionRunnerVersion,
	functionsBySignature map[string]*schemapb.FunctionSchema,
	operation string,
) error {
	for {
		entry := m.getOrCreateEntry(collectionID)
		err := entry.allocOrUpdate(key, schema, versionRunners, functionsBySignature, operation)
		// Final Release may close an entry after it was read from the manager.
		// Retry registration against the current collection entry only in that case.
		if errors.Is(err, errFunctionRunnerCollectionEntryRemoved) {
			continue
		}
		return err
	}
}

func (e *functionRunnerCollectionEntry) ensureVersion(
	key string,
	schema *schemapb.CollectionSchema,
	versionRunners *functionRunnerVersion,
	functionsBySignature map[string]*schemapb.FunctionSchema,
) ([]*functionRunnerEntry, []*functionRunnerEntry, error) {
	schemaVersion := schema.GetVersion()
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil, nil, errFunctionRunnerCollectionEntryRemoved
	}
	existingVersion, ok := e.versionRunners[schemaVersion]
	if ok {
		if len(existingVersion.signatures) != len(versionRunners.signatures) {
			e.mu.Unlock()
			return nil, nil, merr.WrapErrFunctionFailedMsg("function runner metadata does not match schema version %d", schemaVersion)
		}
		for i, signature := range existingVersion.signatures {
			if signature != versionRunners.signatures[i] {
				e.mu.Unlock()
				return nil, nil, merr.WrapErrFunctionFailedMsg("function runner metadata does not match schema version %d", schemaVersion)
			}
		}
		// Same-version metadata refreshes may update collection or standalone
		// analyzer properties without rebuilding function runners.
		existingVersion.schema = versionRunners.schema
		versionRunners = existingVersion
	} else {
		e.versionRunners[schemaVersion] = versionRunners
	}
	e.updateKeyVersionLocked(key, schemaVersion)

	for _, signature := range versionRunners.signatures {
		if e.runners[signature] == nil {
			e.runners[signature] = newFunctionRunnerEntry(
				schema,
				functionsBySignature[signature],
			)
		}
	}

	initRunnerEntries := make([]*functionRunnerEntry, 0, len(versionRunners.signatures))
	for _, signature := range versionRunners.signatures {
		runnerEntry := e.runners[signature]
		if runnerEntry.isReady() {
			continue
		}
		initRunnerEntries = append(initRunnerEntries, runnerEntry)
	}
	runnerEntries := e.gcLocked()
	e.mu.Unlock()

	return initRunnerEntries, runnerEntries, nil
}

func buildFunctionRunnerVersion(schema *schemapb.CollectionSchema) (*functionRunnerVersion, map[string]*schemapb.FunctionSchema, error) {
	if schema == nil {
		return nil, nil, merr.WrapErrFunctionFailedMsg("collection schema is nil")
	}

	functions := embeddingFunctions(schema)
	versionRunners := &functionRunnerVersion{
		schema:                  proto.Clone(schema).(*schemapb.CollectionSchema),
		signatures:              make([]string, 0, len(functions)),
		outputFieldIDs:          make([]int64, 0, len(functions)),
		outputFieldSignatures:   make(map[int64]string),
		analyzerFieldSignatures: make(map[int64]string),
	}
	functionsBySignature := make(map[string]*schemapb.FunctionSchema, len(functions))
	for _, fn := range functions {
		signature, inputFieldIDs, outputFieldIDs, err := embeddingFunctionMetadata(schema, fn)
		if err != nil {
			return nil, nil, err
		}

		versionRunners.signatures = append(versionRunners.signatures, signature)
		versionRunners.outputFieldIDs = append(versionRunners.outputFieldIDs, outputFieldIDs...)
		functionsBySignature[signature] = fn
		for _, outputFieldID := range outputFieldIDs {
			versionRunners.outputFieldSignatures[outputFieldID] = signature
		}
		if fn.GetType() == schemapb.FunctionType_BM25 && len(inputFieldIDs) > 0 {
			if _, ok := versionRunners.analyzerFieldSignatures[inputFieldIDs[0]]; !ok {
				versionRunners.analyzerFieldSignatures[inputFieldIDs[0]] = signature
			}
		}
	}
	return versionRunners, functionsBySignature, nil
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

func (e *functionRunnerCollectionEntry) Materialize(
	ctx context.Context,
	key string,
	schemaVersion int32,
	body *msgpb.InsertRequest,
) (bool, error) {
	if body == nil {
		return false, merr.WrapErrFunctionFailedMsg("insert request is nil")
	}

	e.mu.RLock()
	keyVersion, ok := e.keyVersions[key]
	if !ok {
		e.mu.RUnlock()
		if schemaVersion == LatestFunctionRunnerVersion {
			return false, nil
		}
		return false, merr.WrapErrFunctionFailedMsg("function runner schema for key %s is not available", key)
	}
	if schemaVersion != LatestFunctionRunnerVersion && keyVersion != schemaVersion {
		e.mu.RUnlock()
		return false, merr.WrapErrFunctionFailedMsg("function runner schema version mismatch for key %s: expected %d, actual %d", key, schemaVersion, keyVersion)
	}
	runnerEntries, outputFieldIDs, ok, err := e.getVersionRunnerEntriesLocked(keyVersion)
	e.mu.RUnlock()
	if err != nil {
		return false, err
	}
	if !ok {
		return false, merr.WrapErrServiceInternalMsg("function runner metadata not found for key %s at schema version %d", key, keyVersion)
	}
	changed, err := materializeWithRunnerEntries(ctx, runnerEntries, outputFieldIDs, body)
	return changed, err
}

func materializeWithRunnerEntries(
	ctx context.Context,
	runnerEntries []*functionRunnerEntry,
	outputFieldIDs []int64,
	body *msgpb.InsertRequest,
) (bool, error) {
	if len(outputFieldIDs) == 0 || HasAllFieldDataByID(body.GetFieldsData(), outputFieldIDs) {
		return false, nil
	}

	changed := false
	err := runWithRunnerEntries(ctx, runnerEntries, func(runners []FunctionRunner) error {
		var runErr error
		changed, runErr = FillFunctionFields(runners, body)
		return runErr
	})
	if err != nil {
		return false, err
	}
	return changed, nil
}

// TryMaterialize is only used by compatibility logic to try materializing old
// insert messages with cached runners.
func (e *functionRunnerCollectionEntry) TryMaterialize(
	ctx context.Context,
	schemaVersion int32,
	body *msgpb.InsertRequest,
) (bool, bool, error) {
	if body == nil {
		return false, false, merr.WrapErrFunctionFailedMsg("insert request is nil")
	}

	runnerEntries, outputFieldIDs, ok, err := e.getVersionRunnerEntries(schemaVersion)
	if err != nil {
		return false, true, err
	}
	if !ok {
		return false, false, nil
	}
	changed, err := materializeWithRunnerEntries(ctx, runnerEntries, outputFieldIDs, body)
	if err != nil {
		if errors.Is(err, errFunctionRunnerEntryRemoved) {
			return false, false, nil
		}
		return false, true, err
	}
	return changed, true, nil
}

func (e *functionRunnerCollectionEntry) RunWithRunner(
	ctx context.Context,
	key string,
	outputFieldID int64,
	run func(FunctionRunner) error,
) (bool, error) {
	e.mu.RLock()
	schemaVersion, ok := e.keyVersions[key]
	if !ok {
		e.mu.RUnlock()
		return false, merr.WrapErrServiceUnavailableMsg("function runner schema for key %s is not available", key)
	}
	versionRunners := e.versionRunners[schemaVersion]
	if versionRunners == nil {
		e.mu.RUnlock()
		return true, merr.WrapErrServiceInternalMsg("function runner metadata not found for key %s at schema version %d", key, schemaVersion)
	}
	signature, ok := versionRunners.outputFieldSignatures[outputFieldID]
	if !ok {
		fieldExists := typeutil.GetField(versionRunners.schema, outputFieldID) != nil
		e.mu.RUnlock()
		if !fieldExists {
			return false, merr.WrapErrServiceUnavailableMsg("field %d is not available in function runner schema for key %s at schema version %d", outputFieldID, key, schemaVersion)
		}
		return false, nil
	}
	runnerEntry := e.runners[signature]
	if runnerEntry == nil {
		e.mu.RUnlock()
		return true, merr.WrapErrServiceInternalMsg("function runner entry not found for key %s and output field %d", key, outputFieldID)
	}
	e.mu.RUnlock()
	runner, unlock, err := runnerEntry.GetRunner(ctx)
	if err != nil {
		return true, err
	}
	defer unlock()

	return true, run(runner)
}

func (e *functionRunnerCollectionEntry) RunWithAnalyzer(
	ctx context.Context,
	key string,
	fieldID int64,
	run func(Analyzer) error,
) (bool, error) {
	e.mu.RLock()
	schemaVersion, ok := e.keyVersions[key]
	if !ok {
		e.mu.RUnlock()
		return false, merr.WrapErrServiceUnavailableMsg("function runner schema for key %s is not available", key)
	}
	versionRunners := e.versionRunners[schemaVersion]
	if versionRunners == nil {
		e.mu.RUnlock()
		return true, merr.WrapErrServiceInternalMsg("function runner metadata not found for key %s at schema version %d", key, schemaVersion)
	}
	if signature, ok := versionRunners.analyzerFieldSignatures[fieldID]; ok {
		runnerEntry := e.runners[signature]
		if runnerEntry == nil {
			e.mu.RUnlock()
			return true, merr.WrapErrServiceInternalMsg("function runner entry not found for key %s and analyzer field %d", key, fieldID)
		}
		e.mu.RUnlock()

		runner, unlock, err := runnerEntry.GetRunner(ctx)
		if err != nil {
			return true, err
		}
		defer unlock()

		analyzer, ok := runner.(Analyzer)
		if !ok {
			return true, merr.WrapErrFunctionFailedMsg("function runner cannot serve analyzer requests")
		}
		return true, run(analyzer)
	}

	field := typeutil.GetField(versionRunners.schema, fieldID)
	if field != nil {
		field = proto.Clone(field).(*schemapb.FieldSchema)
	}
	e.mu.RUnlock()
	if field == nil {
		return false, merr.WrapErrServiceUnavailableMsg("field %d is not available in function runner schema for key %s at schema version %d", fieldID, key, schemaVersion)
	}
	if !typeutil.CreateFieldSchemaHelper(field).EnableAnalyzer() {
		return false, nil
	}

	analyzer, err := NewAnalyzerRunner(field)
	if err != nil {
		return true, err
	}
	if runner, ok := analyzer.(FunctionRunner); ok {
		defer runner.Close()
	}
	return true, run(analyzer)
}

func (m *functionRunnerManager) getOrCreateEntry(collectionID int64) *functionRunnerCollectionEntry {
	m.mu.RLock()
	entry := m.entries[collectionID]
	m.mu.RUnlock()
	if entry != nil {
		return entry
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	entry = m.entries[collectionID]
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
	runnerEntries, remove := entry.Release(key)
	if remove {
		delete(m.entries, collectionID)
	}
	m.mu.Unlock()

	closeFunctionRunnerEntries(runnerEntries)
}

func (m *functionRunnerManager) Materialize(
	ctx context.Context,
	collectionID int64,
	key string,
	schemaVersion int32,
	body *msgpb.InsertRequest,
) (bool, error) {
	entry := m.getEntry(collectionID)
	if entry == nil {
		if schemaVersion == LatestFunctionRunnerVersion {
			return false, nil
		}
		return false, merr.WrapErrFunctionFailedMsg("function runners for collection %d are not allocated", collectionID)
	}
	changed, err := entry.Materialize(ctx, key, schemaVersion, body)
	return changed, wrapFunctionRunnerLifecycleError(collectionID, err)
}

func (m *functionRunnerManager) TryMaterialize(
	ctx context.Context,
	collectionID int64,
	schemaVersion int32,
	body *msgpb.InsertRequest,
) (bool, bool, error) {
	entry := m.getEntry(collectionID)
	if entry == nil {
		return false, false, nil
	}
	changed, ok, err := entry.TryMaterialize(ctx, schemaVersion, body)
	if errors.Is(err, errFunctionRunnerCollectionEntryRemoved) || errors.Is(err, errFunctionRunnerEntryRemoved) {
		return false, false, nil
	}
	return changed, ok, err
}

func (m *functionRunnerManager) RunWithRunner(
	ctx context.Context,
	collectionID int64,
	key string,
	outputFieldID int64,
	run func(FunctionRunner) error,
) (bool, error) {
	entry := m.getEntry(collectionID)
	if entry == nil {
		return false, merr.WrapErrServiceUnavailableMsg("function runner schema for collection %d is not available", collectionID)
	}
	ok, err := entry.RunWithRunner(ctx, key, outputFieldID, run)
	return ok, wrapFunctionRunnerLifecycleError(collectionID, err)
}

func (m *functionRunnerManager) RunWithAnalyzer(
	ctx context.Context,
	collectionID int64,
	key string,
	fieldID int64,
	run func(Analyzer) error,
) (bool, error) {
	entry := m.getEntry(collectionID)
	if entry == nil {
		return false, merr.WrapErrServiceUnavailableMsg("function runner schema for collection %d is not available", collectionID)
	}
	ok, err := entry.RunWithAnalyzer(ctx, key, fieldID, run)
	return ok, wrapFunctionRunnerLifecycleError(collectionID, err)
}

func wrapFunctionRunnerLifecycleError(collectionID int64, err error) error {
	if errors.Is(err, errFunctionRunnerCollectionEntryRemoved) || errors.Is(err, errFunctionRunnerEntryRemoved) {
		return merr.WrapErrServiceUnavailableMsg("function runners for collection %d changed during execution", collectionID)
	}
	return err
}

func (m *functionRunnerManager) Close() {
	m.mu.Lock()
	runnerEntries := make([]*functionRunnerEntry, 0)
	for collectionID, entry := range m.entries {
		runnerEntries = append(runnerEntries, entry.detachForClose()...)
		delete(m.entries, collectionID)
	}
	m.mu.Unlock()

	closeFunctionRunnerEntries(runnerEntries)
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
		_, _, functionOutputFieldIDs, err := embeddingFunctionMetadata(schema, fn)
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

func embeddingFunctionSignature(schema *schemapb.CollectionSchema, fn *schemapb.FunctionSchema) (string, error) {
	signature, _, _, err := embeddingFunctionMetadata(schema, fn)
	return signature, err
}

func embeddingFunctionMetadata(schema *schemapb.CollectionSchema, fn *schemapb.FunctionSchema) (string, []int64, []int64, error) {
	if schema == nil {
		return "", nil, nil, merr.WrapErrFunctionFailedMsg("collection schema is nil")
	}
	if fn == nil {
		return "", nil, nil, merr.WrapErrFunctionFailedMsg("function schema is nil")
	}

	inputIDs := fn.GetInputFieldIds()
	inputNames := fn.GetInputFieldNames()
	if len(inputIDs) == 0 && len(inputNames) == 0 {
		return "", nil, nil, merr.WrapErrFunctionFailedMsg("function %s input fields not found", fn.GetName())
	}
	outputIDs := fn.GetOutputFieldIds()
	outputNames := fn.GetOutputFieldNames()
	if len(outputIDs) == 0 && len(outputNames) == 0 {
		return "", nil, nil, merr.WrapErrFunctionFailedMsg("function %s output fields not found", fn.GetName())
	}

	hasher := sha256.New()
	fmt.Fprintf(hasher, "fn:%d:%d:%s|", fn.GetId(), fn.GetType(), fn.GetName())
	writeInt64s(hasher, "input_ids", inputIDs)
	writeStrings(hasher, "input_names", inputNames)
	writeInt64s(hasher, "output_ids", outputIDs)
	writeStrings(hasher, "output_names", outputNames)
	writeKeyValuePairs(hasher, "fn_params", fn.GetParams())

	resolvedInputIDs := append([]int64(nil), inputIDs...)
	for _, fieldID := range inputIDs {
		field := typeutil.GetField(schema, fieldID)
		if field == nil {
			return "", nil, nil, merr.WrapErrFunctionFailedMsg("function %s input field %d not found", fn.GetName(), fieldID)
		}
		writeFieldSignature(hasher, "input", field)
	}
	for _, fieldName := range inputNames {
		field := typeutil.GetFieldByName(schema, fieldName)
		if field == nil {
			return "", nil, nil, merr.WrapErrFunctionFailedMsg("function %s input field %s not found", fn.GetName(), fieldName)
		}
		writeFieldSignature(hasher, "input_name", field)
		if len(inputIDs) == 0 {
			resolvedInputIDs = append(resolvedInputIDs, field.GetFieldID())
		}
	}

	resolvedOutputIDs := append([]int64(nil), outputIDs...)
	for _, fieldID := range outputIDs {
		field := typeutil.GetField(schema, fieldID)
		if field == nil {
			return "", nil, nil, merr.WrapErrFunctionFailedMsg("function %s output field %d not found", fn.GetName(), fieldID)
		}
		writeFieldSignature(hasher, "output", field)
	}
	for _, fieldName := range outputNames {
		field := typeutil.GetFieldByName(schema, fieldName)
		if field == nil {
			return "", nil, nil, merr.WrapErrFunctionFailedMsg("function %s output field %s not found", fn.GetName(), fieldName)
		}
		writeFieldSignature(hasher, "output_name", field)
		if len(outputIDs) == 0 {
			resolvedOutputIDs = append(resolvedOutputIDs, field.GetFieldID())
		}
	}
	return hex.EncodeToString(hasher.Sum(nil)), resolvedInputIDs, resolvedOutputIDs, nil
}

func writeFieldSignature(hasher hashWriter, prefix string, field *schemapb.FieldSchema) {
	fmt.Fprintf(hasher, "%s:%d:%s:%d:%d:%t|",
		prefix,
		field.GetFieldID(),
		field.GetName(),
		field.GetDataType(),
		field.GetElementType(),
		field.GetIsFunctionOutput())
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

		output, err := RunFunction(runner, body)
		if err != nil {
			return false, err
		}
		body.FieldsData = append(body.FieldsData, output)
		changed = true
	}

	return changed, nil
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
