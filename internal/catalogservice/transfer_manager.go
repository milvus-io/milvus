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

package catalogservice

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// TransferState is the durable collection-transfer job state. A Catalog Service
// can resume a job from any state after a retry or leader move.
type TransferState string

const (
	TransferStatePending           TransferState = "PENDING"
	TransferStatePrepared          TransferState = "PREPARED"
	TransferStateSourceDropped     TransferState = "SOURCE_DROPPED"
	TransferStateCatalogMoved      TransferState = "CATALOG_MOVED"
	TransferStateSourceDeactivated TransferState = "SOURCE_DEACTIVATED"
	TransferStateTargetApplied     TransferState = "TARGET_APPLIED"
	TransferStateDone              TransferState = "DONE"
	TransferStateFailed            TransferState = "FAILED"
	TransferStateAborted           TransferState = "ABORTED"
	TransferStateCommitUncertain   TransferState = "COMMIT_UNCERTAIN"
)

var errTransferJobModified = errors.New("transfer job was modified concurrently")

// StartCollectionTransferRequest describes a logical collection metadata move.
// Namespace identifies a Milvus cluster/catalog namespace, not a raw KV prefix.
type StartCollectionTransferRequest struct {
	TransferID      string
	TransferEpoch   int64
	SourceNamespace string
	TargetNamespace string
	DBName          string
	CollectionName  string
	CommitTs        typeutil.Timestamp
	CacheExpireTs   typeutil.Timestamp
	DrainTimeoutMs  int64
}

type StartCollectionTransferResponse struct {
	TransferID   string
	State        TransferState
	CollectionID int64
}

// TransferRootCoord is the narrow RootCoord runtime API the service needs for
// transfer. The service owns durable metadata movement; RootCoord owns live
// drain/cache/apply behavior.
type TransferRootCoord interface {
	CatalogTransferPrepare(ctx context.Context, req *rootcoordpb.CatalogTransferPrepareRequest) error
	CatalogTransferDeactivate(ctx context.Context, req *rootcoordpb.CatalogTransferDeactivateRequest) error
	CatalogTransferApply(ctx context.Context, req *rootcoordpb.CatalogTransferApplyRequest) error
	CatalogTransferAbort(ctx context.Context, req *rootcoordpb.CatalogTransferAbortRequest) error
}

type RootCoordCatalogResolver interface {
	RootCoordCatalog(namespace string) (metastore.RootCoordCatalog, error)
}

type TransferRootCoordResolver interface {
	RootCoord(namespace string) (TransferRootCoord, error)
}

type staticRootCoordCatalogResolver map[string]metastore.RootCoordCatalog

func StaticRootCoordCatalogResolver(catalogs map[string]metastore.RootCoordCatalog) RootCoordCatalogResolver {
	return staticRootCoordCatalogResolver(catalogs)
}

func (r staticRootCoordCatalogResolver) RootCoordCatalog(namespace string) (metastore.RootCoordCatalog, error) {
	catalog, ok := r[namespace]
	if !ok || catalog == nil {
		return nil, merr.WrapErrParameterInvalidMsg("rootcoord catalog for namespace %q is not registered", namespace)
	}
	return catalog, nil
}

type staticTransferRootCoordResolver map[string]TransferRootCoord

func StaticTransferRootCoordResolver(clients map[string]TransferRootCoord) TransferRootCoordResolver {
	return staticTransferRootCoordResolver(clients)
}

func (r staticTransferRootCoordResolver) RootCoord(namespace string) (TransferRootCoord, error) {
	client, ok := r[namespace]
	if !ok || client == nil {
		return nil, merr.WrapErrParameterInvalidMsg("rootcoord transfer client for namespace %q is not registered", namespace)
	}
	return client, nil
}

type TransferJob struct {
	TransferID      string
	Version         int64
	TransferEpoch   int64
	SourceNamespace string
	TargetNamespace string
	DBName          string
	CollectionName  string
	CommitTs        typeutil.Timestamp
	CacheExpireTs   typeutil.Timestamp
	DrainTimeoutMs  int64
	CollectionID    int64
	Database        *model.Database
	Collection      *model.Collection
	State           TransferState
	LastError       string
	storeValue      string
}

func (j *TransferJob) clone() *TransferJob {
	if j == nil {
		return nil
	}
	cp := *j
	if j.Database != nil {
		cp.Database = j.Database.Clone()
	}
	if j.Collection != nil {
		cp.Collection = j.Collection.Clone()
	}
	return &cp
}

type TransferJobStore interface {
	Get(ctx context.Context, transferID string) (*TransferJob, error)
	Save(ctx context.Context, job *TransferJob) error
	CompareAndSave(ctx context.Context, expected *TransferJob, job *TransferJob) error
}

type memoryTransferJobStore struct {
	mu   sync.Mutex
	jobs map[string]*TransferJob
}

func NewMemoryTransferJobStore() TransferJobStore {
	return &memoryTransferJobStore{jobs: make(map[string]*TransferJob)}
}

func (s *memoryTransferJobStore) Get(ctx context.Context, transferID string) (*TransferJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.jobs[transferID].clone(), nil
}

func (s *memoryTransferJobStore) Save(ctx context.Context, job *TransferJob) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	version := int64(1)
	if current := s.jobs[job.TransferID]; current != nil {
		version = current.Version + 1
	}
	job.Version = version
	s.jobs[job.TransferID] = job.clone()
	return nil
}

func (s *memoryTransferJobStore) CompareAndSave(ctx context.Context, expected *TransferJob, job *TransferJob) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.jobs[job.TransferID]
	if expected == nil {
		if current != nil {
			return errTransferJobModified
		}
		job.Version = 1
		s.jobs[job.TransferID] = job.clone()
		return nil
	}
	if current == nil || current.Version != expected.Version {
		return errTransferJobModified
	}
	job.Version = current.Version + 1
	s.jobs[job.TransferID] = job.clone()
	return nil
}

type TransferManager struct {
	mu        sync.Mutex
	catalogs  RootCoordCatalogResolver
	roots     TransferRootCoordResolver
	jobStore  TransferJobStore
	maxReadTs typeutil.Timestamp
}

func NewTransferManager(catalogs RootCoordCatalogResolver, roots TransferRootCoordResolver, jobStore TransferJobStore) *TransferManager {
	if jobStore == nil {
		jobStore = NewMemoryTransferJobStore()
	}
	return &TransferManager{
		catalogs:  catalogs,
		roots:     roots,
		jobStore:  jobStore,
		maxReadTs: typeutil.MaxTimestamp,
	}
}

func (m *TransferManager) StartCollectionTransfer(ctx context.Context, req StartCollectionTransferRequest) (*StartCollectionTransferResponse, error) {
	if err := validateStartTransferRequest(req); err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	job, err := m.loadOrCreateJob(ctx, req)
	if err != nil {
		return nil, err
	}
	req = requestFromJob(job)
	initialState := job.State
	if job.State == TransferStateDone {
		return &StartCollectionTransferResponse{TransferID: job.TransferID, State: job.State, CollectionID: job.CollectionID}, nil
	}

	srcCatalog, err := m.catalogs.RootCoordCatalog(req.SourceNamespace)
	if err != nil {
		return nil, err
	}
	dstCatalog, err := m.catalogs.RootCoordCatalog(req.TargetNamespace)
	if err != nil {
		return nil, err
	}
	srcRoot, err := m.roots.RootCoord(req.SourceNamespace)
	if err != nil {
		return nil, err
	}
	dstRoot, err := m.roots.RootCoord(req.TargetNamespace)
	if err != nil {
		return nil, err
	}

	if job.Collection == nil {
		srcDB, srcColl, err := m.loadSourceCollection(ctx, srcCatalog, req)
		if err != nil {
			return nil, err
		}
		expected := job.clone()
		job.Database = srcDB.Clone()
		job.CollectionID = srcColl.CollectionID
		job.Collection = normalizeCollectionForTransfer(srcColl)
		if err := m.jobStore.CompareAndSave(ctx, expected, job); err != nil {
			return nil, err
		}
	} else {
		job.CollectionID = job.Collection.CollectionID
		if job.Database == nil {
			expected := job.clone()
			job.Database = &model.Database{
				ID:    job.Collection.DBID,
				Name:  req.DBName,
				State: etcdpb.DatabaseState_DatabaseCreated,
			}
			if err := m.jobStore.CompareAndSave(ctx, expected, job); err != nil {
				return nil, err
			}
		}
	}
	preparedColl := job.Collection.Clone()
	preparedDB := job.Database.Clone()

	if job.State == TransferStatePending {
		if err := srcRoot.CatalogTransferPrepare(ctx, buildPrepareRequest(req, preparedColl.CollectionID)); err != nil {
			return nil, m.failJob(ctx, job, err, srcRoot)
		}
		if err := m.advance(ctx, job, TransferStatePrepared); err != nil {
			return nil, err
		}
	}

	if job.State == TransferStateCommitUncertain {
		if err := m.advance(ctx, job, TransferStateSourceDropped); err != nil {
			return nil, err
		}
	}

	prepared := job.State == TransferStatePrepared ||
		job.State == TransferStateSourceDropped ||
		job.State == TransferStateCatalogMoved ||
		job.State == TransferStateSourceDeactivated ||
		job.State == TransferStateTargetApplied
	targetCollectionExists := false
	targetDatabaseExists := false
	targetPreflightDone := false
	if job.State == TransferStatePrepared {
		if initialState == TransferStatePrepared {
			if err := srcRoot.CatalogTransferPrepare(ctx, buildPrepareRequest(req, preparedColl.CollectionID)); err != nil {
				return nil, m.failJob(ctx, job, err, srcRoot)
			}
		}
		if err := m.ensureSourceStillMatchesSnapshot(ctx, srcCatalog, req, preparedColl); err != nil {
			return nil, m.failJob(ctx, job, err, srcRoot)
		}
		targetDatabaseExists, targetCollectionExists, err = m.preflightTargetCatalog(ctx, dstCatalog, preparedDB, preparedColl, req)
		if err != nil {
			return nil, m.failJob(ctx, job, err, srcRoot)
		}
		targetPreflightDone = true
		if err := m.advance(ctx, job, TransferStateSourceDropped); err != nil {
			return nil, err
		}
	}

	if !prepared {
		return nil, merr.WrapErrServiceInternalMsg("transfer entered unexpected state: %s", job.State)
	}

	if job.State == TransferStateSourceDropped {
		if err := m.dropSourceCatalogIfPresent(ctx, srcCatalog, req, preparedColl); err != nil {
			return nil, m.markCommitUncertain(ctx, job, err)
		}
		if err := m.createTargetCatalog(ctx, dstCatalog, preparedDB, preparedColl, req, targetPreflightDone, targetDatabaseExists, targetCollectionExists); err != nil {
			return nil, m.markCommitUncertain(ctx, job, err)
		}
		if err := m.advance(ctx, job, TransferStateCatalogMoved); err != nil {
			return nil, err
		}
	}

	if job.State == TransferStateCatalogMoved {
		if err := srcRoot.CatalogTransferDeactivate(ctx, &rootcoordpb.CatalogTransferDeactivateRequest{
			TransferId:     req.TransferID,
			TransferEpoch:  req.TransferEpoch,
			CollectionId:   preparedColl.CollectionID,
			DbName:         req.DBName,
			CollectionName: req.CollectionName,
			Aliases:        cloneStringList(preparedColl.Aliases),
			CacheExpireTs:  req.CacheExpireTs,
		}); err != nil {
			return nil, m.failJob(ctx, job, err, nil)
		}
		if err := m.advance(ctx, job, TransferStateSourceDeactivated); err != nil {
			return nil, err
		}
	}

	if job.State == TransferStateSourceDeactivated {
		if err := dstRoot.CatalogTransferApply(ctx, buildApplyRequest(req, preparedColl)); err != nil {
			return nil, m.failJob(ctx, job, err, nil)
		}
		if err := m.advance(ctx, job, TransferStateTargetApplied); err != nil {
			return nil, err
		}
	}
	if job.State == TransferStateTargetApplied {
		if err := m.advance(ctx, job, TransferStateDone); err != nil {
			return nil, err
		}
	}

	return &StartCollectionTransferResponse{TransferID: job.TransferID, State: job.State, CollectionID: job.CollectionID}, nil
}

func (m *TransferManager) GetCollectionTransfer(ctx context.Context, transferID string) (*TransferJob, error) {
	if transferID == "" {
		return nil, merr.WrapErrParameterInvalidMsg("transfer id is required")
	}
	job, err := m.jobStore.Get(ctx, transferID)
	if err != nil {
		return nil, err
	}
	if job == nil {
		return nil, merr.WrapErrParameterInvalidMsg("transfer id %q not found", transferID)
	}
	return job, nil
}

func validateStartTransferRequest(req StartCollectionTransferRequest) error {
	switch {
	case req.TransferID == "":
		return merr.WrapErrParameterInvalidMsg("transfer id is required")
	case req.TransferEpoch <= 0:
		return merr.WrapErrParameterInvalidMsg("transfer epoch must be positive")
	case req.SourceNamespace == "":
		return merr.WrapErrParameterInvalidMsg("source namespace is required")
	case req.TargetNamespace == "":
		return merr.WrapErrParameterInvalidMsg("target namespace is required")
	case req.SourceNamespace == req.TargetNamespace:
		return merr.WrapErrParameterInvalidMsg("source namespace and target namespace must differ")
	case req.DBName == "":
		return merr.WrapErrParameterInvalidMsg("database name is required")
	case req.CollectionName == "":
		return merr.WrapErrParameterInvalidMsg("collection name is required")
	case req.CommitTs == 0:
		return merr.WrapErrParameterInvalidMsg("commit timestamp is required")
	}
	return nil
}

func (m *TransferManager) loadOrCreateJob(ctx context.Context, req StartCollectionTransferRequest) (*TransferJob, error) {
	job, err := m.jobStore.Get(ctx, req.TransferID)
	if err != nil {
		return nil, err
	}
	if job != nil {
		if job.TransferEpoch != req.TransferEpoch ||
			job.SourceNamespace != req.SourceNamespace ||
			job.TargetNamespace != req.TargetNamespace ||
			job.DBName != req.DBName ||
			job.CollectionName != req.CollectionName ||
			job.CommitTs != req.CommitTs ||
			job.CacheExpireTs != req.CacheExpireTs ||
			job.DrainTimeoutMs != req.DrainTimeoutMs {
			return nil, merr.WrapErrParameterInvalidMsg("transfer id %q already exists with different parameters", req.TransferID)
		}
		return job, nil
	}
	job = &TransferJob{
		TransferID:      req.TransferID,
		TransferEpoch:   req.TransferEpoch,
		SourceNamespace: req.SourceNamespace,
		TargetNamespace: req.TargetNamespace,
		DBName:          req.DBName,
		CollectionName:  req.CollectionName,
		CommitTs:        req.CommitTs,
		CacheExpireTs:   req.CacheExpireTs,
		DrainTimeoutMs:  req.DrainTimeoutMs,
		State:           TransferStatePending,
	}
	if err := m.jobStore.CompareAndSave(ctx, nil, job); err != nil {
		return nil, err
	}
	return job, nil
}

func requestFromJob(job *TransferJob) StartCollectionTransferRequest {
	return StartCollectionTransferRequest{
		TransferID:      job.TransferID,
		TransferEpoch:   job.TransferEpoch,
		SourceNamespace: job.SourceNamespace,
		TargetNamespace: job.TargetNamespace,
		DBName:          job.DBName,
		CollectionName:  job.CollectionName,
		CommitTs:        job.CommitTs,
		CacheExpireTs:   job.CacheExpireTs,
		DrainTimeoutMs:  job.DrainTimeoutMs,
	}
}

func (m *TransferManager) loadSourceCollection(ctx context.Context, catalog metastore.RootCoordCatalog, req StartCollectionTransferRequest) (*model.Database, *model.Collection, error) {
	db, err := findDatabase(ctx, catalog, req.DBName, m.maxReadTs)
	if err != nil {
		return nil, nil, err
	}
	coll, err := catalog.GetCollectionByName(ctx, db.ID, req.DBName, req.CollectionName, m.maxReadTs)
	if err != nil {
		return nil, nil, err
	}
	if coll == nil {
		return nil, nil, merr.WrapErrCollectionNotFoundWithDB(req.DBName, req.CollectionName)
	}
	if coll.DBID != db.ID {
		return nil, nil, merr.WrapErrServiceInternalMsg("source collection database id mismatch, db: %s, db id: %d, collection db id: %d", req.DBName, db.ID, coll.DBID)
	}
	coll, err = m.hydrateCollectionAliases(ctx, catalog, coll.Clone())
	if err != nil {
		return nil, nil, err
	}
	return db, coll, nil
}

func (m *TransferManager) ensureSourceStillMatchesSnapshot(ctx context.Context, catalog metastore.RootCoordCatalog, req StartCollectionTransferRequest, expected *model.Collection) error {
	coll, err := catalog.GetCollectionByName(ctx, expected.DBID, req.DBName, req.CollectionName, m.maxReadTs)
	if err != nil {
		return err
	}
	if coll == nil {
		return merr.WrapErrCollectionNotFoundWithDB(req.DBName, req.CollectionName)
	}
	coll, err = m.hydrateCollectionAliases(ctx, catalog, coll.Clone())
	if err != nil {
		return err
	}
	if !collectionsEquivalent(coll, expected) {
		return merr.WrapErrServiceInternalMsg("source collection changed during transfer prepare, collection id: %d", expected.CollectionID)
	}
	return nil
}

func (m *TransferManager) dropSourceCatalogIfPresent(ctx context.Context, catalog metastore.RootCoordCatalog, req StartCollectionTransferRequest, expected *model.Collection) error {
	coll, err := catalog.GetCollectionByName(ctx, expected.DBID, req.DBName, req.CollectionName, m.maxReadTs)
	if err != nil {
		if errors.Is(err, merr.ErrCollectionNotFound) {
			return nil
		}
		return err
	}
	if coll == nil {
		return nil
	}
	coll, err = m.hydrateCollectionAliases(ctx, catalog, coll.Clone())
	if err != nil {
		return err
	}
	if !collectionsEquivalent(coll, expected) {
		return merr.WrapErrServiceInternalMsg("source collection changed before source drop, collection id: %d", expected.CollectionID)
	}
	return catalog.Update(ctx, req.CommitTs, metastore.DropCollection(expected.Clone()))
}

func (m *TransferManager) hydrateCollectionAliases(ctx context.Context, catalog metastore.RootCoordCatalog, coll *model.Collection) (*model.Collection, error) {
	aliases, err := catalog.ListAliases(ctx, coll.DBID, m.maxReadTs)
	if err != nil {
		return nil, err
	}
	names := typeutil.NewSet[string](coll.Aliases...)
	for _, alias := range aliases {
		if alias != nil && alias.CollectionID == coll.CollectionID && alias.Available() {
			names.Insert(alias.Name)
		}
	}
	coll.Aliases = names.Collect()
	slices.Sort(coll.Aliases)
	return coll, nil
}

func (m *TransferManager) preflightTargetCatalog(ctx context.Context, dstCatalog metastore.RootCoordCatalog, db *model.Database, coll *model.Collection, req StartCollectionTransferRequest) (bool, bool, error) {
	targetDatabaseExists, err := targetDatabaseExistsWithExpectedID(ctx, dstCatalog, req.DBName, db.ID, m.maxReadTs)
	if err != nil {
		return false, false, err
	}
	existingByID, err := dstCatalog.GetCollectionByID(ctx, coll.DBID, m.maxReadTs, coll.CollectionID)
	if err != nil && !errors.Is(err, merr.ErrCollectionNotFound) {
		return false, false, err
	}
	if existingByID != nil {
		existingByID, err = m.hydrateCollectionAliases(ctx, dstCatalog, existingByID.Clone())
		if err != nil {
			return false, false, err
		}
		if !collectionsEquivalent(existingByID, coll) {
			return false, false, merr.WrapErrParameterInvalidMsg("target collection id %d already exists with different metadata", coll.CollectionID)
		}
	}
	existing, err := dstCatalog.GetCollectionByName(ctx, coll.DBID, req.DBName, req.CollectionName, m.maxReadTs)
	if err != nil && !errors.Is(err, merr.ErrCollectionNotFound) {
		return false, false, err
	}
	if existing != nil {
		existing, err = m.hydrateCollectionAliases(ctx, dstCatalog, existing.Clone())
		if err != nil {
			return false, false, err
		}
		if !collectionsEquivalent(existing, coll) {
			return false, false, merr.WrapErrParameterInvalidMsg("target collection %s.%s already exists with different metadata", req.DBName, req.CollectionName)
		}
		if err := m.preflightTargetAliases(ctx, dstCatalog, coll); err != nil {
			return false, false, err
		}
		return targetDatabaseExists, true, nil
	}
	if err := m.preflightTargetAliases(ctx, dstCatalog, coll); err != nil {
		return false, false, err
	}
	return targetDatabaseExists, existingByID != nil, nil
}

func (m *TransferManager) preflightTargetAliases(ctx context.Context, dstCatalog metastore.RootCoordCatalog, coll *model.Collection) error {
	if len(coll.Aliases) == 0 {
		return nil
	}
	aliases, err := dstCatalog.ListAliases(ctx, coll.DBID, m.maxReadTs)
	if err != nil {
		return err
	}
	transferred := typeutil.NewSet[string](coll.Aliases...)
	for _, alias := range aliases {
		if alias != nil && alias.Available() && transferred.Contain(alias.Name) && alias.CollectionID != coll.CollectionID {
			return merr.WrapErrParameterInvalidMsg("target alias %q already exists for collection id %d", alias.Name, alias.CollectionID)
		}
	}
	return nil
}

func (m *TransferManager) createTargetCatalog(ctx context.Context, dstCatalog metastore.RootCoordCatalog, db *model.Database, coll *model.Collection, req StartCollectionTransferRequest, preflightDone bool, targetDatabaseExists bool, targetCollectionExists bool) error {
	if !preflightDone {
		var err error
		targetDatabaseExists, targetCollectionExists, err = m.preflightTargetCatalog(ctx, dstCatalog, db, coll, req)
		if err != nil {
			return err
		}
	}
	if !targetDatabaseExists {
		if err := dstCatalog.CreateDatabase(ctx, db.Clone(), req.CommitTs); err != nil {
			return err
		}
	}
	if !targetCollectionExists {
		if err := dstCatalog.Update(ctx, req.CommitTs, metastore.CreateCollection(coll.Clone())); err != nil {
			return err
		}
	}
	existingAliases, err := m.existingTargetAliasCollections(ctx, dstCatalog, coll)
	if err != nil {
		return err
	}
	for _, aliasName := range coll.Aliases {
		if collectionID, ok := existingAliases[aliasName]; ok {
			if collectionID == coll.CollectionID {
				continue
			}
			return merr.WrapErrParameterInvalidMsg("target alias %q already exists for collection id %d", aliasName, collectionID)
		}
		if err := dstCatalog.CreateAlias(ctx, &model.Alias{
			Name:         aliasName,
			CollectionID: coll.CollectionID,
			State:        etcdpb.AliasState_AliasCreated,
			DbID:         coll.DBID,
		}, req.CommitTs); err != nil {
			return err
		}
	}
	return nil
}

func (m *TransferManager) existingTargetAliasCollections(ctx context.Context, dstCatalog metastore.RootCoordCatalog, coll *model.Collection) (map[string]int64, error) {
	aliasesByName := make(map[string]int64)
	if len(coll.Aliases) == 0 {
		return aliasesByName, nil
	}
	aliases, err := dstCatalog.ListAliases(ctx, coll.DBID, m.maxReadTs)
	if err != nil {
		return nil, err
	}
	for _, alias := range aliases {
		if alias != nil && alias.Available() {
			aliasesByName[alias.Name] = alias.CollectionID
		}
	}
	return aliasesByName, nil
}

func buildPrepareRequest(req StartCollectionTransferRequest, collectionID int64) *rootcoordpb.CatalogTransferPrepareRequest {
	return &rootcoordpb.CatalogTransferPrepareRequest{
		TransferId:     req.TransferID,
		TransferEpoch:  req.TransferEpoch,
		CollectionId:   collectionID,
		DbName:         req.DBName,
		CollectionName: req.CollectionName,
		DrainTimeoutMs: req.DrainTimeoutMs,
	}
}

func targetDatabaseExistsWithExpectedID(ctx context.Context, catalog metastore.RootCoordCatalog, dbName string, expectedDBID int64, ts typeutil.Timestamp) (bool, error) {
	db, err := findDatabase(ctx, catalog, dbName, ts)
	if err != nil {
		if errors.Is(err, merr.ErrDatabaseNotFound) {
			return false, nil
		}
		return false, err
	}
	if db.ID != expectedDBID {
		return false, merr.WrapErrParameterInvalidMsg("target database id mismatch for %s, expected %d, got %d", dbName, expectedDBID, db.ID)
	}
	return true, nil
}

func findDatabase(ctx context.Context, catalog metastore.RootCoordCatalog, dbName string, ts typeutil.Timestamp) (*model.Database, error) {
	dbs, err := catalog.ListDatabases(ctx, ts)
	if err != nil {
		return nil, err
	}
	for _, db := range dbs {
		if db != nil && db.Name == dbName {
			return db, nil
		}
	}
	return nil, merr.WrapErrDatabaseNotFound(dbName)
}

func buildApplyRequest(req StartCollectionTransferRequest, coll *model.Collection) *rootcoordpb.CatalogTransferApplyRequest {
	coll = normalizeCollectionForTransfer(coll)
	apply := &rootcoordpb.CatalogTransferApplyRequest{
		TransferId:    req.TransferID,
		TransferEpoch: req.TransferEpoch,
		Collection: model.MarshalCollectionModelWithOption(
			coll,
			model.WithFields(),
			model.WithStructArrayFields(),
			model.WithFunctions(),
		),
		CacheExpireTs: req.CacheExpireTs,
	}
	for _, partition := range coll.Partitions {
		apply.Partitions = append(apply.Partitions, model.MarshalPartitionModel(partition))
	}
	for _, alias := range coll.Aliases {
		apply.Aliases = append(apply.Aliases, model.MarshalAliasModel(&model.Alias{
			Name:         alias,
			CollectionID: coll.CollectionID,
			State:        etcdpb.AliasState_AliasCreated,
			DbID:         coll.DBID,
		}))
	}
	return apply
}

func collectionsEquivalent(a, b *model.Collection) bool {
	a = normalizeCollectionForTransfer(a)
	b = normalizeCollectionForTransfer(b)
	return reflect.DeepEqual(
		model.MarshalCollectionModelWithOption(a, model.WithFields(), model.WithStructArrayFields(), model.WithFunctions(), model.WithPartitions()),
		model.MarshalCollectionModelWithOption(b, model.WithFields(), model.WithStructArrayFields(), model.WithFunctions(), model.WithPartitions()),
	) && reflect.DeepEqual(a.Aliases, b.Aliases)
}

func normalizeCollectionForTransfer(coll *model.Collection) *model.Collection {
	if coll == nil {
		return nil
	}
	cp := coll.Clone()
	if len(cp.VirtualChannelNames) == 0 {
		slices.Sort(cp.Aliases)
		cp.Aliases = slices.Compact(cp.Aliases)
		return cp
	}
	if cp.ShardInfos == nil {
		cp.ShardInfos = make(map[string]*model.ShardInfo, len(cp.VirtualChannelNames))
	}
	for i, vchannel := range cp.VirtualChannelNames {
		if _, ok := cp.ShardInfos[vchannel]; ok {
			continue
		}
		pchannel := ""
		if i < len(cp.PhysicalChannelNames) {
			pchannel = cp.PhysicalChannelNames[i]
		}
		cp.ShardInfos[vchannel] = &model.ShardInfo{
			VChannelName: vchannel,
			PChannelName: pchannel,
		}
	}
	slices.Sort(cp.Aliases)
	cp.Aliases = slices.Compact(cp.Aliases)
	return cp
}

func (m *TransferManager) advance(ctx context.Context, job *TransferJob, state TransferState) error {
	expected := job.clone()
	job.State = state
	job.LastError = ""
	return m.jobStore.CompareAndSave(ctx, expected, job)
}

func (m *TransferManager) failJob(ctx context.Context, job *TransferJob, cause error, srcRoot TransferRootCoord) error {
	if srcRoot == nil && (job.State == TransferStateSourceDropped || job.State == TransferStateCatalogMoved || job.State == TransferStateSourceDeactivated || job.State == TransferStateTargetApplied) {
		expected := job.clone()
		job.LastError = cause.Error()
		_ = m.jobStore.CompareAndSave(ctx, expected, job)
		return cause
	}
	if srcRoot != nil && job.CollectionID != 0 {
		abortErr := srcRoot.CatalogTransferAbort(ctx, &rootcoordpb.CatalogTransferAbortRequest{
			TransferId:    job.TransferID,
			TransferEpoch: job.TransferEpoch,
			CollectionId:  job.CollectionID,
		})
		if abortErr != nil {
			expected := job.clone()
			job.LastError = fmt.Sprintf("%v; abort failed: %v", cause, abortErr)
			_ = m.jobStore.CompareAndSave(ctx, expected, job)
			return fmt.Errorf("%w; abort failed: %v", cause, abortErr)
		}
		expected := job.clone()
		job.State = TransferStateAborted
		job.LastError = cause.Error()
		_ = m.jobStore.CompareAndSave(ctx, expected, job)
		return cause
	}
	expected := job.clone()
	job.State = TransferStateFailed
	job.LastError = cause.Error()
	_ = m.jobStore.CompareAndSave(ctx, expected, job)
	return cause
}

func (m *TransferManager) markCommitUncertain(ctx context.Context, job *TransferJob, cause error) error {
	expected := job.clone()
	job.State = TransferStateCommitUncertain
	job.LastError = cause.Error()
	_ = m.jobStore.CompareAndSave(ctx, expected, job)
	return cause
}

func cloneStringList(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}
