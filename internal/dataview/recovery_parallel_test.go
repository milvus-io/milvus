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

package dataview

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// recoveryTestCatalog fails saves and prefix deletions per Collection, so
// outcomes do not depend on the order of concurrent recovery tasks.
type recoveryTestCatalog struct {
	fakeDataViewCatalog
	saveErrs map[int64]error
	dropErrs map[int64]error
	saved    []string
}

func (c *recoveryTestCatalog) SaveDataView(ctx context.Context, view *viewpb.DataViewOfCollection) error {
	if err := c.saveErrs[view.GetCollectionId()]; err != nil {
		return err
	}
	if err := c.fakeDataViewCatalog.SaveDataView(ctx, view); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.saved = append(c.saved, fmt.Sprintf("%d/%s", view.GetCollectionId(), dataVersionKey(view.GetDataVersion())))
	return nil
}

func (c *recoveryTestCatalog) DropDataViews(ctx context.Context, collectionID int64) error {
	if err := c.dropErrs[collectionID]; err != nil {
		return err
	}
	return c.fakeDataViewCatalog.DropDataViews(ctx, collectionID)
}

// contents returns the persisted snapshots keyed by Collection and version.
func (c *recoveryTestCatalog) contents(t *testing.T) map[string][][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string][][]byte)
	for _, view := range c.views {
		key := "malformed"
		if view.GetDataVersion() != nil {
			key = fmt.Sprintf("%d/%s", view.GetCollectionId(), dataVersionKey(view.GetDataVersion()))
		}
		out[key] = append(out[key], marshalDeterministic(t, view))
	}
	return out
}

func (c *recoveryTestCatalog) writes() ([]string, []int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	saved := append([]string(nil), c.saved...)
	dropped := append([]int64(nil), c.dropAllCall...)
	sort.Strings(saved)
	sort.Slice(dropped, func(i, j int) bool { return dropped[i] < dropped[j] })
	return saved, dropped
}

func marshalDeterministic(t *testing.T, view *viewpb.DataViewOfCollection) []byte {
	if view == nil {
		return nil
	}
	bytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(view)
	require.NoError(t, err)
	return bytes
}

// The recovery fixture assigns one case to every Collection by id % 10.
const (
	caseUpToDate = iota
	caseStale
	caseDropped
	caseBootstrap
	caseNotLive
	caseVersionHistory
	caseProjectionFailure
	caseBootstrapProjectionFailure
	caseManifestRegression
	caseLiveNotRecoverable
	recoveryCases
)

var (
	errFixtureProjection = errors.New("fixture projection failure")
	errFixtureSave       = errors.New("fixture save failure")
)

type recoveryFixture struct {
	catalog   *recoveryTestCatalog
	live      []int64
	vchannels map[int64][]string
	// failValidation fails the validator for these Collections.
	failValidation map[int64]error

	validations sync.Map // collectionID -> *atomic.Int64
}

func fixtureSegment(collectionID, n int64, manifestVersion int64) LoadableSegment {
	return LoadableSegment{
		SegmentID:       collectionID*100 + n,
		VChannel:        fmt.Sprintf("c%d-v0", collectionID),
		PartitionID:     collectionID*10 + 1,
		ManifestVersion: manifestVersion,
		RowNum:          collectionID + n,
	}
}

func fixtureView(collectionID int64, version *viewpb.DataVersion, segments ...LoadableSegment) *viewpb.DataViewOfCollection {
	view := buildEmptyDataView(collectionID, []string{fmt.Sprintf("c%d-v0", collectionID)})
	view.DataVersion = version
	if err := addSegments(view, segments); err != nil {
		panic(err)
	}
	canonicalizeDataView(view)
	return view
}

// fixtureBases returns the two bases an up-to-date Collection selects.
func fixtureBases(collectionID int64) (LoadableSegment, LoadableSegment) {
	return fixtureSegment(collectionID, 1, 2), fixtureSegment(collectionID, 2, 2)
}

// fixtureUpToDateView is the snapshot that matches fixtureBases.
func fixtureUpToDateView(collectionID int64) *viewpb.DataViewOfCollection {
	a, b := fixtureBases(collectionID)
	return fixtureView(collectionID, version(2, 0), a, b)
}

func newRecoveryFixture(collections int64, inject func(*recoveryFixture)) *recoveryFixture {
	f := &recoveryFixture{
		catalog: &recoveryTestCatalog{
			saveErrs: make(map[int64]error),
			dropErrs: make(map[int64]error),
		},
		vchannels:      make(map[int64][]string),
		failValidation: make(map[int64]error),
	}
	if inject != nil {
		inject(f)
	}
	var history []*viewpb.DataViewOfCollection
	for id := int64(1); id <= collections; id++ {
		a, b := fixtureBases(id)
		f.vchannels[id] = []string{fmt.Sprintf("c%d-v0", id), fmt.Sprintf("c%d-v1", id)}
		switch id % recoveryCases {
		case caseUpToDate, caseProjectionFailure:
			f.catalog.views = append(f.catalog.views, fixtureUpToDateView(id))
			f.live = append(f.live, id)
		case caseStale:
			f.catalog.views = append(f.catalog.views, fixtureView(id, version(2, 0), a, fixtureSegment(id, 2, 1)))
			f.live = append(f.live, id)
			if id%20 == caseStale {
				f.catalog.saveErrs[id] = errFixtureSave
			}
		case caseDropped:
			f.catalog.views = append(f.catalog.views, fixtureView(id, version(1, 0), a))
			history = append(history, fixtureView(id, version(2, 0), a, b))
			if id%24 == caseDropped {
				f.live = append(f.live, id)
			}
		case caseBootstrap, caseBootstrapProjectionFailure, caseLiveNotRecoverable:
			f.live = append(f.live, id)
			if id%20 == caseBootstrap {
				f.catalog.saveErrs[id] = errFixtureSave
			}
		case caseNotLive:
			f.catalog.views = append(f.catalog.views, fixtureView(id, version(3, 2), a, b))
		case caseVersionHistory:
			// First wins among identical and conflicting snapshots of one version.
			f.catalog.views = append(f.catalog.views,
				fixtureView(id, version(1, 0), a),
				fixtureView(id, version(1, 0), a, b))
			history = append(history,
				fixtureView(id, version(2, 0), a, b),
				fixtureView(id, version(2, 0), a, b),
				fixtureView(id, version(2, 1), a, fixtureSegment(id, 2, 3)))
			f.live = append(f.live, id)
		case caseManifestRegression:
			f.catalog.views = append(f.catalog.views, fixtureView(id, version(2, 0), a, fixtureSegment(id, 2, 5)))
			f.live = append(f.live, id)
		}
	}
	// Later versions and malformed keys are listed after every first version.
	f.catalog.views = append(f.catalog.views, history...)
	f.catalog.views = append(f.catalog.views,
		nil,
		&viewpb.DataViewOfCollection{CollectionId: 0, DataVersion: version(1, 0)},
		&viewpb.DataViewOfCollection{CollectionId: collections + 1},
		&viewpb.DataViewOfCollection{CollectionId: collections + 2, DataVersion: version(1, 0), Shards: []*viewpb.DataViewOfShard{{
			Vchannel:   "misaligned",
			Partitions: []*viewpb.DataViewOfPartition{{SegmentIds: []int64{1, 2}, SegmentManifestVersions: []int64{1}}},
		}}})
	return f
}

func (f *recoveryFixture) validate(_ context.Context, collectionID int64) (bool, error) {
	counter, _ := f.validations.LoadOrStore(collectionID, &atomic.Int64{})
	counter.(*atomic.Int64).Add(1)
	if err := f.failValidation[collectionID]; err != nil {
		return false, err
	}
	switch collectionID % recoveryCases {
	case caseDropped, caseLiveNotRecoverable:
		return false, nil
	}
	return true, nil
}

func (f *recoveryFixture) project(_ context.Context, id int64) ([]LoadableSegment, error) {
	a, b := fixtureBases(id)
	switch id % recoveryCases {
	case caseStale:
		return []LoadableSegment{a, fixtureSegment(id, 3, 1)}, nil
	case caseBootstrap:
		return []LoadableSegment{a}, nil
	case caseVersionHistory:
		return []LoadableSegment{a, fixtureSegment(id, 2, 3)}, nil
	case caseProjectionFailure, caseBootstrapProjectionFailure:
		return nil, errFixtureProjection
	case caseManifestRegression:
		return []LoadableSegment{a, fixtureSegment(id, 2, 3)}, nil
	}
	return []LoadableSegment{a, b}, nil
}

func (f *recoveryFixture) recover(ctx context.Context, workers int) (*dataViewManager, error) {
	manager, err := RecoverManager(ctx, f.catalog, f.validate, f.project, f.live, f.vchannels,
		WithRecoveryConcurrency(workers))
	if err != nil {
		return nil, err
	}
	return manager.(*dataViewManager), nil
}

type recoveredStateSummary struct {
	Latest   string
	Versions map[string][]byte
	Stats    map[string]map[int64]SegmentStats
}

func summarizeRecoveredStates(t *testing.T, m *dataViewManager) map[int64]recoveredStateSummary {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[int64]recoveredStateSummary, len(m.states))
	for id, state := range m.states {
		state.mu.Lock()
		summary := recoveredStateSummary{
			Versions: make(map[string][]byte, len(state.versions)),
			Stats:    make(map[string]map[int64]SegmentStats, len(state.versions)),
		}
		if state.latest != nil {
			summary.Latest = dataVersionKey(state.latest.view.GetDataVersion())
		}
		for key, entry := range state.versions {
			require.Zero(t, entry.refs.count)
			require.False(t, entry.isTombstone)
			summary.Versions[key.String()] = marshalDeterministic(t, entry.view)
			summary.Stats[key.String()] = entry.stats
		}
		state.mu.Unlock()
		out[id] = summary
	}
	return out
}

// accessResults records what Latest and Get serve for every Collection.
func accessResults(ctx context.Context, m *dataViewManager, collections int64) map[int64][]string {
	out := make(map[int64][]string)
	for id := int64(0); id <= collections+2; id++ {
		var results []string
		describe := func(ref DataViewRef, err error) string {
			if err != nil {
				return "error: " + err.Error()
			}
			if ref == nil {
				return "nil"
			}
			defer ref.Deref()
			return dataVersionKey(ref.Version())
		}
		results = append(results, describe(m.Latest(ctx, id)))
		for _, v := range []*viewpb.DataVersion{version(1, 0), version(2, 0), version(2, 1), version(3, 2)} {
			results = append(results, describe(m.Get(ctx, id, v)))
		}
		out[id] = results
	}
	return out
}

func TestRecoverManagerParallelMatchesSerial(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const collections = 240
	serial := newRecoveryFixture(collections, nil)
	parallel := newRecoveryFixture(collections, nil)

	serialManager, err := serial.recover(ctx, 1)
	require.NoError(t, err)
	parallelManager, err := parallel.recover(ctx, 8)
	require.NoError(t, err)

	serialSaved, serialDropped := serial.catalog.writes()
	parallelSaved, parallelDropped := parallel.catalog.writes()
	require.Equal(t, serialSaved, parallelSaved)
	require.Equal(t, serialDropped, parallelDropped)
	require.NotEmpty(t, serialSaved, "the fixture rebuilds stale and bootstraps new snapshots")
	require.NotEmpty(t, serialDropped, "the fixture deletes dropped collections")
	require.Equal(t, serial.catalog.contents(t), parallel.catalog.contents(t))

	serialStates := summarizeRecoveredStates(t, serialManager)
	require.Equal(t, serialStates, summarizeRecoveredStates(t, parallelManager))
	for id := int64(1); id <= collections; id++ {
		_, recovered := serialStates[id]
		switch id % recoveryCases {
		case caseDropped, caseBootstrapProjectionFailure, caseLiveNotRecoverable:
			require.False(t, recovered, "collection %d", id)
		case caseBootstrap:
			require.Equal(t, id%20 != caseBootstrap, serialStates[id].Latest == "1/0", "collection %d", id)
		default:
			require.True(t, recovered, "collection %d", id)
		}
	}
	require.Equal(t, "2/1", serialStates[recoveryCases+caseStale].Latest, "stale snapshots are rebuilt")
	require.Equal(t, "2/0", serialStates[caseStale].Latest, "a failed rebuild keeps the recovered snapshot")
	require.Equal(t, "2/1", serialStates[caseVersionHistory].Latest)
	require.Len(t, serialStates[caseVersionHistory].Versions, 3)
	require.NotEmpty(t, serialStates[recoveryCases+caseUpToDate].Stats["2/0"], "recovery fills the footprint")
	serialManager.publicationMu.Lock()
	parallelManager.publicationMu.Lock()
	require.Equal(t, serialManager.published, parallelManager.published)
	require.NotEmpty(t, serialManager.published)
	serialManager.publicationMu.Unlock()
	parallelManager.publicationMu.Unlock()

	// Every discovered and every live Collection is validated exactly once.
	for _, f := range []*recoveryFixture{serial, parallel} {
		validated := 0
		f.validations.Range(func(key, value any) bool {
			validated++
			require.Equal(t, int64(1), value.(*atomic.Int64).Load(), "collection %d", key)
			return true
		})
		require.Equal(t, collections, validated)
	}

	require.Equal(t, accessResults(ctx, serialManager, collections), accessResults(ctx, parallelManager, collections))
}

func TestRecoverManagerParallelFailuresMatchSerial(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const collections = 120
	errFirst := errors.New("first failure")
	errSecond := errors.New("second failure")
	for _, tc := range []struct {
		name   string
		inject func(*recoveryFixture)
		expect error
		// wantDropped is the number of prefixes deleted before the failure;
		// -1 only requires that every deleted prefix is unrecoverable.
		wantDropped int
	}{
		{
			name: "discovered-validator",
			inject: func(f *recoveryFixture) {
				f.failValidation[recoveryCases*2+caseStale] = errFirst
				f.failValidation[recoveryCases*6+caseNotLive] = errSecond
			},
			expect: errFirst,
		},
		{
			name: "cleanup",
			inject: func(f *recoveryFixture) {
				f.catalog.dropErrs[recoveryCases*3+caseDropped] = errFirst
				f.catalog.dropErrs[recoveryCases*7+caseDropped] = errSecond
			},
			expect:      errFirst,
			wantDropped: -1,
		},
		{
			name: "live-validator",
			inject: func(f *recoveryFixture) {
				f.failValidation[recoveryCases*2+caseBootstrap] = errFirst
				f.failValidation[recoveryCases*5+caseLiveNotRecoverable] = errSecond
			},
			expect:      errFirst,
			wantDropped: collections / recoveryCases,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var final []map[string][][]byte
			var states []map[int64]recoveredStateSummary
			for _, workers := range []int{1, 8} {
				f := newRecoveryFixture(collections, tc.inject)
				_, err := f.recover(ctx, workers)
				require.ErrorIs(t, err, tc.expect, "workers=%d", workers)
				require.NotErrorIs(t, err, errSecond, "workers=%d: the lowest failure is reported", workers)
				saved, dropped := f.catalog.writes()
				require.Empty(t, saved, "workers=%d: no reconciliation write before every validation succeeded", workers)
				for _, id := range dropped {
					require.Equal(t, int64(caseDropped), id%recoveryCases, "workers=%d: only unrecoverable prefixes are deleted", workers)
				}
				if tc.wantDropped >= 0 {
					require.Len(t, dropped, tc.wantDropped, "workers=%d", workers)
				}

				// The retry lists again and converges to the same state.
				clear(f.failValidation)
				clear(f.catalog.dropErrs)
				manager, err := f.recover(ctx, workers)
				require.NoError(t, err)
				final = append(final, f.catalog.contents(t))
				states = append(states, summarizeRecoveredStates(t, manager))
			}
			require.Equal(t, final[0], final[1])
			require.Equal(t, states[0], states[1])
		})
	}
}

func TestRunRecoveryTasks(t *testing.T) {
	failAt := func(i int) error { return fmt.Errorf("task %d failed", i) }
	for _, workers := range []int{0, 1, 2, 3, 64} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			require.NoError(t, runRecoveryTasks(workers, 0, func(int) error {
				t.Error("no task expected")
				return nil
			}))

			// Every task runs exactly once, at most workers at a time.
			var calls [100]atomic.Int64
			var active, peak atomic.Int64
			require.NoError(t, runRecoveryTasks(workers, len(calls), func(i int) error {
				running := active.Add(1)
				defer active.Add(-1)
				for {
					current := peak.Load()
					if running <= current || peak.CompareAndSwap(current, running) {
						break
					}
				}
				calls[i].Add(1)
				return nil
			}))
			for i := range calls {
				require.Equal(t, int64(1), calls[i].Load(), "task %d", i)
			}
			require.LessOrEqual(t, peak.Load(), int64(max(workers, 1)))

			// The lowest failed index is returned, and every lower index has run.
			var started [100]atomic.Bool
			err := runRecoveryTasks(workers, len(started), func(i int) error {
				started[i].Store(true)
				if i == 40 || i == 70 {
					return failAt(i)
				}
				return nil
			})
			require.EqualError(t, err, failAt(40).Error())
			for i := range 40 {
				require.True(t, started[i].Load(), "task %d", i)
			}
			if workers <= 1 {
				for i := 41; i < len(started); i++ {
					require.False(t, started[i].Load(), "a serial pass stops at the failure: task %d", i)
				}
			}
		})
	}

	// A lower index failing after a higher one still determines the error, and
	// no index starts once a failure is recorded.
	release := make(chan struct{})
	var started [10]atomic.Bool
	err := runRecoveryTasks(2, len(started), func(i int) error {
		started[i].Store(true)
		switch i {
		case 0:
			<-release
			return failAt(0)
		case 1:
			close(release)
			return failAt(1)
		}
		return nil
	})
	require.EqualError(t, err, failAt(0).Error())
	for i := 2; i < len(started); i++ {
		require.False(t, started[i].Load(), "task %d started after a failure", i)
	}
}

// referenceMembershipEqual is the comparison of canonical deep clones that the
// clone-free path of dataViewMembershipEqual must match.
func referenceMembershipEqual(left, right *viewpb.DataViewOfCollection) bool {
	left = canonicalDataViewClone(left)
	right = canonicalDataViewClone(right)
	if left != nil {
		left.DataVersion = nil
	}
	if right != nil {
		right.DataVersion = nil
	}
	return proto.Equal(left, right)
}

func TestDataViewMembershipEqualMatchesCanonicalClones(t *testing.T) {
	// withoutDataVersion copies the collection fields, and isCanonicalDataView
	// checks every per-Segment array: a new field must update both.
	for message, want := range map[proto.Message][]protoreflect.Name{
		&viewpb.DataViewOfCollection{}: {"collection_id", "shards", "data_version", "schema_version"},
		&viewpb.DataViewOfShard{}:      {"vchannel", "partitions", "transform_start_after_timetick"},
		&viewpb.DataViewOfPartition{}:  {"partition_id", "segment_ids", "segment_manifest_versions"},
	} {
		fields := message.ProtoReflect().Descriptor().Fields()
		names := make([]protoreflect.Name, 0, fields.Len())
		for i := range fields.Len() {
			names = append(names, fields.Get(i).Name())
		}
		require.ElementsMatch(t, want, names, "%T", message)
	}

	unknown := func(numbers ...protowire.Number) []byte {
		var raw []byte
		for _, number := range numbers {
			raw = protowire.AppendTag(raw, number, protowire.VarintType)
			raw = protowire.AppendVarint(raw, uint64(number))
		}
		return raw
	}
	partition := func(v *viewpb.DataViewOfCollection) *viewpb.DataViewOfPartition { return v.Shards[0].Partitions[0] }
	mutations := map[string]func(*viewpb.DataViewOfCollection){
		"base":       func(*viewpb.DataViewOfCollection) {},
		"version":    func(v *viewpb.DataViewOfCollection) { v.DataVersion = version(3, 1) },
		"no-version": func(v *viewpb.DataViewOfCollection) { v.DataVersion = nil },
		"collection": func(v *viewpb.DataViewOfCollection) { v.CollectionId++ },
		"schema":     func(v *viewpb.DataViewOfCollection) { v.SchemaVersion++ },
		"transform":  func(v *viewpb.DataViewOfCollection) { v.Shards[0].TransformStartAfterTimetick++ },
		"manifest":   func(v *viewpb.DataViewOfCollection) { partition(v).SegmentManifestVersions[1]++ },
		"extra-segment": func(v *viewpb.DataViewOfCollection) {
			p := partition(v)
			p.SegmentIds = append(p.SegmentIds, 999)
			p.SegmentManifestVersions = append(p.SegmentManifestVersions, 1)
		},
		"unsorted-segments": func(v *viewpb.DataViewOfCollection) {
			p := partition(v)
			p.SegmentIds[0], p.SegmentIds[1] = p.SegmentIds[1], p.SegmentIds[0]
			p.SegmentManifestVersions[0], p.SegmentManifestVersions[1] = p.SegmentManifestVersions[1], p.SegmentManifestVersions[0]
		},
		"duplicate-segment": func(v *viewpb.DataViewOfCollection) {
			p := partition(v)
			p.SegmentIds = append(p.SegmentIds, p.SegmentIds[0])
			p.SegmentManifestVersions = append(p.SegmentManifestVersions, 1)
		},
		"zero-arrays": func(v *viewpb.DataViewOfCollection) {
			partition(v).SegmentManifestVersions = make([]int64, len(partition(v).SegmentIds))
		},
		"empty-shard": func(v *viewpb.DataViewOfCollection) {
			v.Shards = append(v.Shards, &viewpb.DataViewOfShard{Vchannel: "z"})
		},
		"unsorted-shards": func(v *viewpb.DataViewOfCollection) {
			v.Shards = append(v.Shards, &viewpb.DataViewOfShard{Vchannel: "a"})
		},
		"duplicate-shard": func(v *viewpb.DataViewOfCollection) {
			v.Shards = append(v.Shards, &viewpb.DataViewOfShard{Vchannel: v.Shards[0].Vchannel})
		},
		"unsorted-partitions": func(v *viewpb.DataViewOfCollection) {
			v.Shards[0].Partitions = append([]*viewpb.DataViewOfPartition{{PartitionId: 99}}, v.Shards[0].Partitions...)
		},
		"empty-partition": func(v *viewpb.DataViewOfCollection) {
			v.Shards[0].Partitions = append(v.Shards[0].Partitions, &viewpb.DataViewOfPartition{PartitionId: 99})
		},
		"unknown":           func(v *viewpb.DataViewOfCollection) { v.ProtoReflect().SetUnknown(unknown(98, 99)) },
		"unknown-reordered": func(v *viewpb.DataViewOfCollection) { v.ProtoReflect().SetUnknown(unknown(99, 98)) },
		"shard-unknown":     func(v *viewpb.DataViewOfCollection) { v.Shards[0].ProtoReflect().SetUnknown(unknown(97)) },
		"nil":               nil,
		"empty":             nil,
	}
	base := fixtureUpToDateView(1)
	build := func(name string) *viewpb.DataViewOfCollection {
		switch name {
		case "nil":
			return nil
		case "empty":
			return &viewpb.DataViewOfCollection{}
		}
		view := proto.Clone(base).(*viewpb.DataViewOfCollection)
		mutations[name](view)
		return view
	}
	for leftName := range mutations {
		for rightName := range mutations {
			left, right := build(leftName), build(rightName)
			leftBefore, rightBefore := marshalDeterministic(t, left), marshalDeterministic(t, right)
			require.Equal(t, referenceMembershipEqual(left, right), dataViewMembershipEqual(left, right), "%s vs %s", leftName, rightName)
			require.Equal(t, leftBefore, marshalDeterministic(t, left), "%s is not mutated", leftName)
			require.Equal(t, rightBefore, marshalDeterministic(t, right), "%s is not mutated", rightName)
		}
	}
	require.True(t, isCanonicalDataView(base))
	require.False(t, isCanonicalDataView(build("unsorted-segments")))
}
