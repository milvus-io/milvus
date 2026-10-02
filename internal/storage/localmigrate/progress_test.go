// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package localmigrate

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The plan is what a slow or interrupted migration has to be reconciled against,
// so every directory must be named before anything moves, and again as it moves
// and once it is done.
func TestMigrateReportsPlanAndEveryDirectory(t *testing.T) {
	t.Chdir(t.TempDir())
	root := t.TempDir()
	layouts := []string{"index_files", "insert_log", "json_stats"}
	for _, layout := range layouts {
		writeFile(t, filepath.Join(displaced(root), layout, "1", "data"), layout)
	}

	var plan, starts, dones []Dir
	var elapsed []time.Duration
	report, err := Migrate(t.Context(), root, Options{
		LegacyPrefix: "files",
		OnPlan:       func(dirs []Dir) { plan = append(plan, dirs...) },
		OnDirStart:   func(dir Dir) { starts = append(starts, dir) },
		OnDirDone: func(dir Dir, took time.Duration) {
			dones = append(dones, dir)
			elapsed = append(elapsed, took)
		},
	})
	require.NoError(t, err)

	// Discovery walks a map for CWD leaves, so the order is fixed explicitly.
	sources := make([]string, 0, len(plan))
	targets := make([]string, 0, len(plan))
	for _, dir := range plan {
		sources = append(sources, dir.Source)
		targets = append(targets, dir.Target)
		assert.Equal(t, 1, dir.Files)
		assert.Positive(t, dir.Bytes)
	}
	wantSources := make([]string, 0, len(layouts))
	wantTargets := make([]string, 0, len(layouts))
	for _, layout := range layouts {
		wantSources = append(wantSources, filepath.Join(displaced(root), layout))
		wantTargets = append(wantTargets, filepath.Join(root, layout))
	}
	assert.Equal(t, wantSources, sources)
	assert.Equal(t, wantTargets, targets)

	// Every planned directory is reported starting and finishing, in plan order.
	assert.Equal(t, plan, starts)
	assert.Equal(t, plan, dones)
	assert.Len(t, elapsed, len(layouts))
	assert.Positive(t, report.Elapsed)
}

func TestMigrateWithoutLegacyDirectoriesReportsAnEmptyPlan(t *testing.T) {
	t.Chdir(t.TempDir())
	root := t.TempDir()

	planned := 0
	moved := 0
	report, err := Migrate(t.Context(), root, Options{
		LegacyPrefix: "files",
		OnPlan:       func(dirs []Dir) { planned = len(dirs) },
		OnDirStart:   func(Dir) { moved++ },
	})

	require.NoError(t, err)
	assert.Zero(t, planned)
	assert.Zero(t, moved)
	assert.Zero(t, report.Renamed+report.Copied)
}

// An expired deadline must surface as a failure rather than a partially applied
// layout that looks finished.
func TestMigrateExpiredDeadlineFailsBeforeMoving(t *testing.T) {
	t.Chdir(t.TempDir())
	root := t.TempDir()
	key := filepath.Join("insert_log", "1", "data")
	writeFile(t, filepath.Join(displaced(root), key), "payload")

	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	_, err := Migrate(ctx, root, Options{LegacyPrefix: "files"})

	require.ErrorIs(t, err, context.DeadlineExceeded)
	// The source is untouched, so a later run still has everything to move.
	assert.Equal(t, "payload", readFile(t, filepath.Join(displaced(root), key)))
}

// Cancellation is safe because the migration is resumable: whatever has not
// moved is rediscovered, so a restart with more time finishes the job.
func TestMigrateCancelledMidwayResumesOnRestart(t *testing.T) {
	t.Chdir(t.TempDir())
	root := t.TempDir()
	layouts := []string{"index_files", "insert_log", "json_stats"}
	for _, layout := range layouts {
		writeFile(t, filepath.Join(displaced(root), layout, "1", "data"), layout)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	first, err := Migrate(ctx, root, Options{
		LegacyPrefix: "files",
		// Stop after the first directory is fully in place.
		OnDirDone: func(Dir, time.Duration) { cancel() },
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, first.Renamed+first.Copied)
	assert.Equal(t, "index_files", readFile(t, filepath.Join(root, "index_files", "1", "data")))

	second, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
	require.NoError(t, err)
	assert.Equal(t, len(layouts)-1, second.Renamed+second.Copied)
	for _, layout := range layouts {
		assert.Equal(t, layout, readFile(t, filepath.Join(root, layout, "1", "data")))
	}
}
