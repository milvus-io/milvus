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

package roles

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/storage/localmigrate"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// maxLoggedConflicts bounds the conflict sample in the failure log. The full set
// stays in the returned Report; a startup log must not grow to the size of the
// storage root.
const maxLoggedConflicts = 100

// migrateLocalStorageLayoutOrDie moves the double-joined 3.0 local layout and
// recognized 2.6 working-directory index directories before any component opens
// data. It is enabled automatically for standalone with common.storageType=local.
// Directories are merged without overwriting files; cross-filesystem moves copy.
func (mr *MilvusRoles) migrateLocalStorageLayoutOrDie(ctx context.Context, params *paramtable.ComponentParam) bool {
	if !mr.Local || params.CommonCfg.StorageType.GetValue() != "local" {
		return true
	}
	migrationCtx, cancel := mr.localStorageMigrationContext(ctx)
	defer cancel()

	report, err := mr.migrateLocalStorageLayout(migrationCtx, params)
	if err != nil {
		if migrationCtx.Err() != nil {
			mlog.Info(ctx, "local storage layout migration canceled before component startup",
				mlog.String("root", params.LocalStorageCfg.Path.GetValue()),
				mlog.Err(err))
			return false
		}
		mlog.Fatal(ctx, "local storage layout migration failed, refusing to start",
			mlog.String("root", params.LocalStorageCfg.Path.GetValue()),
			mlog.String("report", report.String()),
			mlog.Int("conflictCount", len(report.Conflicts)),
			mlog.Strings("conflictSample", firstStrings(report.Conflicts, maxLoggedConflicts)),
			mlog.Err(err))
		return false
	}
	// A shutdown can race with the final migration operation. Do not continue
	// into etcd/component startup after the signal handler has requested exit.
	if migrationCtx.Err() != nil {
		mlog.Info(ctx, "local storage layout migration completed during shutdown; skipping component startup",
			mlog.String("root", params.LocalStorageCfg.Path.GetValue()))
		return false
	}
	// Always report the outcome. "Checked the root and found nothing to move"
	// and "never ran" must not look identical in a startup log.
	mlog.Info(ctx, "local storage layout migration finished",
		mlog.String("report", report.String()))
	return true
}

func (mr *MilvusRoles) localStorageMigrationContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	if mr.closed == nil {
		return ctx, cancel
	}
	go func() {
		select {
		case <-mr.closed:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

func (mr *MilvusRoles) migrateLocalStorageLayout(ctx context.Context, params *paramtable.ComponentParam) (*localmigrate.Report, error) {
	if !mr.Local || params.CommonCfg.StorageType.GetValue() != "local" {
		return nil, nil
	}
	root := params.LocalStorageCfg.Path.GetValue()

	logger := mlog.With(mlog.String("root", root))
	logger.Info(ctx, "checking local storage layout for legacy directories to migrate")

	planned, started, completed := 0, 0, 0
	return localmigrate.Migrate(ctx, root, localmigrate.Options{
		LegacyPrefix: params.MinioCfg.RootPath.GetValue(),
		OnPlan: func(dirs []localmigrate.Dir) {
			planned = len(dirs)
			files, bytes := 0, int64(0)
			for _, dir := range dirs {
				files += dir.Files
				bytes += dir.Bytes
			}
			logger.Info(ctx, "local storage layout migration plan decided",
				mlog.Int("directories", planned),
				mlog.Int("files", files),
				mlog.Int64("bytes", bytes))
			// Report the full plan before moving any directory.
			for index, dir := range dirs {
				logger.Info(ctx, "local storage layout migration will move directory",
					mlog.Int("index", index+1), mlog.Int("total", planned),
					mlog.String("source", dir.Source), mlog.String("target", dir.Target),
					mlog.Int("files", dir.Files), mlog.Int64("bytes", dir.Bytes))
			}
		},
		OnDirStart: func(dir localmigrate.Dir) {
			started++
			logger.Info(ctx, "moving local storage layout directory",
				mlog.Int("index", started), mlog.Int("total", planned),
				mlog.String("source", dir.Source), mlog.String("target", dir.Target),
				mlog.Int("files", dir.Files), mlog.Int64("bytes", dir.Bytes))
		},
		OnDirDone: func(dir localmigrate.Dir, elapsed time.Duration) {
			completed++
			logger.Info(ctx, "moved local storage layout directory",
				mlog.Int("index", completed), mlog.Int("total", planned),
				mlog.String("source", dir.Source), mlog.String("target", dir.Target),
				mlog.Int("files", dir.Files), mlog.Int64("bytes", dir.Bytes),
				mlog.Duration("elapsed", elapsed))
		},
	})
}

func firstStrings(values []string, limit int) []string {
	if len(values) <= limit {
		return values
	}
	return values[:limit]
}
