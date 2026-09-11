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

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/storage/localmigrate"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
func (mr *MilvusRoles) migrateLocalStorageLayoutOrDie(ctx context.Context, params *paramtable.ComponentParam) {
	if !mr.Local || params.CommonCfg.StorageType.GetValue() != "local" {
		return
	}
	report, err := mr.migrateLocalStorageLayout(ctx, params)
	if err != nil {
		mlog.Fatal(ctx, "local storage layout migration failed, refusing to start",
			mlog.String("root", params.LocalStorageCfg.Path.GetValue()),
			mlog.String("report", report.String()),
			mlog.Int("conflictCount", len(report.Conflicts)),
			mlog.Strings("conflictSample", firstStrings(report.Conflicts, maxLoggedConflicts)),
			mlog.Err(err))
		return
	}
	// Always report the outcome. "Checked the root and found nothing to move"
	// and "never ran" must not look identical in a startup log.
	mlog.Info(ctx, "local storage layout migration finished",
		mlog.String("report", report.String()))
}

func (mr *MilvusRoles) migrateLocalStorageLayout(ctx context.Context, params *paramtable.ComponentParam) (*localmigrate.Report, error) {
	if !mr.Local || params.CommonCfg.StorageType.GetValue() != "local" {
		return nil, nil
	}
	root := params.LocalStorageCfg.Path.GetValue()
	timeout := params.LocalStorageCfg.LayoutMigrationTimeout.GetAsDuration(time.Second)
	if timeout > 0 {
		// This blocks startup ahead of etcd and every component, so it runs on a
		// bounded budget: a stalled filesystem has to surface as a failed start
		// rather than as an instance that never comes up and says nothing.
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	logger := mlog.With(mlog.String("root", root))
	logger.Info(ctx, "checking local storage layout for legacy directories to migrate",
		mlog.Duration("timeout", timeout))

	planned, started, completed := 0, 0, 0
	report, err := localmigrate.Migrate(ctx, root, localmigrate.Options{
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
			// Name every directory up front: the plan is what a later failure or
			// timeout has to be reconciled against.
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
	if errors.Is(err, context.DeadlineExceeded) {
		// Nothing is broken and nothing needs repairing: the migration resumes
		// from whatever is left, so the operator's action is to grant more time.
		return report, merr.Wrapf(err,
			"local storage layout migration exceeded its %s budget after moving %d of %d directories; "+
				"it resumes where it stopped, so raise %s and restart",
			timeout, completed, planned, params.LocalStorageCfg.LayoutMigrationTimeout.Key)
	}
	return report, err
}

func firstStrings(values []string, limit int) []string {
	if len(values) <= limit {
		return values
	}
	return values[:limit]
}
