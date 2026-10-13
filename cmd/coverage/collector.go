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

// Package coverage implements local snapshots for instrumented E2E servers.
package coverage

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime/coverage"
	"strconv"
	"strings"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type collector struct {
	root          string
	dump          func()
	writeMeta     func(string) error
	writeCounters func(string) error
	// A failed native dump can lose an interval after reset. Never advertise
	// a later snapshot as successful after such a failure.
	nativeError error
}

func start(ctx context.Context, root string, dump func()) error {
	root = filepath.Clean(root)
	if !filepath.IsAbs(root) {
		return merr.WrapErrParameterInvalidMsg("MILVUS_COVERAGE_DIR must be absolute")
	}
	for key, value := range map[string]string{
		"GCOV_PREFIX":       filepath.Join(root, "cpp"),
		"GCOV_PREFIX_STRIP": "0",
		"GCOV_ERROR_FILE":   filepath.Join(root, "gcov-errors.log"),
	} {
		if existing := os.Getenv(key); existing != "" && existing != value {
			return merr.WrapErrParameterInvalidMsg("%s must be %q for server coverage", key, value)
		}
		if err := os.Setenv(key, value); err != nil {
			return err
		}
	}
	for _, dir := range []string{"requests", "runs", "cpp", "go"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o755); err != nil {
			return err
		}
	}
	for _, dir := range []string{"cpp", "runs", "requests"} {
		entries, err := os.ReadDir(filepath.Join(root, dir))
		if err != nil {
			return err
		}
		if len(entries) != 0 {
			return merr.WrapErrParameterInvalidMsg("coverage directory %s is not fresh", root)
		}
	}
	c := &collector{root: root, dump: dump, writeMeta: coverage.WriteMetaDir, writeCounters: coverage.WriteCountersDir}
	// Verify actual Go instrumentation before publishing the ready marker.
	if err := c.writeMeta(filepath.Join(root, "go")); err != nil {
		return merr.Wrap(err, "server was not built with Go coverage")
	}
	ready, err := os.OpenFile(filepath.Join(root, "ready"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return merr.Wrap(err, "coverage output is already owned; use a fresh directory")
	}
	_, writeErr := ready.WriteString(strconv.Itoa(os.Getpid()))
	if err := errors.Join(writeErr, ready.Close()); err != nil {
		return err
	}
	go c.serve(ctx)
	return nil
}

func validID(id string) bool {
	_, err := hex.DecodeString(id)
	return err == nil && len(id) == 32 && strings.ToLower(id) == id
}

func (c *collector) serve(ctx context.Context) {
	ticks := time.Tick(500 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticks:
			entries, err := os.ReadDir(filepath.Join(c.root, "requests"))
			if err != nil {
				mlog.RatedWarn(ctx, 0.1, "read coverage requests", mlog.Err(err))
				continue
			}
			for _, entry := range entries {
				if !entry.Type().IsRegular() || !validID(entry.Name()) {
					continue
				}
				c.handle(ctx, entry.Name())
			}
		}
	}
}

func (c *collector) handle(ctx context.Context, id string) {
	request := filepath.Join(c.root, "requests", id)
	run := filepath.Join(c.root, "runs", id)
	// A previously used ID must never overwrite an existing snapshot.
	if err := os.Mkdir(run, 0o755); err != nil {
		mlog.Warn(ctx, "claim coverage snapshot", mlog.Err(err))
	} else if err := c.snapshot(run); err != nil {
		mlog.Warn(ctx, "export coverage snapshot", mlog.Err(err))
		if writeErr := os.WriteFile(filepath.Join(run, "error"), []byte(err.Error()), 0o644); writeErr != nil {
			mlog.Warn(ctx, "write coverage failure marker", mlog.Err(writeErr))
		}
	}
	if err := os.Remove(request); err != nil {
		mlog.Warn(ctx, "remove coverage request", mlog.Err(err))
	}
}

func (c *collector) snapshot(run string) error {
	goDir := filepath.Join(run, "go")
	if err := os.Mkdir(goDir, 0o755); err != nil {
		return err
	}
	if err := c.writeMeta(goDir); err != nil {
		return merr.Wrap(err, "Go metadata")
	}
	if err := c.writeCounters(goDir); err != nil {
		return merr.Wrap(err, "Go counters")
	}
	if c.nativeError != nil {
		return c.nativeError
	}
	errorFile := filepath.Join(c.root, "gcov-errors.log")
	if err := os.WriteFile(errorFile, nil, 0o644); err != nil {
		return err
	}
	c.dump()
	diagnostics, err := os.ReadFile(errorFile)
	if err != nil {
		c.nativeError = err
		return err
	}
	if len(diagnostics) != 0 {
		c.nativeError = merr.WrapErrServiceInternalMsg("gcov dump failed: %s", diagnostics)
		return c.nativeError
	}
	cppRoot := filepath.Join(c.root, "cpp")
	count := 0
	err = filepath.WalkDir(cppRoot, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.Type().IsRegular() || filepath.Ext(path) != ".gcda" {
			return nil
		}
		relative, err := filepath.Rel(cppRoot, path)
		if err != nil {
			return err
		}
		if err := copyFile(path, filepath.Join(run, "cpp", relative)); err != nil {
			return err
		}
		count++
		return nil
	})
	if err != nil {
		return merr.Wrap(err, "C++ counters")
	}
	if count == 0 {
		return merr.WrapErrServiceInternalMsg("C++ dump produced no .gcda files")
	}
	manifest, err := json.Marshal(struct {
		PID        int       `json:"pid"`
		ExportedAt time.Time `json:"exported_at"`
		GCDAFiles  int       `json:"gcda_files"`
		Scope      string    `json:"scope"`
	}{os.Getpid(), time.Now().UTC(), count, "cumulative since process start"})
	if err != nil {
		return err
	}
	temp := filepath.Join(run, "done.json.tmp")
	if err := os.WriteFile(temp, manifest, 0o644); err != nil {
		return err
	}
	return os.Rename(temp, filepath.Join(run, "done.json"))
}

func copyFile(source, destination string) error {
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return err
	}
	src, err := os.Open(source)
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(dst, src)
	return errors.Join(copyErr, dst.Close())
}
