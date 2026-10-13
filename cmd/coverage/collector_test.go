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

package coverage

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func fixture(t *testing.T) *collector {
	t.Helper()
	root := t.TempDir()
	for _, dir := range []string{"requests", "runs", "cpp"} {
		if err := os.Mkdir(filepath.Join(root, dir), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write := func(name string) func(string) error {
		return func(dir string) error { return os.WriteFile(filepath.Join(dir, name), []byte("go data"), 0o644) }
	}
	return &collector{
		root:          root,
		writeMeta:     write("covmeta.test"),
		writeCounters: write("covcounters.test"),
		dump: func() {
			if err := os.WriteFile(filepath.Join(root, "cpp", "probe.gcda"), []byte("native data"), 0o644); err != nil {
				t.Fatal(err)
			}
		},
	}
}

func requestSnapshot(t *testing.T, c *collector, id string) string {
	t.Helper()
	request := filepath.Join(c.root, "requests", id)
	if err := os.WriteFile(request, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	c.handle(t.Context(), id)
	if _, err := os.Stat(request); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("request not consumed: %v", err)
	}
	return filepath.Join(c.root, "runs", id)
}

func TestSnapshotPreservesRawDataAndRejectsReuse(t *testing.T) {
	c := fixture(t)
	id := strings.Repeat("a", 32)
	run := requestSnapshot(t, c, id)
	for _, name := range []string{"done.json", "go/covmeta.test", "go/covcounters.test", "cpp/probe.gcda"} {
		if _, err := os.Stat(filepath.Join(run, name)); err != nil {
			t.Fatal(err)
		}
	}
	c.dump = func() { t.Fatal("duplicate request dumped counters again") }
	requestSnapshot(t, c, id)
	if _, err := os.Stat(filepath.Join(run, "error")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("duplicate request changed existing result: %v", err)
	}
	// The accumulator belongs to the process, not to an individual snapshot.
	if _, err := os.Stat(filepath.Join(c.root, "cpp", "probe.gcda")); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotFailuresNeverPublishCompletion(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prepare func(*collector)
		want    string
	}{
		{"Go metadata", func(c *collector) { c.writeMeta = func(string) error { return os.ErrPermission } }, "Go metadata"},
		{"Go counters", func(c *collector) { c.writeCounters = func(string) error { return os.ErrPermission } }, "Go counters"},
		{"missing native counters", func(c *collector) { c.dump = func() {} }, "no .gcda"},
		{"native dump error", func(c *collector) {
			c.dump = func() {
				if err := os.WriteFile(filepath.Join(c.root, "gcov-errors.log"), []byte("profiling: permission denied"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
		}, "gcov dump failed"},
		{"copy error", func(c *collector) {
			original := c.dump
			c.dump = func() {
				original()
				// Block creation of the destination directory.
				if err := os.WriteFile(filepath.Join(c.root, "runs", strings.Repeat("b", 32), "cpp"), nil, 0o644); err != nil {
					t.Fatal(err)
				}
			}
		}, "C++ counters"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := fixture(t)
			tc.prepare(c)
			run := requestSnapshot(t, c, strings.Repeat("b", 32))
			data, err := os.ReadFile(filepath.Join(run, "error"))
			if err != nil || !strings.Contains(string(data), tc.want) {
				t.Fatalf("failure marker: %q, %v; want %q", data, err, tc.want)
			}
			if _, err := os.Stat(filepath.Join(run, "done.json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed snapshot published completion: %v", err)
			}
		})
	}
}

func TestNativeFailureIsSticky(t *testing.T) {
	c := fixture(t)
	c.dump = func() {
		if err := os.WriteFile(filepath.Join(c.root, "gcov-errors.log"), []byte("write failed"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	requestSnapshot(t, c, strings.Repeat("c", 32))
	c.dump = func() { t.Fatal("native dump retried after losing an interval") }
	run := requestSnapshot(t, c, strings.Repeat("d", 32))
	if _, err := os.Stat(filepath.Join(run, "error")); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotIDs(t *testing.T) {
	for _, id := range []string{"", "../escape", strings.Repeat("A", 32), strings.Repeat("g", 32), strings.Repeat("a", 31)} {
		if validID(id) {
			t.Errorf("accepted invalid ID %q", id)
		}
	}
	if !validID("0123456789abcdef0123456789abcdef") {
		t.Fatal("rejected valid ID")
	}
}

func TestStartRejectsConflictingOrStaleOutput(t *testing.T) {
	for _, name := range []string{"relative", "prefix", "strip", "stale"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			for _, key := range []string{"GCOV_PREFIX", "GCOV_PREFIX_STRIP", "GCOV_ERROR_FILE"} {
				t.Setenv(key, "")
			}
			switch name {
			case "relative":
				root = "relative"
			case "prefix":
				t.Setenv("GCOV_PREFIX", "/another-process")
			case "strip":
				t.Setenv("GCOV_PREFIX_STRIP", "1")
			case "stale":
				if err := os.MkdirAll(filepath.Join(root, "cpp", "old-build"), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			if err := start(t.Context(), root, func() {}); err == nil {
				t.Fatal("accepted invalid output")
			}
		})
	}
}
