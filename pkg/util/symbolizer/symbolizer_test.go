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

package symbolizer

import (
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
)

// TestPanicExitCode is a regression test for
// https://github.com/milvus-io/milvus/issues/53956.
//
// Merely importing this package (as cmd/roles does for every component)
// must not force core dumps: a panicking process should exit promptly
// (exit code 2) unless the operator explicitly opts into core dumps with
// GOTRACEBACK=crash.
//
// The test re-execs the test binary as a helper that panics and inspects
// how the helper died.
func TestPanicExitCode(t *testing.T) {
	if os.Getenv("SYMBOLIZER_TEST_HELPER") == "1" {
		panic("symbolizer test panic")
	}

	// Never let the helper write a core file to disk while the test runs;
	// SIGABRT is still delivered when the crash level is active.
	_ = syscall.Setrlimit(syscall.RLIMIT_CORE, &syscall.Rlimit{Cur: 0, Max: 0})

	cases := []struct {
		name        string
		gotraceback string
		wantAbort   bool
	}{
		{name: "default", gotraceback: "", wantAbort: false},
		{name: "gotraceback_all", gotraceback: "all", wantAbort: false},
		{name: "gotraceback_crash_optin", gotraceback: "crash", wantAbort: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := []string{"SYMBOLIZER_TEST_HELPER=1"}
			for _, e := range os.Environ() {
				if !strings.HasPrefix(e, "GOTRACEBACK=") && !strings.HasPrefix(e, "SYMBOLIZER_TEST_HELPER=") {
					env = append(env, e)
				}
			}
			if tc.gotraceback != "" {
				env = append(env, "GOTRACEBACK="+tc.gotraceback)
			}

			//nolint:gosec // G204: os.Args[0] is the test binary itself, re-executed
			// as a panic helper; no external input reaches the command line.
			cmd := exec.Command(os.Args[0], "-test.run=TestPanicExitCode", "-test.count=1")
			cmd.Env = env
			if err := cmd.Run(); err == nil {
				t.Fatal("helper exited 0, want it to panic")
			} else {
				exitErr, ok := err.(*exec.ExitError)
				if !ok {
					t.Fatalf("unexpected helper error: %v", err)
				}
				ws := exitErr.Sys().(syscall.WaitStatus)
				if tc.wantAbort {
					if !ws.Signaled() || ws.Signal() != syscall.SIGABRT {
						t.Fatalf("want SIGABRT for the GOTRACEBACK=crash opt-in, got %v", ws)
					}
					return
				}
				if ws.Signaled() {
					t.Fatalf("helper killed by signal %v; importing symbolizer must not force core dumps (#53956)", ws.Signal())
				}
				if ws.ExitStatus() != 2 {
					t.Fatalf("helper exit status = %d, want 2 (plain panic, no core dump)", ws.ExitStatus())
				}
			}
		})
	}
}
