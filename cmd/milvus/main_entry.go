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

package milvus

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"

	"golang.org/x/exp/slices"

	"github.com/milvus-io/milvus/cmd/asan"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Main is the process entry point, exported so a distribution can build its own
// main package around it instead of duplicating the startup preamble. The body
// is the former cmd/main.go moved unchanged apart from three adaptations: it
// reads args instead of the os.Args global (with the inner slice renamed to
// subArgs), the original if/else became an if block that always returns
// followed by an unconditional tail call, and the subprocess marker is
// stripped into a copy of args rather than in place.
//
// args is the full argument vector, program name first, as os.Args is. Main
// does not modify the caller's slice: the subprocess branch deletes an element,
// which on os.Args was invisible and on an exported API would hand the caller
// back a shortened vector with a cleared tail, so it deletes from a copy.
//
// On a zero exit status, Main returns normally after session cleanup and
// deferred checks complete. On a nonzero status (including a failed or
// signal-terminated subprocess under --run-with-subprocess, a subprocess start
// failure, or invalid --native-profile arguments), Main finishes cleanup and
// deferred checks first and then calls os.Exit with the resulting status code.
//
// A distribution installs its compiled-in hook and coordinator engine
// (pkg/extension) before calling Main; nothing runs earlier than the first
// line here except package initialization.
func Main(args []string) {
	if code := runMain(args); code != 0 {
		os.Exit(code)
	}
}

func runMain(args []string) int {
	// after 2.6.0, we enable streaming service by default.
	// TODO: after remove all streamingutil.IsStreamingServiceEnabled(), we can remove this code.
	streamingutil.SetStreamingServiceEnabled()

	defer asan.LsanDoLeakCheck()
	args, profileEnv, err := prepareNativeProfile(args, os.Environ())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	// execute command as a subprocess if the command contains "--run-with-subprocess"
	if subArgs, runSubprocess := subprocessArgs(args); runSubprocess {
		return superviseSubprocess(subArgs, profileEnv, defaultCleanChildSession)
	}

	RunMilvus(args)
	return 0
}

func superviseSubprocess(subArgs, profileEnv []string, cleanChild func(pid int) error) int {
	log.Println("run subprocess with cmd:", subArgs) //nolint:gosec // args are from os.Args, not user input

	sc := make(chan os.Signal, 8)
	signal.Notify(sc)
	defer signal.Stop(sc)

	/* #nosec G204 */
	cmd := exec.Command(subArgs[0], subArgs[1:]...) //nolint:gosec // args are from os.Args, not user input
	cmd.Env = profileEnv

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		// Command not found on PATH, not executable, &c.
		log.Println(err)
		return 1
	}

	// wait for the command to finish
	waitCh := make(chan error, 1)
	go func() {
		waitCh <- cmd.Wait()
		close(waitCh)
	}()

	// Need a for loop to handle multiple signals
	for {
		select {
		case sig := <-sc:
			if err := cmd.Process.Signal(sig); err != nil {
				log.Println("error sending signal", sig, err)
			}
		case waitErr := <-waitCh:
			// clean session
			if len(subArgs) >= 3 && cleanChild != nil {
				if cleanErr := cleanChild(cmd.Process.Pid); cleanErr != nil {
					log.Println("clean session failed", cleanErr.Error())
				}
			}

			if waitErr != nil {
				log.Println("subprocess exit, ", waitErr.Error())
			} else {
				log.Println("exit code:", cmd.ProcessState.ExitCode())
			}
			return subprocessExitCode(waitErr, cmd.ProcessState)
		}
	}
}

func defaultCleanChildSession(pid int) error {
	defer sessionutil.RemoveServerInfoFile(pid)
	sessionSuffix := sessionutil.GetSessions(pid)
	if len(sessionSuffix) == 0 {
		return CleanSession("", nil, nil)
	}
	paramtable.Init()
	params := paramtable.Get()
	metaPath := params.EtcdCfg.MetaRootPath.GetValue()
	endpoints := params.EtcdCfg.Endpoints.GetValue()
	etcdEndpoints := strings.Split(endpoints, ",")
	return CleanSession(metaPath, etcdEndpoints, sessionSuffix)
}

// subprocessExitCode maps the result of cmd.Wait() to a valid process exit
// code. Signal-terminated children report ProcessState.ExitCode() == -1 in Go,
// which is mapped to the POSIX convention 128 + signal rather than passed as a
// negative value to os.Exit.
func subprocessExitCode(waitErr error, state *os.ProcessState) int {
	if waitErr == nil {
		return 0
	}
	if state != nil {
		if code := state.ExitCode(); code > 0 {
			return code
		}
		if ws, ok := state.Sys().(syscall.WaitStatus); ok && ws.Signaled() && ws.Signal() > 0 {
			return 128 + int(ws.Signal())
		}
	}
	return 1
}

// subprocessArgs returns args with the --run-with-subprocess marker removed,
// and whether the marker was present. It deletes from a copy, never from args,
// which is what lets Main hand a caller its own argument vector back untouched.
func subprocessArgs(args []string) ([]string, bool) {
	idx := slices.Index(args, "--run-with-subprocess")
	if idx <= 0 {
		// idx == 0 is the program name, which is never the marker.
		return args, false
	}
	stripped := slices.Clone(args)
	return slices.Delete(stripped, idx, idx+1), true
}
