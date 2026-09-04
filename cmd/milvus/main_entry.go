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
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"

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
// followed by an unconditional tail call, and args is copied on entry.
//
// args is the full argument vector, program name first, as os.Args is. Main
// does not modify the caller's slice: the subprocess branch below deletes an
// element in place, which on os.Args was invisible and on an exported API
// would hand the caller back a shortened vector with a cleared tail.
//
// A distribution installs its compiled-in hook and coordinator engine
// (pkg/extension) before calling Main; nothing runs earlier than the first
// line here except package initialization.
func Main(args []string) {
	args = slices.Clone(args)

	// after 2.6.0, we enable streaming service by default.
	// TODO: after remove all streamingutil.IsStreamingServiceEnabled(), we can remove this code.
	streamingutil.SetStreamingServiceEnabled()

	defer asan.LsanDoLeakCheck()
	idx := slices.Index(args, "--run-with-subprocess")

	// execute command as a subprocess if the command contains "--run-with-subprocess"
	if idx > 0 {
		subArgs := slices.Delete(args, idx, idx+1)
		log.Println("run subprocess with cmd:", subArgs) //nolint:gosec // args are from os.Args, not user input

		/* #nosec G204 */
		cmd := exec.Command(subArgs[0], subArgs[1:]...) //nolint:gosec // args are from os.Args, not user input

		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Start(); err != nil {
			// Command not found on PATH, not executable, &c.
			log.Fatal(err)
		}

		// wait for the command to finish
		waitCh := make(chan error, 1)
		go func() {
			waitCh <- cmd.Wait()
			close(waitCh)
		}()

		sc := make(chan os.Signal, 1)
		signal.Notify(sc)

		// Need a for loop to handle multiple signals
		for {
			select {
			case sig := <-sc:
				if err := cmd.Process.Signal(sig); err != nil {
					log.Println("error sending signal", sig, err)
				}
			case err := <-waitCh:
				// clean session
				paramtable.Init()
				params := paramtable.Get()
				if len(subArgs) >= 3 {
					metaPath := params.EtcdCfg.MetaRootPath.GetValue()
					endpoints := params.EtcdCfg.Endpoints.GetValue()
					etcdEndpoints := strings.Split(endpoints, ",")

					sessionSuffix := sessionutil.GetSessions(cmd.Process.Pid)
					defer sessionutil.RemoveServerInfoFile(cmd.Process.Pid)

					if err := CleanSession(metaPath, etcdEndpoints, sessionSuffix); err != nil {
						log.Println("clean session failed", err.Error())
					}
				}

				if err != nil {
					log.Println("subprocess exit, ", err.Error())
				} else {
					log.Println("exit code:", cmd.ProcessState.ExitCode())
				}
				return
			}
		}
	}

	RunMilvus(args)
}
