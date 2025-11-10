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

package main

import (
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"

	"golang.org/x/exp/slices"

	"github.com/milvus-io/milvus/cmd/asan"
	"github.com/milvus-io/milvus/cmd/milvus"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func main() {
	// after 2.6.0, we enable streaming service by default.
	// TODO: after remove all streamingutil.IsStreamingServiceEnabled(), we can remove this code.
	streamingutil.SetStreamingServiceEnabled()

	defer asan.LsanDoLeakCheck()
	idx := slices.Index(os.Args, "--run-with-subprocess")

	// execute command as a subprocess if the command contains "--run-with-subprocess"
	if idx > 0 {
		args := slices.Delete(os.Args, idx, idx+1)
		log.Println("run subprocess with cmd:", args)

		/* #nosec G204 */
		cmd := exec.Command(args[0], args[1:]...)

		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Start(); err != nil {
			// Command not found on PATH, not executable, &c.
			log.Fatal(err)
		}

		// wait for the command to finish. This is crucial for preventing the child process from becoming a Zombie (Z state).
		waitCh := make(chan error, 1)
		go func() {
			waitCh <- cmd.Wait()
			close(waitCh)
		}()

		sc := make(chan os.Signal, 1)
		// The filtering logic is now handled within the select case block.
		signal.Notify(sc)

		// Need a for loop to handle multiple signals
		for {
			select {
			case sig := <-sc:
				// --- Enhanced judgment logic for robustness and filtering ---

				// 1. **Filter out SIGCHLD**: This signal is for the parent process to handle the child's exit status.
				// It must not be forwarded, and cmd.Wait() handles the actual reaping.
				if sig == syscall.SIGCHLD {
					// We only log the event and continue to let cmd.Wait() handle the exit state.
					log.Println("Received SIGCHLD. Not forwarding, handled by cmd.Wait().")
					continue
				}

				// 2. Check if the child process has already exited
				// ProcessState not nil and Exited() is true means the process is terminated
				if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
					log.Printf("Received signal %v, but child process (PID %d) has already exited. Skipping forwarding.", sig, cmd.Process.Pid)
					continue
				}

				// Attempt to forward the signal to the child process (For SIGTERM, SIGINT, SIGHUP, etc.)
				log.Printf("Forwarding signal %v to subprocess (PID %d)...", sig, cmd.Process.Pid)
				if err := cmd.Process.Signal(sig); err != nil {
					// Common errors: "no such process" if the process exited milliseconds before
					log.Printf("Error forwarding signal %v to subprocess (PID %d): %v", sig, cmd.Process.Pid, err)
				}

			case err := <-waitCh:
				// Received result from cmd.Wait(), indicating the child process has terminated.
				// Start execution of cleanup logic.
				log.Println("Subprocess termination detected. Starting cleanup...")

				// clean session
				paramtable.Init()
				params := paramtable.Get()
				if len(args) >= 3 {
					metaPath := params.EtcdCfg.MetaRootPath.GetValue()
					endpoints := params.EtcdCfg.Endpoints.GetValue()
					etcdEndpoints := strings.Split(endpoints, ",")

					sessionSuffix := sessionutil.GetSessions(cmd.Process.Pid)
					defer sessionutil.RemoveServerInfoFile(cmd.Process.Pid)

					if cleanErr := milvus.CleanSession(metaPath, etcdEndpoints, sessionSuffix); cleanErr != nil {
						log.Println("clean session failed:", cleanErr.Error())
					} else {
						log.Println("Etcd session cleaned successfully.")
					}
				}

				if err != nil {
					log.Println("Subprocess exited with error:", err.Error())
				} else {
					log.Println("Subprocess exited successfully with code:", cmd.ProcessState.ExitCode())
				}
				// Exit the Wrapper process, which triggers Kubelet to restart the Pod
				return
			}
		}
	} else {
		// Original Milvus startup logic when the --run-with-subprocess flag is absent
		milvus.RunMilvus(os.Args)
	}
}
