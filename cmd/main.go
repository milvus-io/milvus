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
				if len(args) >= 3 {
					metaPath := params.EtcdCfg.MetaRootPath.GetValue()
					endpoints := params.EtcdCfg.Endpoints.GetValue()
					etcdEndpoints := strings.Split(endpoints, ",")

					sessionSuffix := sessionutil.GetSessions(cmd.Process.Pid)
					defer sessionutil.RemoveServerInfoFile(cmd.Process.Pid)

					if err := milvus.CleanSession(metaPath, etcdEndpoints, sessionSuffix); err != nil {
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
	} else {
		milvus.RunMilvus(os.Args)
	}
}
