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
	"runtime/debug"

	_ "github.com/benesch/cgosymbolizer" // enable cpp stack
)

// defaultTracebackLevel is the Go runtime traceback level applied when the
// operator has not chosen one via the GOTRACEBACK environment variable.
// "crash" is deliberately not the default: on a panic it raises SIGABRT so
// the kernel writes a core dump, which stalls pod restarts for minutes on
// multi-GB processes (see #53956). Operators that want core dumps can opt
// back in with GOTRACEBACK=crash.
const defaultTracebackLevel = "all"

func init() {
	// The Go runtime ORs the GOTRACEBACK value into any level requested via
	// debug.SetTraceback, so a hardcoded "crash" here could never be lowered
	// from the environment. Only override the level when the operator has
	// not set GOTRACEBACK at all; otherwise the environment wins.
	if _, ok := os.LookupEnv("GOTRACEBACK"); !ok {
		debug.SetTraceback(defaultTracebackLevel)
	}
}
