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
	"flag"
	"io"
	"slices"
	"strconv"
	"strings"
)

// prepareNativeProfile consumes the opt-in startup flag and configures the
// existing subprocess. A nil environment keeps normal startup unchanged.
func prepareNativeProfile(args, environ []string) ([]string, []string, error) {
	command, role := "", -1
	for i := 1; i < len(args); i++ {
		if args[i] == "--run-with-subprocess" {
			continue
		}
		if command == "" {
			command = args[i]
		} else {
			role = i
			break
		}
	}
	if command != "run" || role < 0 {
		return args, nil, nil
	}
	flags := flag.NewFlagSet("milvus", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.String("alias", "", "")
	for _, name := range []string{"rootcoord", "querycoord", "datacoord", "indexcoord", "querynode", "datanode", "proxy", "streamingnode"} {
		flags.Bool(name, false, "")
	}
	var flagArgs []string
	var positions []int
	for i := role + 1; i < len(args); i++ {
		if args[i] != "--run-with-subprocess" {
			flagArgs = append(flagArgs, args[i])
			positions = append(positions, i)
		}
	}
	enabled := false
	var nativeErr error
	consumed := make(map[int]bool)
	flags.BoolFunc("native-profile", "", func(value string) error {
		// BoolFunc runs after the flag token is consumed. Remove only tokens
		// parsed as this flag, preserving alias values and unparsed arguments.
		consumed[positions[len(flagArgs)-len(flags.Args())-1]] = true
		enabled, nativeErr = strconv.ParseBool(value)
		return nativeErr
	})
	parseErr := flags.Parse(flagArgs)
	if nativeErr != nil {
		return nil, nil, parseErr
	}
	if len(consumed) == 0 {
		return args, nil, nil
	}
	clean := make([]string, 0, len(args))
	for i, arg := range args {
		if !consumed[i] {
			clean = append(clean, arg)
		}
	}
	// Let the existing role parser handle help and invalid flags.
	if parseErr != nil || !enabled {
		return clean, nil, nil
	}
	conf := ""
	for _, entry := range environ {
		if value, ok := strings.CutPrefix(entry, "MALLOC_CONF="); ok {
			conf = value
		}
	}
	if conf != "" {
		conf += ","
	}
	conf += "prof:true,prof_active:true,prof_thread_active_init:true"
	if !slices.Contains(clean, "--run-with-subprocess") {
		clean = append(clean, "--run-with-subprocess")
	}
	return clean, append(append([]string(nil), environ...), "MALLOC_CONF="+conf), nil
}
