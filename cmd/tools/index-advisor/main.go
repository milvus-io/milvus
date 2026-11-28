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

// Command index-advisor lists scalar index candidates to validate, from the
// filter expressions in Milvus proxy access logs. It is advisory only: it reads
// logs and a schema snapshot and never creates, alters or drops an index. See
// README.md.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const usage = `Usage:
  index-advisor --access-log FILE [--access-log FILE ...] --snapshot FILE [--field-stats FILE] [--milvus-config FILE] [--output FILE]

See README.md for the input formats.
`

func main() {
	c := &cli{stdout: os.Stdout, stderr: os.Stderr, engine: defaultRuleEngine()}
	if err := c.run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "index-advisor: %v\n", err)
		os.Exit(1)
	}
}

// cli holds what a command depends on. main wires the real dependencies;
// tests replace the writers or the rule engine.
type cli struct {
	stdout, stderr io.Writer
	engine         *ruleEngine
}

// stringList is a repeatable string flag.
type stringList []string

func (s *stringList) String() string     { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error { *s = append(*s, v); return nil }

// parseFlags parses args and reports whether the command should run; -h
// prints the flags and stops without an error.
func parseFlags(fs *flag.FlagSet, args []string) (bool, error) {
	err := fs.Parse(args)
	switch {
	case errors.Is(err, flag.ErrHelp):
		return false, nil
	case err != nil:
		return false, merr.WrapErrParameterInvalidErr(err, "invalid flags")
	case fs.NArg() > 0:
		return false, merr.WrapErrParameterInvalidMsg("unexpected argument %q", fs.Arg(0))
	}
	return true, nil
}

// run parses args, loads the inputs, and writes the advice.
func (c *cli) run(args []string) error {
	fs := flag.NewFlagSet("index-advisor", flag.ContinueOnError)
	fs.SetOutput(c.stderr)
	fs.Usage = func() {
		fmt.Fprint(c.stderr, usage, "\nFlags:\n")
		fs.PrintDefaults()
	}
	var logs stringList
	fs.Var(&logs, "access-log", "proxy access log file; repeatable, e.g. for rotated files (required)")
	snapshotPath := fs.String("snapshot", "", "schema and existing-index snapshot JSON; see README.md for its format (required)")
	statsPath := fs.String("field-stats", "", "optional field statistics JSON; statistics it does not give are reported as unknown")
	configPath := fs.String("milvus-config", "", "optional milvus.yaml or user.yaml whose proxy.accessLog.formatters override the defaults")
	outputPath := fs.String("output", "", "write the advice JSON to this file instead of stdout")
	if ok, err := parseFlags(fs, args); !ok {
		return err
	}
	if len(logs) == 0 || *snapshotPath == "" {
		fs.Usage()
		return merr.WrapErrParameterMissingMsg("--access-log and --snapshot are required")
	}

	cfgs, err := loadFormatterConfigs(*configPath)
	if err != nil {
		return err
	}
	formatters, err := newFormatterSet(cfgs)
	if err != nil {
		return err
	}
	snap, snapWarnings, err := loadSnapshotFile(*snapshotPath)
	if err != nil {
		return err
	}
	warnings := append(formatters.analysisWarnings(), snapWarnings...)
	var stats *statsSet
	if *statsPath != "" {
		var statsWarnings []string
		if stats, statsWarnings, err = loadStatsFile(*statsPath, snap); err != nil {
			return err
		}
		warnings = append(warnings, statsWarnings...)
	}

	w := newWorkload(snap)
	for _, path := range logs {
		logWarnings, err := readRecordsFile(path, formatters, w.skipped, w.add)
		if err != nil {
			return err
		}
		warnings = append(warnings, logWarnings...)
	}
	for _, warning := range warnings {
		fmt.Fprintf(c.stderr, "index-advisor: warning: %s\n", warning)
	}
	return writeOutput(*outputPath, c.stdout, advise(logs, w, stats, c.engine))
}

// writeOutput writes v as JSON to path, or to stdout when path is empty.
func writeOutput(path string, stdout io.Writer, v any) error {
	if path == "" {
		return writeJSON(stdout, v)
	}
	f, err := os.Create(path) // #nosec G304 -- path is a CLI argument
	if err != nil {
		return merr.WrapErrIoFailed(path, err)
	}
	if err := writeJSON(f, v); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return merr.WrapErrIoFailed(path, err)
	}
	return nil
}

// writeJSON writes v as indented JSON.
func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return merr.WrapErrIoFailed("output", err)
	}
	return nil
}
