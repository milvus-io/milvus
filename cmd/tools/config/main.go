package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

const (
	generateCsv  = "gen-csv"
	generateYaml = "gen-yaml"
	showYaml     = "show-yaml"
)

func main() {
	args := os.Args

	if len(args) < 2 {
		mlog.Error(context.TODO(), "len of args should large than 2")
		os.Exit(-1)
	}
	var outputPath string
	var render func(io.Writer, []DocContent)
	switch args[1] {
	case generateCsv:
		outputPath = "configs.csv"
		render = WriteCsv
	case generateYaml:
		outputPath = "milvus.yaml"
		render = WriteYaml
	case showYaml:
		var f string
		if len(args) == 2 {
			f = "configs/milvus.yaml"
		} else {
			f = args[2]
		}
		ShowYaml(f)
		return
	default:
		mlog.Error(context.TODO(), fmt.Sprintf("unknown argument %s", args[1]))
		return
	}
	values, err := collect()
	if err != nil {
		// The parser may include configuration values in its error message.
		mlog.Error(context.TODO(), "failed to load release configuration from milvus.yaml")
		os.Exit(1)
	}
	var output bytes.Buffer
	render(&output, values)
	if err := os.WriteFile(outputPath, output.Bytes(), 0o666); err != nil { // #nosec G306 -- preserve os.Create permissions, subject to the caller's umask.
		mlog.Error(context.TODO(), "write generated config failed", mlog.Err(err))
		os.Exit(-2)
	}
}
