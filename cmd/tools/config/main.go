package main

import (
	"fmt"
	"os"

	"github.com/milvus-io/milvus/internal/log"
)

const (
	generateCsv  = "gen-csv"
	generateYaml = "gen-yaml"
	showYaml     = "show-yaml"
)

func main() {
	args := os.Args

	if len(args) < 2 {
		log.Error("len of args should large than 2")
		os.Exit(-1)
	}
	switch args[1] {
	case generateCsv:
		WriteCsv()
	case generateYaml:
		WriteYaml()
	case showYaml:
		var f string
		if len(args) == 2 {
			f = "configs/milvus.yaml"
		} else {
			f = args[2]
		}
		ShowYaml(f)
	default:
		log.Error(fmt.Sprintf("unknown argument %s", args[1]))
	}

}
