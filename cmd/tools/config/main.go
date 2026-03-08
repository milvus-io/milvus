package main

import (
	"fmt"
	"os"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
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
		f, err := os.Create("configs.csv")
		defer f.Close()
		if err != nil {
			log.Error("create file failed", zap.Error(err))
			os.Exit(-2)
		}
		WriteCsv(f)
	case generateYaml:
		f, err := os.Create("milvus.yaml")
		defer f.Close()
		if err != nil {
			log.Error("create file failed", zap.Error(err))
			os.Exit(-2)
		}
		WriteYaml(f)
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
