package main

import (
	"os"

	"github.com/milvus-io/milvus/cmd/tools/migration/command"
)

func main() {
	command.Execute(os.Args)
}
