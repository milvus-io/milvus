package main

import (
	"fmt"
	"os"

	"github.com/zilliztech/milvus-distributed/internal/storage"
)

func main() {
	if len(os.Args) == 1 {
		fmt.Println("usage: binlog file1 file2 ...")
	}
	if err := storage.PrintBinlogFiles(os.Args[1:]); err != nil {
		fmt.Printf("error: %s\n", err.Error())
	}
}
