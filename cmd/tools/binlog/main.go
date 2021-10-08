// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package main

import (
	"fmt"
	"os"

	"github.com/milvus-io/milvus/internal/storage"
)

func main() {
	if len(os.Args) == 1 {
		fmt.Println("usage: binlog file1 file2 ...")
	}
	if err := storage.PrintBinlogFiles(os.Args[1:]); err != nil {
		fmt.Printf("error: %s\n", err.Error())
	} else {
		fmt.Printf("print binlog complete.\n")
	}
}
