// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package inspector

import (
	"fmt"

	"github.com/milvus-io/milvus/internal/storage"
)

func InspectV2(raw []byte, expectedEZID int64) error {
	reader, err := storage.NewBinlogReader(raw)
	if err != nil {
		return fmt.Errorf("parse V2 IndexData envelope: %w", err)
	}
	defer reader.Close()

	edek, ok := reader.GetEdek()
	if !ok || edek == "" {
		return fmt.Errorf("V2 IndexData envelope has no EDEK")
	}
	ezID, ok := reader.GetEzID()
	if !ok {
		return fmt.Errorf("V2 IndexData envelope has no EZ id")
	}
	if ezID != expectedEZID {
		return fmt.Errorf("V2 IndexData envelope EZ id %d, want %d", ezID, expectedEZID)
	}
	return nil
}
