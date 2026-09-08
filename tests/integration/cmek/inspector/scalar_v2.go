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

package inspector

import (
	"encoding/json"
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
	var extras struct {
		EZID json.Number `json:"encryption_zone"`
	}
	if err := json.Unmarshal(reader.ExtraBytes, &extras); err != nil {
		return fmt.Errorf("parse V2 IndexData descriptor extras: %w", err)
	}
	if extras.EZID == "" {
		return fmt.Errorf("V2 IndexData envelope has no EZ id")
	}
	ezID, err := extras.EZID.Int64()
	if err != nil {
		return fmt.Errorf("V2 IndexData envelope has invalid EZ id %q: %w", extras.EZID, err)
	}
	if ezID != expectedEZID {
		return fmt.Errorf("V2 IndexData envelope EZ id %d, want %d", ezID, expectedEZID)
	}
	return nil
}
