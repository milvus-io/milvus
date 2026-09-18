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

package segments

import (
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// growingFlushEncryptionZone uses the flush task's schema snapshot. Collection.Ref
// owns key registration; the packed writer resolves the encryption properties.
func growingFlushEncryptionZone(config *FlushConfig) (int64, error) {
	var ezID int64
	encrypted := false
	for _, property := range config.Schema.GetProperties() {
		if property.GetKey() != common.EncryptionEzIDKey {
			continue
		}
		var err error
		ezID, err = strconv.ParseInt(property.GetValue(), 10, 64)
		if err != nil || ezID <= 0 {
			return 0, merr.WrapErrServiceInternalMsg("invalid encryption zone ID in growing flush schema")
		}
		encrypted = true
		break
	}
	if !encrypted {
		return 0, nil
	}
	if config.CollectionID <= 0 {
		return 0, merr.WrapErrServiceInternalMsg("invalid collection ID for encrypted growing flush")
	}
	// Parquet encryption does not protect the independent Vortex LOB writer.
	if len(config.TextFieldIDs) > 0 {
		return 0, merr.Wrap(merr.ErrServiceUnimplemented, "CMEK growing-source flush does not support TEXT/LOB")
	}
	if config.WriterFormat != "" && config.WriterFormat != "parquet" {
		return 0, merr.Wrap(merr.ErrServiceUnimplemented, "CMEK growing-source flush requires Parquet")
	}
	if config.SchemaBasedPattern != "" && config.SchemaBasedFormats != "" {
		for _, format := range strings.Split(config.SchemaBasedFormats, ",") {
			if format != "parquet" {
				return 0, merr.Wrap(merr.ErrServiceUnimplemented, "CMEK growing-source flush requires Parquet column groups")
			}
		}
	}
	return ezID, nil
}
