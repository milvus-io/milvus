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

package storage

import (
	"io/fs"
	"os"

	"github.com/milvus-io/milvus/pkg/util/merr"
)

//////////////////////////////////////////////////////////////////////////////////////////////////

// Open opens file as os.Open works,
// also converts the os errors to Milvus errors
func Open(filepath string) (*os.File, error) {
	// NOLINT
	reader, err := os.Open(filepath)
	if os.IsNotExist(err) {
		return nil, merr.WrapErrIoKeyNotFound(filepath)
	} else if err != nil {
		return nil, merr.WrapErrIoFailed(filepath, err)
	}

	return reader, nil
}

// ReadFile reads file as os.ReadFile works,
// also converts the os errors to Milvus errors
func ReadFile(filepath string) ([]byte, error) {
	// NOLINT
	data, err := os.ReadFile(filepath)
	if os.IsNotExist(err) {
		return nil, merr.WrapErrIoKeyNotFound(filepath)
	} else if err != nil {
		return nil, merr.WrapErrIoFailed(filepath, err)
	}

	return data, nil
}

// WriteFile writes file as os.WriteFile works，
// also converts the os errors to Milvus errors
func WriteFile(filepath string, data []byte, perm fs.FileMode) error {
	// NOLINT
	err := os.WriteFile(filepath, data, perm)
	if err != nil {
		return merr.WrapErrIoFailed(filepath, err)
	}
	return nil
}

///////////////////////////////////////////////////////////////////////////////////////////
