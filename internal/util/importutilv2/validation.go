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

package importutilv2

import "github.com/milvus-io/milvus/pkg/v3/proto/internalpb"

// ValidateImportFiles checks source paths and descriptors before a worker reads
// them or an ACK creates a runnable job. It performs no storage access.
func ValidateImportFiles(files []*internalpb.ImportFile, options Options) error {
	if HasSourceStorageOptions(options) {
		if err := ValidateImportSourceOptions(options); err != nil {
			return err
		}
		for _, file := range files {
			if err := ValidateSourceStorageFilePaths(file.GetPaths()); err != nil {
				return err
			}
		}
	}
	return ValidateSnapshotImportFiles(files, options)
}

// ValidateImportPlan checks the durable source representation before publication
// and after ACK/recovery, including shared L0 inventories for snapshot imports.
func ValidateImportPlan(files []*internalpb.ImportFile, options Options, sources []*internalpb.SnapshotImportL0Source) error {
	if err := ValidateImportFiles(files, options); err != nil {
		return err
	}
	return validateSnapshotImportPlan(files, options, sources)
}
