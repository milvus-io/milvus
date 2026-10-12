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

package manifest

import "github.com/cockroachdb/errors"

var (
	// ErrFenced reports an edit from a pchannel term below the manifest term.
	ErrFenced = errors.New("pchannel term is below the manifest term")
	// ErrNotClaimed reports an edit on a vchannel that has no manifest.
	ErrNotClaimed = errors.New("manifest is not claimed")
	// ErrCheckpointRegressed reports a flush from a pchannel term above the
	// manifest term whose checkpoint is not after the manifest checkpoint.
	ErrCheckpointRegressed = errors.New("checkpoint time tick does not move forward")
	// ErrDuplicateSST reports an edit that adds an SST ID the manifest already
	// has, or that lists the same SST ID twice.
	ErrDuplicateSST = errors.New("sst ID is already in the manifest or repeated in the edit")
	// ErrStaleEdit reports a compaction whose inputs are not a contiguous run
	// of the manifest SST list.
	ErrStaleEdit = errors.New("compaction inputs are not in the manifest")
	// ErrSchemaMismatch reports a primary key data type that differs from the manifest.
	ErrSchemaMismatch = errors.New("primary key data type does not match the manifest")
)
