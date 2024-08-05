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

package flusher

import "github.com/milvus-io/milvus/internal/streamingnode/server/wal"

type Flusher interface {
	// RegisterPChannel ASYNCHRONOUSLY creates and starts pipelines belonging to the pchannel/WAL.
	// If a pipeline creation fails, the flusher will keep retrying to create it indefinitely.
	RegisterPChannel(pchannel string, w wal.WAL) error

	// UnregisterPChannel stops and removes pipelines belonging to the pchannel.
	UnregisterPChannel(pchannel string)

	// RegisterVChannel ASYNCHRONOUSLY create pipeline belonging to the vchannel.
	RegisterVChannel(vchannel string, wal wal.WAL)

	// UnregisterVChannel stops and removes pipeline belonging to the vchannel.
	UnregisterVChannel(vchannel string)

	// Start flusher service.
	Start()

	// Stop flusher, will synchronously flush all remaining data.
	Stop()
}
