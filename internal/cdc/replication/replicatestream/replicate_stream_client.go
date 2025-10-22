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

package replicatestream

import (
	context "context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/cdc/cluster"
	"github.com/milvus-io/milvus/internal/cdc/meta"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

var ErrReplicateIgnored = errors.New("ignored replicate message")

// ReplicateStreamClient is the client that replicates the message to the given cluster.
type ReplicateStreamClient interface {
	// Replicate replicates the message to the target cluster.
	// Replicate opeartion doesn't promise the message is delivered to the target cluster.
	// It will cache the message in memory and retry until the message is delivered to the target cluster or the client is closed.
	// Once the error is returned, the replicate operation will be unrecoverable.
	// return ErrReplicateIgnored if the message should not be replicated.
	Replicate(msg message.ImmutableMessage) error

	// Close closes the replicate stream client.
	Close()
}

type CreateReplicateStreamClientFunc func(ctx context.Context, c cluster.MilvusClient, rm *meta.ReplicateChannel) ReplicateStreamClient
