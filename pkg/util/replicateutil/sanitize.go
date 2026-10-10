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

package replicateutil

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
)

// SanitizeReplicateConfiguration returns a deep copy of the configuration
// with sensitive fields (like tokens) cleared.
func SanitizeReplicateConfiguration(config *commonpb.ReplicateConfiguration) *commonpb.ReplicateConfiguration {
	if config == nil {
		return nil
	}

	// Deep copy to avoid modifying original
	sanitized := proto.Clone(config).(*commonpb.ReplicateConfiguration)

	// Clear sensitive fields
	for _, cluster := range sanitized.Clusters {
		if cluster.ConnectionParam != nil {
			cluster.ConnectionParam.Token = ""
		}
	}

	return sanitized
}

// FillRedactedConnectionTokens returns a copy of incoming in which a cluster
// with an empty connection token takes the token stored for the same cluster in
// current.
//
// SanitizeReplicateConfiguration clears connection tokens on every read, so a
// caller that reads the configuration, edits the topology and writes it back
// cannot send a token it was never given. The validator requires connection
// parameters to be unchanged, so without this the read-modify-write round trip
// is always rejected with "connection_param.token cannot be changed", and
// simply accepting the empty token would store it and erase the credential CDC
// uses to reach the peer.
//
// A non-empty incoming token is left untouched, so an actual attempt to change
// a token is still rejected by the validator.
func FillRedactedConnectionTokens(incoming, current *commonpb.ReplicateConfiguration) *commonpb.ReplicateConfiguration {
	if incoming == nil || current == nil {
		return incoming
	}

	stored := make(map[string]string, len(current.GetClusters()))
	for _, cluster := range current.GetClusters() {
		if token := cluster.GetConnectionParam().GetToken(); token != "" {
			stored[cluster.GetClusterId()] = token
		}
	}
	if len(stored) == 0 {
		return incoming
	}

	filled := proto.Clone(incoming).(*commonpb.ReplicateConfiguration)
	for _, cluster := range filled.GetClusters() {
		// A nil ConnectionParam is left alone: its uri is empty too, and that
		// mismatch is reported on its own.
		if cluster.GetConnectionParam() == nil || cluster.GetConnectionParam().GetToken() != "" {
			continue
		}
		if token, ok := stored[cluster.GetClusterId()]; ok {
			cluster.ConnectionParam.Token = token
		}
	}
	return filled
}
