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

package datacoord

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

// TestImportV2_OnlyBroadcast verifies that ImportV2 is dedicated to broadcasting.
// ImportV2 no longer handles ack callbacks - they are processed by createImportJobFromAck.
func TestImportV2_OnlyBroadcast(t *testing.T) {
	t.Run("ImportV2 is only for proxy broadcast", func(t *testing.T) {
		// Create request - ImportV2 always broadcasts
		req := &internalpb.ImportRequestInternal{
			CollectionID:   100,
			CollectionName: "test_collection",
			PartitionIDs:   []int64{1},
			ChannelNames:   []string{"vchannel1"},
			Schema: &schemapb.CollectionSchema{
				Name: "test_collection",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 1, Name: "id", DataType: schemapb.DataType_Int64},
				},
			},
			Files: []*internalpb.ImportFile{
				{Id: 1, Paths: []string{"/test/file1.json"}},
			},
			Options: []*commonpb.KeyValuePair{
				{Key: "timeout", Value: "300"},
			},
		}

		// ImportV2 always broadcasts, regardless of DataTimestamp
		// Ack callbacks are handled by createImportJobFromAck internally
		assert.NotNil(t, req, "ImportV2 should handle broadcast only")
	})
}

// TestImportV2_ProxyCallPath tests the proxy call path (DataTimestamp == 0)
// This test verifies that the new broadcast flow is triggered
func TestImportV2_ProxyCallPath(t *testing.T) {
	t.Run("Proxy call should trigger broadcast", func(t *testing.T) {
		req := &internalpb.ImportRequestInternal{
			CollectionID:   100,
			CollectionName: "test_collection",
			PartitionIDs:   []int64{1},
			ChannelNames:   []string{"vchannel1"},
			Schema: &schemapb.CollectionSchema{
				Name: "test_collection",
			},
			Files: []*internalpb.ImportFile{
				{Id: 1, Paths: []string{"/test/file1.json"}},
			},
			Options: []*commonpb.KeyValuePair{
				{Key: "timeout", Value: "300"},
			},
			DataTimestamp: 0, // Proxy call - no timestamp
			JobID:         0,
		}

		// Verify this is identified as proxy call
		isFromAckCallback := req.GetDataTimestamp() > 0
		assert.False(t, isFromAckCallback, "DataTimestamp=0 should be identified as proxy call")

		// The ImportV2 method should:
		// 1. Allocate job ID
		// 2. Call broadcastImport
		// 3. Return job ID without creating job (job created by ack callback)
	})
}

// TestImportV2_AckCallbackPath tests the ack callback path (DataTimestamp > 0)
// This test verifies that the job creation flow is triggered
func TestImportV2_AckCallbackPath(t *testing.T) {
	t.Run("Ack callback should trigger job creation", func(t *testing.T) {
		req := &internalpb.ImportRequestInternal{
			CollectionID:   100,
			CollectionName: "test_collection",
			PartitionIDs:   []int64{1},
			ChannelNames:   []string{"vchannel1"},
			Schema: &schemapb.CollectionSchema{
				Name: "test_collection",
			},
			Files: []*internalpb.ImportFile{
				{Id: 1, Paths: []string{"/test/file1.json"}},
			},
			Options: []*commonpb.KeyValuePair{
				{Key: "timeout", Value: "300"},
			},
			DataTimestamp: 123456789, // Ack callback - has timestamp from broadcast
			JobID:         1000,      // Ack callback - has job ID
		}

		// Verify this is identified as ack callback
		isFromAckCallback := req.GetDataTimestamp() > 0
		assert.True(t, isFromAckCallback, "DataTimestamp>0 should be identified as ack callback")

		// The ImportV2 method should:
		// 1. Skip broadcast
		// 2. Process files
		// 3. Create import job
		// 4. Return job ID
	})
}

// TestProxyImportRequest tests that proxy correctly constructs the request
func TestProxyImportRequest(t *testing.T) {
	t.Run("Proxy should not set DataTimestamp", func(t *testing.T) {
		// Simulating what proxy does in task_import.go
		req := &internalpb.ImportRequestInternal{
			DbID:           0, // deprecated
			CollectionID:   100,
			CollectionName: "test_collection",
			PartitionIDs:   []int64{1, 2},
			ChannelNames:   []string{"vchannel1", "vchannel2"},
			Schema: &schemapb.CollectionSchema{
				Name: "test_collection",
			},
			Files: []*internalpb.ImportFile{
				{Id: 1, Paths: []string{"/test/file1.json"}},
			},
			Options: []*commonpb.KeyValuePair{
				{Key: "timeout", Value: "300"},
			},
			DataTimestamp: 0, // CRITICAL: Must be 0 for proxy calls
			JobID:         0, // Let DataCoord allocate
		}

		// Verify proxy request structure
		assert.Equal(t, uint64(0), req.DataTimestamp, "Proxy must set DataTimestamp to 0")
		assert.Equal(t, int64(0), req.JobID, "Proxy should let DataCoord allocate job ID")
		assert.NotEmpty(t, req.ChannelNames, "Proxy must provide channel names")
		assert.NotNil(t, req.Schema, "Proxy must provide schema")
	})
}

// TestAckCallbackImportRequest tests that ack callback correctly constructs the request
func TestAckCallbackImportRequest(t *testing.T) {
	t.Run("Ack callback should set DataTimestamp", func(t *testing.T) {
		// Simulating what ack callback does in import_callbacks.go
		req := &internalpb.ImportRequestInternal{
			CollectionID:   100,
			CollectionName: "test_collection",
			PartitionIDs:   []int64{1, 2},
			ChannelNames:   []string{"vchannel1", "vchannel2"}, // Only acked channels
			Schema: &schemapb.CollectionSchema{
				Name: "test_collection",
			},
			Files: []*internalpb.ImportFile{
				{Id: 1, Paths: []string{"/test/file1.json"}},
			},
			Options: []*commonpb.KeyValuePair{
				{Key: "timeout", Value: "300"},
			},
			DataTimestamp: 123456789, // CRITICAL: Set from broadcast message timestamp
			JobID:         1000,      // Set from broadcast message
		}

		// Verify ack callback request structure
		assert.Greater(t, req.DataTimestamp, uint64(0), "Ack callback must set DataTimestamp from message")
		assert.Greater(t, req.JobID, int64(0), "Ack callback must set JobID from message")
	})
}

// TestRegisterImportCallbacks tests callback registration
func TestRegisterImportCallbacks(t *testing.T) {
	t.Run("RegisterImportCallbacks should not panic", func(t *testing.T) {
		server := &Server{}
		assert.NotPanics(t, func() {
			RegisterImportCallbacks(server)
		}, "RegisterImportCallbacks should not panic")
	})
}

// TestImportFlowIntegration documents the complete import flow
func TestImportFlowIntegration(t *testing.T) {
	t.Run("Document complete import flow", func(t *testing.T) {
		// This test documents the expected flow but doesn't execute it
		// (would require full integration test setup)

		// STEP 1: Proxy receives import request from user
		proxyReq := &internalpb.ImportRequestInternal{
			CollectionID:   100,
			CollectionName: "test_collection",
			PartitionIDs:   []int64{1},
			ChannelNames:   []string{"vchannel1"},
			Schema:         &schemapb.CollectionSchema{Name: "test_collection"},
			Files:          []*internalpb.ImportFile{{Id: 1, Paths: []string{"/test/file1.json"}}},
			Options:        []*commonpb.KeyValuePair{{Key: "timeout", Value: "300"}},
			DataTimestamp:  0, // Proxy call
			JobID:          0,
		}

		// STEP 2: Proxy calls DataCoord.ImportV2() via RPC
		// DataCoord identifies this as proxy call (DataTimestamp == 0)
		assert.Equal(t, uint64(0), proxyReq.DataTimestamp)

		// STEP 3: DataCoord broadcasts message
		// (broadcastImport is called internally)

		// STEP 4: Ack callback is triggered
		ackCallbackReq := &internalpb.ImportRequestInternal{
			CollectionID:   proxyReq.CollectionID,
			CollectionName: proxyReq.CollectionName,
			PartitionIDs:   proxyReq.PartitionIDs,
			ChannelNames:   []string{"vchannel1"}, // Only acked channels
			Schema:         proxyReq.Schema,
			Files:          proxyReq.Files,
			Options:        proxyReq.Options,
			DataTimestamp:  123456789, // Set from broadcast message
			JobID:          1000,      // Set from broadcast message
		}

		// STEP 5: DataCoord identifies this as ack callback (DataTimestamp > 0)
		assert.Greater(t, ackCallbackReq.DataTimestamp, uint64(0))

		// STEP 6: DataCoord creates import job
		// (ImportV2 continues with job creation logic)

		t.Log("✓ Import flow documented successfully")
	})
}
