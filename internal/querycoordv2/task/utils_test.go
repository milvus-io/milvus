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

package task

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
)

type UtilsSuite struct {
	suite.Suite
}

func (s *UtilsSuite) TestPackLoadSegmentRequest() {
	ctx := context.Background()

	action := NewSegmentAction(1, ActionTypeGrow, "test-ch", 100)
	task, err := NewSegmentTask(
		ctx,
		time.Second,
		nil,
		1,
		newReplicaDefaultRG(10),
		commonpb.LoadPriority_LOW,
		action,
	)
	s.NoError(err)

	collectionInfoResp := &milvuspb.DescribeCollectionResponse{
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					DataType:     schemapb.DataType_Int64,
					IsPrimaryKey: true,
				},
			},
		},
		Properties: []*commonpb.KeyValuePair{
			{
				Key:   common.MmapEnabledKey,
				Value: "false",
			},
		},
	}

	req := packLoadSegmentRequest(
		task,
		action,
		collectionInfoResp.GetSchema(),
		collectionInfoResp.GetProperties(),
		&querypb.LoadMetaInfo{
			LoadType: querypb.LoadType_LoadCollection,
		},
		&querypb.SegmentLoadInfo{},
		nil,
	)

	s.True(req.GetNeedTransfer())
	s.Equal(task.CollectionID(), req.CollectionID)
	s.Equal(task.ReplicaID(), req.ReplicaID)
	s.Equal(action.Node(), req.GetDstNodeID())
	for _, field := range req.GetSchema().GetFields() {
		mmapEnable, ok := common.IsMmapDataEnabled(field.GetTypeParams()...)
		s.False(mmapEnable)
		s.True(ok)
	}
}

func (s *UtilsSuite) TestPackLoadSegmentRequestMmapDuplicateBug() {
	// This test demonstrates the bug where mmap.enabled is appended twice to field TypeParams
	ctx := context.Background()

	action := NewSegmentAction(1, ActionTypeGrow, "test-ch", 100)
	task, err := NewSegmentTask(
		ctx,
		time.Second,
		nil,
		1,
		newReplicaDefaultRG(10),
		commonpb.LoadPriority_LOW,
		action,
	)
	s.NoError(err)

	collectionInfoResp := &milvuspb.DescribeCollectionResponse{
		Schema: &schemapb.CollectionSchema{
			Name: "test_collection",
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "pk",
					DataType:     schemapb.DataType_Int64,
					IsPrimaryKey: true,
					TypeParams:   []*commonpb.KeyValuePair{}, // No existing mmap setting
				},
				{
					FieldID:  101,
					Name:     "vector",
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "128"},
					}, // No existing mmap setting
				},
				{
					FieldID:    102,
					Name:       "scalar",
					DataType:   schemapb.DataType_Int32,
					TypeParams: []*commonpb.KeyValuePair{}, // No existing mmap setting
				},
			},
		},
		Properties: []*commonpb.KeyValuePair{
			{
				Key:   common.MmapEnabledKey,
				Value: "true",
			},
		},
	}

	req := packLoadSegmentRequest(
		task,
		action,
		collectionInfoResp.GetSchema(),
		collectionInfoResp.GetProperties(),
		&querypb.LoadMetaInfo{
			LoadType: querypb.LoadType_LoadCollection,
		},
		&querypb.SegmentLoadInfo{},
		nil,
	)

	// Check each field for duplicate mmap.enabled properties
	for _, field := range req.GetSchema().GetFields() {
		mmapCount := 0
		for _, kv := range field.GetTypeParams() {
			if kv.Key == common.MmapEnabledKey {
				mmapCount++
			}
		}
		s.LessOrEqualf(mmapCount, 1, "Field %s has %d mmap.enabled properties, should have at most 1", field.GetName(), mmapCount)
	}
}

func (s *UtilsSuite) TestPackLoadSegmentRequestMmap() {
	ctx := context.Background()

	action := NewSegmentAction(1, ActionTypeGrow, "test-ch", 100)
	task, err := NewSegmentTask(
		ctx,
		time.Second,
		nil,
		1,
		newReplicaDefaultRG(10),
		commonpb.LoadPriority_LOW,
		action,
	)
	s.NoError(err)

	collectionInfoResp := &milvuspb.DescribeCollectionResponse{
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					DataType:     schemapb.DataType_Int64,
					IsPrimaryKey: true,
				},
			},
		},
		Properties: []*commonpb.KeyValuePair{
			{
				Key:   common.MmapEnabledKey,
				Value: "true",
			},
		},
	}

	req := packLoadSegmentRequest(
		task,
		action,
		collectionInfoResp.GetSchema(),
		collectionInfoResp.GetProperties(),
		&querypb.LoadMetaInfo{
			LoadType: querypb.LoadType_LoadCollection,
		},
		&querypb.SegmentLoadInfo{},
		nil,
	)

	s.True(req.GetNeedTransfer())
	s.Equal(task.CollectionID(), req.CollectionID)
	s.Equal(task.ReplicaID(), req.ReplicaID)
	s.Equal(action.Node(), req.GetDstNodeID())
	for _, field := range req.GetSchema().GetFields() {
		mmapEnable, ok := common.IsMmapDataEnabled(field.GetTypeParams()...)
		s.True(mmapEnable)
		s.True(ok)
	}
}

func (s *UtilsSuite) TestPackLoadSegmentRequestFieldLevelMmapPriority() {
	// This test demonstrates that field-level mmap settings should not be duplicated
	// when collection-level mmap is also set
	ctx := context.Background()

	action := NewSegmentAction(1, ActionTypeGrow, "test-ch", 100)
	task, err := NewSegmentTask(
		ctx,
		time.Second,
		nil,
		1,
		newReplicaDefaultRG(10),
		commonpb.LoadPriority_LOW,
		action,
	)
	s.NoError(err)

	collectionInfoResp := &milvuspb.DescribeCollectionResponse{
		Schema: &schemapb.CollectionSchema{
			Name: "test_collection",
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "pk",
					DataType:     schemapb.DataType_Int64,
					IsPrimaryKey: true,
					TypeParams:   []*commonpb.KeyValuePair{}, // No field-level mmap
				},
				{
					FieldID:  101,
					Name:     "vector_field_mmap_disabled",
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "128"},
						{Key: common.MmapEnabledKey, Value: "false"}, // Field-level mmap explicitly disabled
					},
				},
				{
					FieldID:  102,
					Name:     "vector_field_mmap_enabled",
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "256"},
						{Key: common.MmapEnabledKey, Value: "true"}, // Field-level mmap explicitly enabled
					},
				},
			},
		},
		Properties: []*commonpb.KeyValuePair{
			{
				Key:   common.MmapEnabledKey,
				Value: "true", // Collection-level mmap enabled
			},
		},
	}

	req := packLoadSegmentRequest(
		task,
		action,
		collectionInfoResp.GetSchema(),
		collectionInfoResp.GetProperties(),
		&querypb.LoadMetaInfo{
			LoadType: querypb.LoadType_LoadCollection,
		},
		&querypb.SegmentLoadInfo{},
		nil,
	)

	// Check field without field-level mmap setting
	pkField := req.GetSchema().GetFields()[0]
	mmapCount := 0
	var mmapValue string
	for _, kv := range pkField.GetTypeParams() {
		if kv.Key == common.MmapEnabledKey {
			mmapCount++
			mmapValue = kv.Value
		}
	}
	s.Equalf(1, mmapCount, "Field %s should have exactly 1 mmap.enabled property", pkField.GetName())
	s.Equalf("true", mmapValue, "Field %s should inherit collection-level mmap=true", pkField.GetName())

	// Check field with field-level mmap=false (should not be overridden by collection-level)
	vectorField1 := req.GetSchema().GetFields()[1]
	mmapCount = 0
	mmapValues := []string{}
	for _, kv := range vectorField1.GetTypeParams() {
		if kv.Key == common.MmapEnabledKey {
			mmapCount++
			mmapValues = append(mmapValues, kv.Value)
		}
	}

	s.Equalf(1, mmapCount, "Field %s should have exactly 1 mmap.enabled property, got %d with values %v",
		vectorField1.GetName(), mmapCount, mmapValues)
	if mmapCount > 0 {
		s.Equalf("false", mmapValues[0], "Field-level mmap=false should be preserved for field %s", vectorField1.GetName())
	}

	// Check field with field-level mmap=true (should not be duplicated)
	vectorField2 := req.GetSchema().GetFields()[2]
	mmapCount = 0
	mmapValues = []string{}
	for _, kv := range vectorField2.GetTypeParams() {
		if kv.Key == common.MmapEnabledKey {
			mmapCount++
			mmapValues = append(mmapValues, kv.Value)
		}
	}
	s.Equalf(1, mmapCount, "Field %s should have exactly 1 mmap.enabled property, got %d with values %v",
		vectorField2.GetName(), mmapCount, mmapValues)
}

// TestFieldMmapPriorityOverCollection tests that field-level mmap settings have priority over collection-level settings
// The priority rule should be: field-level mmap > collection-level mmap > default (no mmap)
func (s *UtilsSuite) TestFieldMmapPriorityOverCollection() {
	ctx := context.Background()

	action := NewSegmentAction(1, ActionTypeGrow, "test-ch", 100)
	task, err := NewSegmentTask(
		ctx,
		time.Second,
		nil,
		1,
		newReplicaDefaultRG(10),
		commonpb.LoadPriority_LOW,
		action,
	)
	s.NoError(err)

	// Test case 1: Collection mmap=true, field mmap=false -> field should remain false
	s.Run("collection_true_field_false", func() {
		schema := &schemapb.CollectionSchema{
			Name: "test_collection",
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:  100,
					Name:     "field_with_mmap_false",
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "128"},
						{Key: common.MmapEnabledKey, Value: "false"}, // Field explicitly sets mmap=false
					},
				},
			},
		}

		collectionProps := []*commonpb.KeyValuePair{
			{Key: common.MmapEnabledKey, Value: "true"}, // Collection sets mmap=true
		}

		req := packLoadSegmentRequest(
			task,
			action,
			schema,
			collectionProps,
			&querypb.LoadMetaInfo{LoadType: querypb.LoadType_LoadCollection},
			&querypb.SegmentLoadInfo{},
			nil,
		)

		field := req.GetSchema().GetFields()[0]
		mmapCount := 0
		mmapValues := []string{}
		for _, kv := range field.GetTypeParams() {
			if kv.Key == common.MmapEnabledKey {
				mmapCount++
				mmapValues = append(mmapValues, kv.Value)
			}
		}

		// Should have exactly 1 mmap property
		s.Equalf(1, mmapCount, "Field should have exactly 1 mmap.enabled, got %d with values %v", mmapCount, mmapValues)
		// Field-level false should override collection-level true
		if mmapCount > 0 {
			s.Equal("false", mmapValues[0], "Field mmap=false should have priority over collection mmap=true")
		}
	})

	// Test case 2: Collection mmap=false, field mmap=true -> field should remain true
	s.Run("collection_false_field_true", func() {
		schema := &schemapb.CollectionSchema{
			Name: "test_collection",
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:  100,
					Name:     "field_with_mmap_true",
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "128"},
						{Key: common.MmapEnabledKey, Value: "true"}, // Field explicitly sets mmap=true
					},
				},
			},
		}

		collectionProps := []*commonpb.KeyValuePair{
			{Key: common.MmapEnabledKey, Value: "false"}, // Collection sets mmap=false
		}

		req := packLoadSegmentRequest(
			task,
			action,
			schema,
			collectionProps,
			&querypb.LoadMetaInfo{LoadType: querypb.LoadType_LoadCollection},
			&querypb.SegmentLoadInfo{},
			nil,
		)

		field := req.GetSchema().GetFields()[0]
		mmapCount := 0
		mmapValues := []string{}
		for _, kv := range field.GetTypeParams() {
			if kv.Key == common.MmapEnabledKey {
				mmapCount++
				mmapValues = append(mmapValues, kv.Value)
			}
		}

		// Should have exactly 1 mmap property
		s.Equalf(1, mmapCount, "Field should have exactly 1 mmap.enabled, got %d with values %v", mmapCount, mmapValues)
		// Field-level true should override collection-level false
		if mmapCount > 0 {
			s.Equal("true", mmapValues[0], "Field mmap=true should have priority over collection mmap=false")
		}
	})

	// Test case 3: Mixed fields - some with field-level settings, some without
	s.Run("mixed_fields", func() {
		schema := &schemapb.CollectionSchema{
			Name: "test_collection",
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         "no_mmap_setting",
					DataType:     schemapb.DataType_Int64,
					IsPrimaryKey: true,
					TypeParams:   []*commonpb.KeyValuePair{}, // No field-level mmap
				},
				{
					FieldID:  101,
					Name:     "field_mmap_false",
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "128"},
						{Key: common.MmapEnabledKey, Value: "false"},
					},
				},
				{
					FieldID:  102,
					Name:     "field_mmap_true",
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "256"},
						{Key: common.MmapEnabledKey, Value: "true"},
					},
				},
				{
					FieldID:  103,
					Name:     "no_mmap_scalar",
					DataType: schemapb.DataType_VarChar,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: "max_length", Value: "100"},
					},
				},
			},
		}

		collectionProps := []*commonpb.KeyValuePair{
			{Key: common.MmapEnabledKey, Value: "true"}, // Collection-level mmap=true
		}

		req := packLoadSegmentRequest(
			task,
			action,
			schema,
			collectionProps,
			&querypb.LoadMetaInfo{LoadType: querypb.LoadType_LoadCollection},
			&querypb.SegmentLoadInfo{},
			nil,
		)

		fields := req.GetSchema().GetFields()
		s.Equal(4, len(fields))

		// Check each field
		for i, expectedValues := range []struct {
			fieldName    string
			expectedMmap string
			description  string
		}{
			{"no_mmap_setting", "true", "Field without setting should inherit collection mmap=true"},
			{"field_mmap_false", "false", "Field with mmap=false should keep false despite collection mmap=true"},
			{"field_mmap_true", "true", "Field with mmap=true should keep true"},
			{"no_mmap_scalar", "true", "Scalar field without setting should inherit collection mmap=true"},
		} {
			field := fields[i]
			s.Equal(expectedValues.fieldName, field.GetName())

			mmapCount := 0
			var actualMmap string
			for _, kv := range field.GetTypeParams() {
				if kv.Key == common.MmapEnabledKey {
					mmapCount++
					actualMmap = kv.Value
				}
			}

			// Check no duplicates
			s.Equalf(1, mmapCount, "Field %s should have exactly 1 mmap.enabled property, got %d",
				field.GetName(), mmapCount)

			// Check correct value based on priority
			s.Equalf(expectedValues.expectedMmap, actualMmap,
				"%s for field %s", expectedValues.description, field.GetName())
		}
	})
}

func TestUtils(t *testing.T) {
	suite.Run(t, new(UtilsSuite))
}
