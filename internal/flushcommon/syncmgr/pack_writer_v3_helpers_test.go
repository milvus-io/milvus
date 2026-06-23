// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package syncmgr

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

func TestClassifyLoonErr(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		assert.NoError(t, classifyLoonErr(nil))
	})
	t.Run("transient stays retryable", func(t *testing.T) {
		wrapped := fmt.Errorf("loon failed: %w", packed.ErrLoonTransient)
		out := classifyLoonErr(wrapped)
		require.Error(t, out)
		assert.True(t, errors.Is(out, packed.ErrLoonTransient),
			"transient error must remain retryable")
		assert.True(t, retry.IsRecoverable(out),
			"transient error must remain recoverable so retry.Do retries")
	})
	t.Run("non-transient becomes unrecoverable", func(t *testing.T) {
		out := classifyLoonErr(errors.New("permanent disk fault"))
		require.Error(t, out)
		assert.False(t, retry.IsRecoverable(out),
			"non-transient error must be wrapped Unrecoverable so retry stops")
	})
}

func TestBuildTextColumnConfigs(t *testing.T) {
	// Required for paramtable.Get().DataNodeCfg.* to return defaults.
	paramtable.Init()

	t.Run("no text fields returns nil", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "varchar", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "vector", DataType: schemapb.DataType_FloatVector},
		}}
		got := buildTextColumnConfigs(schema, "/seg/base")
		assert.Empty(t, got, "schema without TEXT fields should produce no configs")
	})

	t.Run("one text field produces one config with derived lob path", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "doc", DataType: schemapb.DataType_Text},
		}}
		got := buildTextColumnConfigs(schema, "files/insert_log/1/2")
		require.Len(t, got, 1)
		assert.Equal(t, int64(101), got[0].FieldID)
		assert.Equal(t, "files/insert_log/1/2/lobs/101", got[0].LobBasePath)
	})

	t.Run("multiple text fields produce one config each", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "title", DataType: schemapb.DataType_Text},
			{FieldID: 102, Name: "body", DataType: schemapb.DataType_Text},
		}}
		got := buildTextColumnConfigs(schema, "p")
		require.Len(t, got, 2)
		ids := []int64{got[0].FieldID, got[1].FieldID}
		assert.ElementsMatch(t, []int64{101, 102}, ids)
		for _, cfg := range got {
			assert.Contains(t, cfg.LobBasePath, "lobs/")
		}
	})
}
