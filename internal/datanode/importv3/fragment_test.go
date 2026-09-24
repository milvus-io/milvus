// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestSortFieldsValidatesPersistedTypes(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_Int64},
		{FieldID: 101, DataType: schemapb.DataType_VarChar},
	}}
	fields, err := SortFields(&datapb.SortSpec{Fields: []*datapb.SortFieldSpec{
		{FieldId: 101, DataType: schemapb.DataType_VarChar},
		{FieldId: 100, DataType: schemapb.DataType_Int64},
	}}, schema)
	require.NoError(t, err)
	require.Equal(t, []int64{101, 100}, fields)

	_, err = SortFields(&datapb.SortSpec{Fields: []*datapb.SortFieldSpec{{
		FieldId: 101, DataType: schemapb.DataType_Int64,
	}}}, schema)
	require.Error(t, err)
}
