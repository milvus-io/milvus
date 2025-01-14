package exprutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/proto/planpb"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestParsePartitionKeys(t *testing.T) {
	prefix := "TestParsePartitionKeys"
	collectionName := prefix + funcutil.GenRandomStr()

	fieldName2Type := make(map[string]schemapb.DataType)
	fieldName2Type["int64_field"] = schemapb.DataType_Int64
	fieldName2Type["varChar_field"] = schemapb.DataType_VarChar
	fieldName2Type["fvec_field"] = schemapb.DataType_FloatVector
	schema := testutil.ConstructCollectionSchemaByDataType(collectionName, fieldName2Type,
		"int64_field", false, 8)
	partitionKeyField := &schemapb.FieldSchema{
		Name:           "partition_key_field",
		DataType:       schemapb.DataType_Int64,
		IsPartitionKey: true,
	}
	schema.Fields = append(schema.Fields, partitionKeyField)

	fieldID := common.StartOfUserFieldID
	for _, field := range schema.Fields {
		field.FieldID = int64(fieldID)
		fieldID++
	}
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)

	queryInfo := &planpb.QueryInfo{
		Topk:         10,
		MetricType:   "L2",
		SearchParams: "",
		RoundDecimal: -1,
	}

	type testCase struct {
		name                 string
		expr                 string
		expected             int
		validPartitionKeys   []int64
		invalidPartitionKeys []int64
	}
	cases := []testCase{
		{
			name:                 "binary_expr_and with term",
			expr:                 "partition_key_field in [7, 8] && int64_field >= 10",
			expected:             2,
			validPartitionKeys:   []int64{7, 8},
			invalidPartitionKeys: []int64{},
		},
		{
			name:                 "binary_expr_and with equal",
			expr:                 "partition_key_field == 7 && int64_field >= 10",
			expected:             1,
			validPartitionKeys:   []int64{7},
			invalidPartitionKeys: []int64{},
		},
		{
			name:                 "binary_expr_and with term2",
			expr:                 "partition_key_field in [7, 8] && int64_field == 10",
			expected:             2,
			validPartitionKeys:   []int64{7, 8},
			invalidPartitionKeys: []int64{10},
		},
		{
			name:                 "binary_expr_and with partition key in range",
			expr:                 "partition_key_field in [7, 8] && partition_key_field > 9",
			expected:             2,
			validPartitionKeys:   []int64{7, 8},
			invalidPartitionKeys: []int64{9},
		},
		{
			name:                 "binary_expr_and with partition key in range2",
			expr:                 "int64_field == 10 && partition_key_field > 9",
			expected:             0,
			validPartitionKeys:   []int64{},
			invalidPartitionKeys: []int64{},
		},
		{
			name:                 "binary_expr_and with term and not",
			expr:                 "partition_key_field in [7, 8] && partition_key_field not in [10, 20]",
			expected:             2,
			validPartitionKeys:   []int64{7, 8},
			invalidPartitionKeys: []int64{10, 20},
		},
		{
			name:                 "binary_expr_or with term and not",
			expr:                 "partition_key_field in [7, 8] or partition_key_field not in [10, 20]",
			expected:             0,
			validPartitionKeys:   []int64{},
			invalidPartitionKeys: []int64{},
		},
		{
			name:                 "binary_expr_or with term and not 2",
			expr:                 "partition_key_field in [7, 8] or int64_field not in [10, 20]",
			expected:             2,
			validPartitionKeys:   []int64{7, 8},
			invalidPartitionKeys: []int64{10, 20},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// test search plan
			searchPlan, err := planparserv2.CreateSearchPlan(schemaHelper, tc.expr, "fvec_field", queryInfo, nil)
			assert.NoError(t, err)
			expr, err := ParseExprFromPlan(searchPlan)
			assert.NoError(t, err)
			partitionKeys := ParseKeys(expr, PartitionKey)
			assert.Equal(t, tc.expected, len(partitionKeys))
			for _, key := range partitionKeys {
				int64Val := key.Val.(*planpb.GenericValue_Int64Val).Int64Val
				assert.Contains(t, tc.validPartitionKeys, int64Val)
				assert.NotContains(t, tc.invalidPartitionKeys, int64Val)
			}

			// test query plan
			queryPlan, err := planparserv2.CreateRetrievePlan(schemaHelper, tc.expr, nil)
			assert.NoError(t, err)
			expr, err = ParseExprFromPlan(queryPlan)
			assert.NoError(t, err)
			partitionKeys = ParseKeys(expr, PartitionKey)
			assert.Equal(t, tc.expected, len(partitionKeys))
			for _, key := range partitionKeys {
				int64Val := key.Val.(*planpb.GenericValue_Int64Val).Int64Val
				assert.Contains(t, tc.validPartitionKeys, int64Val)
				assert.NotContains(t, tc.invalidPartitionKeys, int64Val)
			}
		})
	}
}

func TestParseIntRanges(t *testing.T) {
	prefix := "TestParseRanges"
	clusterKeyField := "cluster_key_field"
	collectionName := prefix + funcutil.GenRandomStr()

	fieldName2Type := make(map[string]schemapb.DataType)
	fieldName2Type["int64_field"] = schemapb.DataType_Int64
	fieldName2Type["varChar_field"] = schemapb.DataType_VarChar
	fieldName2Type["fvec_field"] = schemapb.DataType_FloatVector
	schema := testutil.ConstructCollectionSchemaByDataType(collectionName, fieldName2Type,
		"int64_field", false, 8)
	clusterKeyFieldSchema := &schemapb.FieldSchema{
		Name:            clusterKeyField,
		DataType:        schemapb.DataType_Int64,
		IsClusteringKey: true,
	}
	schema.Fields = append(schema.Fields, clusterKeyFieldSchema)

	fieldID := common.StartOfUserFieldID
	for _, field := range schema.Fields {
		field.FieldID = int64(fieldID)
		fieldID++
	}
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	// test query plan
	{
		expr := "cluster_key_field > 50"
		queryPlan, err := planparserv2.CreateRetrievePlan(schemaHelper, expr, nil)
		assert.NoError(t, err)
		planExpr, err := ParseExprFromPlan(queryPlan)
		assert.NoError(t, err)
		parsedRanges, matchALL := ParseRanges(planExpr, ClusteringKey)
		assert.False(t, matchALL)
		assert.Equal(t, 1, len(parsedRanges))
		range0 := parsedRanges[0]
		assert.Equal(t, range0.lower.Val.(*planpb.GenericValue_Int64Val).Int64Val, int64(50))
		assert.Nil(t, range0.upper)
		assert.Equal(t, range0.includeLower, false)
		assert.Equal(t, range0.includeUpper, false)
	}

	// test binary query plan
	{
		expr := "cluster_key_field > 50 and cluster_key_field <= 100"
		queryPlan, err := planparserv2.CreateRetrievePlan(schemaHelper, expr, nil)
		assert.NoError(t, err)
		planExpr, err := ParseExprFromPlan(queryPlan)
		assert.NoError(t, err)
		parsedRanges, matchALL := ParseRanges(planExpr, ClusteringKey)
		assert.False(t, matchALL)
		assert.Equal(t, 1, len(parsedRanges))
		range0 := parsedRanges[0]
		assert.Equal(t, range0.lower.Val.(*planpb.GenericValue_Int64Val).Int64Val, int64(50))
		assert.Equal(t, false, range0.includeLower)
		assert.Equal(t, true, range0.includeUpper)
	}

	// test binary query plan
	{
		expr := "cluster_key_field >= 50 and cluster_key_field < 100"
		queryPlan, err := planparserv2.CreateRetrievePlan(schemaHelper, expr, nil)
		assert.NoError(t, err)
		planExpr, err := ParseExprFromPlan(queryPlan)
		assert.NoError(t, err)
		parsedRanges, matchALL := ParseRanges(planExpr, ClusteringKey)
		assert.False(t, matchALL)
		assert.Equal(t, 1, len(parsedRanges))
		range0 := parsedRanges[0]
		assert.Equal(t, range0.lower.Val.(*planpb.GenericValue_Int64Val).Int64Val, int64(50))
		assert.Equal(t, true, range0.includeLower)
		assert.Equal(t, false, range0.includeUpper)
	}

	// test binary query plan
	{
		expr := "cluster_key_field in [100]"
		queryPlan, err := planparserv2.CreateRetrievePlan(schemaHelper, expr, nil)
		assert.NoError(t, err)
		planExpr, err := ParseExprFromPlan(queryPlan)
		assert.NoError(t, err)
		parsedRanges, matchALL := ParseRanges(planExpr, ClusteringKey)
		assert.False(t, matchALL)
		assert.Equal(t, 1, len(parsedRanges))
		range0 := parsedRanges[0]
		assert.Equal(t, range0.lower.Val.(*planpb.GenericValue_Int64Val).Int64Val, int64(100))
		assert.Equal(t, true, range0.includeLower)
		assert.Equal(t, true, range0.includeUpper)
	}
}

func TestParseStrRanges(t *testing.T) {
	prefix := "TestParseRanges"
	clusterKeyField := "cluster_key_field"
	collectionName := prefix + funcutil.GenRandomStr()

	fieldName2Type := make(map[string]schemapb.DataType)
	fieldName2Type["int64_field"] = schemapb.DataType_Int64
	fieldName2Type["varChar_field"] = schemapb.DataType_VarChar
	fieldName2Type["fvec_field"] = schemapb.DataType_FloatVector
	schema := testutil.ConstructCollectionSchemaByDataType(collectionName, fieldName2Type,
		"int64_field", false, 8)
	clusterKeyFieldSchema := &schemapb.FieldSchema{
		Name:            clusterKeyField,
		DataType:        schemapb.DataType_VarChar,
		IsClusteringKey: true,
	}
	schema.Fields = append(schema.Fields, clusterKeyFieldSchema)

	fieldID := common.StartOfUserFieldID
	for _, field := range schema.Fields {
		field.FieldID = int64(fieldID)
		fieldID++
	}
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	// test query plan
	{
		expr := "cluster_key_field >= \"aaa\""
		queryPlan, err := planparserv2.CreateRetrievePlan(schemaHelper, expr, nil)
		assert.NoError(t, err)
		planExpr, err := ParseExprFromPlan(queryPlan)
		assert.NoError(t, err)
		parsedRanges, matchALL := ParseRanges(planExpr, ClusteringKey)
		assert.False(t, matchALL)
		assert.Equal(t, 1, len(parsedRanges))
		range0 := parsedRanges[0]
		assert.Equal(t, range0.lower.Val.(*planpb.GenericValue_StringVal).StringVal, "aaa")
		assert.Nil(t, range0.upper)
		assert.Equal(t, range0.includeLower, true)
		assert.Equal(t, range0.includeUpper, false)
	}
}

func TestValidatePartitionKeyIsolation(t *testing.T) {
	prefix := "TestValidatePartitionKeyIsolation"
	collectionName := prefix + funcutil.GenRandomStr()

	fieldName2Type := make(map[string]schemapb.DataType)
	fieldName2Type["int64_field"] = schemapb.DataType_Int64
	fieldName2Type["varChar_field"] = schemapb.DataType_VarChar
	fieldName2Type["fvec_field"] = schemapb.DataType_FloatVector
	schema := testutil.ConstructCollectionSchemaByDataType(collectionName, fieldName2Type,
		"int64_field", false, 8)
	schema.Properties = append(schema.Properties, &commonpb.KeyValuePair{
		Key:   common.PartitionKeyIsolationKey,
		Value: "true",
	})
	partitionKeyField := &schemapb.FieldSchema{
		Name:           "key_field",
		DataType:       schemapb.DataType_Int64,
		IsPartitionKey: true,
	}
	schema.Fields = append(schema.Fields, partitionKeyField)
	fieldID := common.StartOfUserFieldID
	for _, field := range schema.Fields {
		field.FieldID = int64(fieldID)
		fieldID++
	}
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)

	type testCase struct {
		name                string
		expr                string
		expectedErrorString string
	}
	cases := []testCase{
		{
			name:                "partition key isolation equal",
			expr:                "key_field == 10",
			expectedErrorString: "",
		},
		{
			name:                "partition key isolation equal AND with same field equal",
			expr:                "key_field == 10 && key_field == 10",
			expectedErrorString: "",
		},
		{
			name:                "partition key isolation equal AND with same field equal diff",
			expr:                "key_field == 10 && key_field == 20",
			expectedErrorString: "",
		},
		{
			name:                "partition key isolation equal AND with same field equal 3",
			expr:                "key_field == 10 && key_field == 11 && key_field == 12",
			expectedErrorString: "",
		},
		{
			name:                "partition key isolation equal AND with varchar field equal",
			expr:                "key_field == 10 && varChar_field == 'a'",
			expectedErrorString: "",
		},
		{
			name:                "partition key isolation equal AND with varchar field not equal",
			expr:                "key_field == 10 && varChar_field != 'a'",
			expectedErrorString: "",
		},
		{
			name:                "partition key isolation equal AND with varchar field in",
			expr:                "key_field == 10 && varChar_field in ['a', 'b']",
			expectedErrorString: "",
		},
		{
			name:                "partition key isolation equal AND with varchar field in Reversed",
			expr:                "varChar_field in ['a', 'b'] && key_field == 10",
			expectedErrorString: "",
		},
		{
			name:                "partition key isolation equal AND with varchar field OR",
			expr:                "key_field == 10 && (varChar_field == 'a' || varChar_field == 'b')",
			expectedErrorString: "",
		},
		{
			name:                "partition key isolation equal AND with varchar field OR Reversed",
			expr:                "(varChar_field == 'a' || varChar_field == 'b') && key_field == 10",
			expectedErrorString: "",
		},
		{
			name:                "partition key isolation equal to arithmic operations",
			expr:                "key_field == (1+1)",
			expectedErrorString: "",
		},
		{
			name:                "partition key isolation empty",
			expr:                "",
			expectedErrorString: "partition key not found in expr or the expr is invalid when validating partition key isolation",
		},
		{
			name:                "partition key isolation not equal",
			expr:                "key_field != 10",
			expectedErrorString: "partition key isolation does not support NotEqual",
		},
		{
			name:                "partition key isolation term",
			expr:                "key_field in [10]",
			expectedErrorString: "partition key isolation does not support IN",
		},
		{
			name:                "partition key isolation term multiple",
			expr:                "key_field in [10, 20]",
			expectedErrorString: "partition key isolation does not support IN",
		},
		{
			name:                "partition key isolation NOT term",
			expr:                "key_field not in [10]",
			expectedErrorString: "partition key isolation does not support IN",
		},
		{
			name:                "partition key isolation less",
			expr:                "key_field < 10",
			expectedErrorString: "partition key isolation does not support LessThan",
		},
		{
			name:                "partition key isolation less or equal",
			expr:                "key_field <= 10",
			expectedErrorString: "partition key isolation does not support LessEq",
		},
		{
			name:                "partition key isolation greater",
			expr:                "key_field > 10",
			expectedErrorString: "partition key isolation does not support GreaterThan",
		},
		{
			name:                "partition key isolation equal greator or equal",
			expr:                "key_field >= 10",
			expectedErrorString: "partition key isolation does not support GreaterEqual",
		},
		{
			name:                "partition key isolation binary range",
			expr:                "1 < key_field < 10",
			expectedErrorString: "partition key isolation does not support BinaryRange",
		},
		{
			name:                "partition key isolation NOT equal",
			expr:                "not(key_field == 10)",
			expectedErrorString: "partition key isolation does not support NOT",
		},
		{
			name:                "partition key isolation equal AND with same field term",
			expr:                "key_field == 10 && key_field in [10]",
			expectedErrorString: "partition key isolation does not support IN",
		},
		{
			name:                "partition key isolation equal OR with same field equal",
			expr:                "key_field == 10 || key_field == 11",
			expectedErrorString: "partition key isolation does not support OR",
		},
		{
			name:                "partition key isolation equal OR with same field equal Reversed",
			expr:                "key_field == 11 || key_field == 10",
			expectedErrorString: "partition key isolation does not support OR",
		},
		{
			name:                "partition key isolation equal OR with other field equal",
			expr:                "key_field == 10 || varChar_field == 'a'",
			expectedErrorString: "partition key isolation does not support OR",
		},
		{
			name:                "partition key isolation equal OR with other field equal Reversed",
			expr:                "varChar_field == 'a' || key_field == 10",
			expectedErrorString: "partition key isolation does not support OR",
		},
		{
			name:                "partition key isolation equal OR with other field equal",
			expr:                "key_field == 10 || varChar_field == 'a'",
			expectedErrorString: "partition key isolation does not support OR",
		},
		{
			name:                "partition key isolation equal AND",
			expr:                "key_field == 10 && (key_field == 10 || key_field == 11)",
			expectedErrorString: "partition key isolation does not support OR",
		},
		{
			name:                "partition key isolation other field equal",
			expr:                "varChar_field == 'a'",
			expectedErrorString: "partition key not found in expr or the expr is invalid when validating partition key isolation",
		},
		{
			name:                "partition key isolation other field equal AND",
			expr:                "varChar_field == 'a' && int64_field == 1",
			expectedErrorString: "partition key not found in expr or the expr is invalid when validating partition key isolation",
		},
		{
			name:                "partition key isolation complex OR",
			expr:                "(key_field == 10 and int64_field == 11) or (key_field == 10 and varChar_field == 'a')",
			expectedErrorString: "partition key isolation does not support OR",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			queryPlan, err := planparserv2.CreateRetrievePlan(schemaHelper, tc.expr, nil)
			assert.NoError(t, err)
			planExpr, err := ParseExprFromPlan(queryPlan)
			assert.NoError(t, err)
			if tc.expectedErrorString != "" {
				assert.ErrorContains(t, ValidatePartitionKeyIsolation(planExpr), tc.expectedErrorString)
			} else {
				assert.NoError(t, ValidatePartitionKeyIsolation(planExpr))
			}
		})
	}
}
