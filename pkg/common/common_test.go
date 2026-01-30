package common

import (
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

func TestIsSystemField(t *testing.T) {
	type args struct {
		fieldID int64
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			args: args{fieldID: StartOfUserFieldID},
			want: false,
		},
		{
			args: args{fieldID: StartOfUserFieldID + 1},
			want: false,
		},
		{
			args: args{fieldID: TimeStampField},
			want: true,
		},
		{
			args: args{fieldID: RowIDField},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, IsSystemField(tt.args.fieldID), "IsSystemField(%v)", tt.args.fieldID)
		})
	}
}

func TestDatabaseProperties(t *testing.T) {
	props := []*commonpb.KeyValuePair{
		{
			Key:   DatabaseReplicaNumber,
			Value: "3",
		},
		{
			Key:   DatabaseResourceGroups,
			Value: strings.Join([]string{"rg1", "rg2"}, ","),
		},
	}

	replicaNum, err := DatabaseLevelReplicaNumber(props)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), replicaNum)

	rgs, err := DatabaseLevelResourceGroups(props)
	assert.NoError(t, err)
	assert.Contains(t, rgs, "rg1")
	assert.Contains(t, rgs, "rg2")

	// test prop not found
	_, err = DatabaseLevelReplicaNumber(nil)
	assert.Error(t, err)

	_, err = DatabaseLevelResourceGroups(nil)
	assert.Error(t, err)

	// test invalid prop value

	props = []*commonpb.KeyValuePair{
		{
			Key:   DatabaseReplicaNumber,
			Value: "xxxx",
		},
		{
			Key:   DatabaseResourceGroups,
			Value: "",
		},
	}
	_, err = DatabaseLevelReplicaNumber(props)
	assert.Error(t, err)

	_, err = DatabaseLevelResourceGroups(props)
	assert.Error(t, err)
}

func TestCommonPartitionKeyIsolation(t *testing.T) {
	getProto := func(val string) []*commonpb.KeyValuePair {
		return []*commonpb.KeyValuePair{
			{
				Key:   PartitionKeyIsolationKey,
				Value: val,
			},
		}
	}

	getMp := func(val string) map[string]string {
		return map[string]string{
			PartitionKeyIsolationKey: val,
		}
	}

	t.Run("pb", func(t *testing.T) {
		props := getProto("true")
		res, err := IsPartitionKeyIsolationKvEnabled(props...)
		assert.NoError(t, err)
		assert.True(t, res)

		props = getProto("false")
		res, err = IsPartitionKeyIsolationKvEnabled(props...)
		assert.NoError(t, err)
		assert.False(t, res)

		props = getProto("")
		res, err = IsPartitionKeyIsolationKvEnabled(props...)
		assert.ErrorContains(t, err, "failed to parse partition key isolation")
		assert.False(t, res)

		props = getProto("invalid")
		res, err = IsPartitionKeyIsolationKvEnabled(props...)
		assert.ErrorContains(t, err, "failed to parse partition key isolation")
		assert.False(t, res)
	})

	t.Run("map", func(t *testing.T) {
		props := getMp("true")
		res, err := IsPartitionKeyIsolationPropEnabled(props)
		assert.NoError(t, err)
		assert.True(t, res)

		props = getMp("false")
		res, err = IsPartitionKeyIsolationPropEnabled(props)
		assert.NoError(t, err)
		assert.False(t, res)

		props = getMp("")
		res, err = IsPartitionKeyIsolationPropEnabled(props)
		assert.ErrorContains(t, err, "failed to parse partition key isolation property")
		assert.False(t, res)

		props = getMp("invalid")
		res, err = IsPartitionKeyIsolationPropEnabled(props)
		assert.ErrorContains(t, err, "failed to parse partition key isolation property")
		assert.False(t, res)
	})
}

func TestShouldFieldBeLoaded(t *testing.T) {
	type testCase struct {
		tag          string
		input        []*commonpb.KeyValuePair
		expectOutput bool
		expectError  bool
	}

	testcases := []testCase{
		{tag: "no_params", expectOutput: true},
		{tag: "skipload_true", input: []*commonpb.KeyValuePair{{Key: FieldSkipLoadKey, Value: "true"}}, expectOutput: false},
		{tag: "skipload_false", input: []*commonpb.KeyValuePair{{Key: FieldSkipLoadKey, Value: "false"}}, expectOutput: true},
		{tag: "bad_skip_load_value", input: []*commonpb.KeyValuePair{{Key: FieldSkipLoadKey, Value: "abc"}}, expectError: true},
	}

	for _, tc := range testcases {
		t.Run(tc.tag, func(t *testing.T) {
			result, err := ShouldFieldBeLoaded(tc.input)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectOutput, result)
			}
		})
	}
}

func TestReplicateProperty(t *testing.T) {
	t.Run("ReplicateID", func(t *testing.T) {
		{
			p := []*commonpb.KeyValuePair{
				{
					Key:   ReplicateIDKey,
					Value: "1001",
				},
			}
			e, ok := IsReplicateEnabled(p)
			assert.True(t, e)
			assert.True(t, ok)
			i, ok := GetReplicateID(p)
			assert.True(t, ok)
			assert.Equal(t, "1001", i)
		}

		{
			p := []*commonpb.KeyValuePair{
				{
					Key:   ReplicateIDKey,
					Value: "",
				},
			}
			e, ok := IsReplicateEnabled(p)
			assert.False(t, e)
			assert.True(t, ok)
		}

		{
			p := []*commonpb.KeyValuePair{
				{
					Key:   "foo",
					Value: "1001",
				},
			}
			e, ok := IsReplicateEnabled(p)
			assert.False(t, e)
			assert.False(t, ok)
		}
	})

	t.Run("ReplicateTS", func(t *testing.T) {
		{
			p := []*commonpb.KeyValuePair{
				{
					Key:   ReplicateEndTSKey,
					Value: "1001",
				},
			}
			ts, ok := GetReplicateEndTS(p)
			assert.True(t, ok)
			assert.EqualValues(t, 1001, ts)
		}

		{
			p := []*commonpb.KeyValuePair{
				{
					Key:   ReplicateEndTSKey,
					Value: "foo",
				},
			}
			ts, ok := GetReplicateEndTS(p)
			assert.False(t, ok)
			assert.EqualValues(t, 0, ts)
		}

		{
			p := []*commonpb.KeyValuePair{
				{
					Key:   "foo",
					Value: "1001",
				},
			}
			ts, ok := GetReplicateEndTS(p)
			assert.False(t, ok)
			assert.EqualValues(t, 0, ts)
		}
	})
}

func TestIsEnableDynamicSchema(t *testing.T) {
	type testCase struct {
		tag         string
		input       []*commonpb.KeyValuePair
		expectFound bool
		expectValue bool
		expectError bool
	}

	cases := []testCase{
		{tag: "no_params", expectFound: false},
		{tag: "dynamicfield_true", input: []*commonpb.KeyValuePair{{Key: EnableDynamicSchemaKey, Value: "true"}}, expectFound: true, expectValue: true},
		{tag: "dynamicfield_false", input: []*commonpb.KeyValuePair{{Key: EnableDynamicSchemaKey, Value: "false"}}, expectFound: true, expectValue: false},
		{tag: "bad_kv_value", input: []*commonpb.KeyValuePair{{Key: EnableDynamicSchemaKey, Value: "abc"}}, expectFound: true, expectError: true},
	}

	for _, tc := range cases {
		t.Run(tc.tag, func(t *testing.T) {
			found, value, err := IsEnableDynamicSchema(tc.input)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tc.expectFound, found)
			assert.Equal(t, tc.expectValue, value)
		})
	}
}

func TestAllocAutoID(t *testing.T) {
	start, end, err := AllocAutoID(func(n uint32) (int64, int64, error) {
		return 100, 110, nil
	}, 10, 1)
	assert.NoError(t, err)
	assert.EqualValues(t, 0b0100, start>>60)
	assert.EqualValues(t, 0b0100, end>>60)
}

func TestFunctionProperty(t *testing.T) {
	assert.False(t, GetCollectionAllowInsertNonBM25FunctionOutputs([]*commonpb.KeyValuePair{}))
	assert.False(t, GetCollectionAllowInsertNonBM25FunctionOutputs(
		[]*commonpb.KeyValuePair{{Key: "other", Value: "test"}}),
	)
	assert.False(t, GetCollectionAllowInsertNonBM25FunctionOutputs(
		[]*commonpb.KeyValuePair{{Key: CollectionAllowInsertNonBM25FunctionOutputs, Value: "false"}}),
	)
	assert.False(t, GetCollectionAllowInsertNonBM25FunctionOutputs(
		[]*commonpb.KeyValuePair{{Key: CollectionAllowInsertNonBM25FunctionOutputs, Value: "test"}}),
	)
	assert.True(t, GetCollectionAllowInsertNonBM25FunctionOutputs(
		[]*commonpb.KeyValuePair{{Key: CollectionAllowInsertNonBM25FunctionOutputs, Value: "true"}}),
	)
}

func TestIsDisableFuncRuntimeCheck(t *testing.T) {
	disable, err := IsDisableFuncRuntimeCheck([]*commonpb.KeyValuePair{}...)
	assert.NoError(t, err)
	assert.False(t, disable)
	disable, err = IsDisableFuncRuntimeCheck([]*commonpb.KeyValuePair{{Key: DisableFuncRuntimeCheck, Value: "False"}}...)
	assert.NoError(t, err)
	assert.False(t, disable)
	disable, err = IsDisableFuncRuntimeCheck([]*commonpb.KeyValuePair{{Key: DisableFuncRuntimeCheck, Value: "True"}}...)
	assert.NoError(t, err)
	assert.True(t, disable)
	disable, err = IsDisableFuncRuntimeCheck([]*commonpb.KeyValuePair{{Key: DisableFuncRuntimeCheck, Value: "Error"}}...)
	assert.Error(t, err)
	assert.False(t, disable)
}

func TestGetCollectionTTL(t *testing.T) {
	type testCase struct {
		tag       string
		value     string
		expect    time.Duration
		expectErr bool
	}

	cases := []testCase{
		{tag: "normal_case", value: "3600", expect: time.Duration(3600) * time.Second, expectErr: false},
		{tag: "error_value", value: "error value", expectErr: true},
		{tag: "out_of_int64_range", value: "10000000000000000000000000000000000000000000000000000000000000000000000000000", expectErr: true},
		{tag: "negative", value: "-1", expect: -1 * time.Second},
	}

	for _, tc := range cases {
		t.Run(tc.tag, func(t *testing.T) {
			result, err := GetCollectionTTL([]*commonpb.KeyValuePair{{Key: CollectionTTLConfigKey, Value: tc.value}}, 0)
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.EqualValues(t, tc.expect, result)
			}
			result, err = GetCollectionTTLFromMap(map[string]string{CollectionTTLConfigKey: tc.value}, 0)
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.EqualValues(t, tc.expect, result)
			}
		})
	}

	t.Run("not_config", func(t *testing.T) {
		randValue := rand.Intn(100)
		result, err := GetCollectionTTL([]*commonpb.KeyValuePair{}, time.Duration(randValue)*time.Second)
		assert.NoError(t, err)
		assert.EqualValues(t, time.Duration(randValue)*time.Second, result)
		result, err = GetCollectionTTLFromMap(map[string]string{}, time.Duration(randValue)*time.Second)
		assert.NoError(t, err)
		assert.EqualValues(t, time.Duration(randValue)*time.Second, result)
	})
}

func TestWarmupPolicy(t *testing.T) {
	t.Run("GetWarmupPolicy", func(t *testing.T) {
		// Test when warmup key exists
		props := []*commonpb.KeyValuePair{
			{Key: WarmupKey, Value: WarmupSync},
		}
		policy, exist := GetWarmupPolicy(props...)
		assert.True(t, exist)
		assert.Equal(t, WarmupSync, policy)

		// Test when warmup key doesn't exist
		props = []*commonpb.KeyValuePair{
			{Key: "other_key", Value: "other_value"},
		}
		policy, exist = GetWarmupPolicy(props...)
		assert.False(t, exist)
		assert.Equal(t, "", policy)

		// Test empty props
		policy, exist = GetWarmupPolicy()
		assert.False(t, exist)
		assert.Equal(t, "", policy)
	})

	t.Run("GetWarmupPolicyByKey", func(t *testing.T) {
		props := []*commonpb.KeyValuePair{
			{Key: WarmupScalarFieldKey, Value: WarmupSync},
			{Key: WarmupVectorIndexKey, Value: WarmupDisable},
		}

		// Test getting scalar field warmup
		policy, exist := GetWarmupPolicyByKey(WarmupScalarFieldKey, props...)
		assert.True(t, exist)
		assert.Equal(t, WarmupSync, policy)

		// Test getting vector index warmup
		policy, exist = GetWarmupPolicyByKey(WarmupVectorIndexKey, props...)
		assert.True(t, exist)
		assert.Equal(t, WarmupDisable, policy)

		// Test key not found
		policy, exist = GetWarmupPolicyByKey(WarmupScalarIndexKey, props...)
		assert.False(t, exist)
		assert.Equal(t, "", policy)
	})

	t.Run("ValidateWarmupPolicy", func(t *testing.T) {
		// Valid values
		assert.NoError(t, ValidateWarmupPolicy(WarmupSync))
		assert.NoError(t, ValidateWarmupPolicy(WarmupDisable))

		// Invalid values
		assert.Error(t, ValidateWarmupPolicy("invalid"))
		assert.Error(t, ValidateWarmupPolicy(""))
		assert.Error(t, ValidateWarmupPolicy("async"))
	})

	t.Run("IsWarmupKey", func(t *testing.T) {
		// Valid warmup keys
		assert.True(t, IsWarmupKey(WarmupKey))
		assert.True(t, IsWarmupKey(WarmupScalarFieldKey))
		assert.True(t, IsWarmupKey(WarmupScalarIndexKey))
		assert.True(t, IsWarmupKey(WarmupVectorFieldKey))
		assert.True(t, IsWarmupKey(WarmupVectorIndexKey))

		// Invalid keys
		assert.False(t, IsWarmupKey("warmup.invalid"))
		assert.False(t, IsWarmupKey("other_key"))
		assert.False(t, IsWarmupKey(""))
	})

	t.Run("IsFieldWarmupKey", func(t *testing.T) {
		// Only WarmupKey is a field-level warmup key
		assert.True(t, IsFieldWarmupKey(WarmupKey))

		// Collection-level warmup keys are not field-level
		assert.False(t, IsFieldWarmupKey(WarmupScalarFieldKey))
		assert.False(t, IsFieldWarmupKey(WarmupScalarIndexKey))
		assert.False(t, IsFieldWarmupKey(WarmupVectorFieldKey))
		assert.False(t, IsFieldWarmupKey(WarmupVectorIndexKey))

		// Invalid keys
		assert.False(t, IsFieldWarmupKey("warmup.invalid"))
		assert.False(t, IsFieldWarmupKey("other_key"))
		assert.False(t, IsFieldWarmupKey(""))
	})

	t.Run("IsCollectionWarmupKey", func(t *testing.T) {
		// Collection-level warmup keys
		assert.True(t, IsCollectionWarmupKey(WarmupScalarFieldKey))
		assert.True(t, IsCollectionWarmupKey(WarmupScalarIndexKey))
		assert.True(t, IsCollectionWarmupKey(WarmupVectorFieldKey))
		assert.True(t, IsCollectionWarmupKey(WarmupVectorIndexKey))

		// WarmupKey is field-level, not collection-level
		assert.False(t, IsCollectionWarmupKey(WarmupKey))

		// Invalid keys
		assert.False(t, IsCollectionWarmupKey("warmup.invalid"))
		assert.False(t, IsCollectionWarmupKey("other_key"))
		assert.False(t, IsCollectionWarmupKey(""))
	})
}

func TestWKTWKBConversion(t *testing.T) {
	testCases := []struct {
		name string
		wkt  string
	}{
		{"Point Empty", "POINT EMPTY"},
		{"Polygon Empty", "POLYGON EMPTY"},
		{"Point with coords", "POINT (1 2)"},
		{"Polygon with coords", "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wkb, err := ConvertWKTToWKB(tc.wkt)
			assert.NoError(t, err)
			assert.NotNil(t, wkb)

			wktResult, err := ConvertWKBToWKT(wkb)
			assert.NoError(t, err)
			assert.Equal(t, tc.wkt, wktResult)
		})
	}
}
