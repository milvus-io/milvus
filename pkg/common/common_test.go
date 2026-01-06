package common

import (
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
			result, err := GetCollectionTTL([]*commonpb.KeyValuePair{{Key: CollectionTTLConfigKey, Value: tc.value}})
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.EqualValues(t, tc.expect, result)
			}
			result, err = GetCollectionTTLFromMap(map[string]string{CollectionTTLConfigKey: tc.value})
			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.EqualValues(t, tc.expect, result)
			}
		})
	}

	t.Run("not_config", func(t *testing.T) {
		result, err := GetCollectionTTL([]*commonpb.KeyValuePair{})
		assert.NoError(t, err)
		assert.EqualValues(t, -1, result)
		result, err = GetCollectionTTLFromMap(map[string]string{})
		assert.NoError(t, err)
		assert.EqualValues(t, -1, result)
	})
}

func TestConvertWKTToWKB(t *testing.T) {
	wkt := "POINT EMPTY"
	wkb, err := ConvertWKTToWKB(wkt)
	assert.NoError(t, err)
	assert.NotNil(t, wkb)

	wktResult, err := ConvertWKBToWKT(wkb)
	assert.NoError(t, err)
	assert.Equal(t, wkt, wktResult)

	wkt2 := "POLYGON EMPTY"
	wkb2, err := ConvertWKTToWKB(wkt2)
	assert.NoError(t, err)
	assert.NotNil(t, wkb2)

	wktResult2, err := ConvertWKBToWKT(wkb2)
	assert.NoError(t, err)
	assert.Equal(t, wkt2, wktResult2)
}
