package paramtable

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

func TestKnowhereConfig_GetIndexParam(t *testing.T) {
	bt := NewBaseTable(SkipRemote(true))
	cfg := &knowhereConfig{}
	cfg.init(bt)

	// Set some initial config
	indexParams := map[string]interface{}{
		"knowhere.IVF_FLAT.build.nlist":       1024,
		"knowhere.HNSW.build.efConstruction":  360,
		"knowhere.DISKANN.search.search_list": 100,
	}

	for key, val := range indexParams {
		valStr, _ := json.Marshal(val)
		bt.Save(key, string(valStr))
	}

	tests := []struct {
		name          string
		indexType     string
		stage         string
		expectedKey   string
		expectedValue string
	}{
		{
			name:          "IVF_FLAT Build",
			indexType:     "IVF_FLAT",
			stage:         BuildStage,
			expectedKey:   "nlist",
			expectedValue: "1024",
		},
		{
			name:          "HNSW Build",
			indexType:     "HNSW",
			stage:         BuildStage,
			expectedKey:   "efConstruction",
			expectedValue: "360",
		},
		{
			name:          "DISKANN Search",
			indexType:     "DISKANN",
			stage:         SearchStage,
			expectedKey:   "search_list",
			expectedValue: "100",
		},
		{
			name:          "Non-existent",
			indexType:     "NON_EXISTENT",
			stage:         BuildStage,
			expectedKey:   "",
			expectedValue: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cfg.getIndexParam(tt.indexType, tt.stage)
			if tt.expectedKey != "" {
				assert.Contains(t, result, tt.expectedKey, "The result should contain the expected key")
				assert.Equal(t, tt.expectedValue, result[tt.expectedKey], "The value for the key should match the expected value")
			} else {
				assert.Empty(t, result, "The result should be empty for non-existent index type")
			}
		})
	}
}

func TestKnowhereConfig_GetRuntimeParameter(t *testing.T) {
	cfg := &knowhereConfig{}

	params, err := cfg.GetRuntimeParameter(BuildStage)
	assert.NoError(t, err)
	assert.Contains(t, params, BuildDramBudgetKey)

	params, err = cfg.GetRuntimeParameter(SearchStage)
	assert.NoError(t, err)
	assert.Empty(t, params)
}

func TestKnowhereConfig_UpdateParameter(t *testing.T) {
	bt := NewBaseTable(SkipRemote(true))
	cfg := &knowhereConfig{}
	cfg.init(bt)

	// Set some initial config
	indexParams := map[string]interface{}{
		"knowhere.IVF_FLAT.build.nlist":            1024,
		"knowhere.IVF_FLAT.build.num_build_thread": 12,
	}

	for key, val := range indexParams {
		valStr, _ := json.Marshal(val)
		bt.Save(key, string(valStr))
	}

	tests := []struct {
		name           string
		indexType      string
		stage          string
		inputParams    []*commonpb.KeyValuePair
		expectedParams map[string]string
	}{
		{
			name:      "IVF_FLAT Build",
			indexType: "IVF_FLAT",
			stage:     BuildStage,
			inputParams: []*commonpb.KeyValuePair{
				{Key: "nlist", Value: "128"},
				{Key: "existing_key", Value: "existing_value"},
			},
			expectedParams: map[string]string{
				"existing_key":     "existing_value",
				"nlist":            "128",
				"num_build_thread": "12",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := cfg.UpdateIndexParams(tt.indexType, tt.stage, tt.inputParams)
			assert.NoError(t, err)

			for key, expectedValue := range tt.expectedParams {
				assert.Equal(t, expectedValue, GetKeyFromSlice(result, key), "The value for key %s should match the expected value", key)
			}
		})
	}
}

func TestKnowhereConfig_MergeParameter(t *testing.T) {
	bt := NewBaseTable(SkipRemote(true))
	cfg := &knowhereConfig{}
	cfg.init(bt)

	indexParams := map[string]interface{}{
		"knowhere.IVF_FLAT.build.nlist":            1024,
		"knowhere.IVF_FLAT.build.num_build_thread": 12,
	}

	for key, val := range indexParams {
		valStr, _ := json.Marshal(val)
		bt.Save(key, string(valStr))
	}

	tests := []struct {
		name           string
		indexType      string
		stage          string
		inputParams    map[string]string
		expectedParams map[string]string
	}{
		{
			name:      "IVF_FLAT Build",
			indexType: "IVF_FLAT",
			stage:     BuildStage,
			inputParams: map[string]string{
				"nlist":        "128",
				"existing_key": "existing_value",
			},
			expectedParams: map[string]string{
				"existing_key":     "existing_value",
				"nlist":            "128",
				"num_build_thread": "12",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := cfg.MergeIndexParams(tt.indexType, tt.stage, tt.inputParams)
			assert.NoError(t, err)

			for key, expectedValue := range tt.expectedParams {
				assert.Equal(t, expectedValue, result[key], "The value for key %s should match the expected value", key)
			}
		})
	}
}

func TestKnowhereConfig_MergeWithResource(t *testing.T) {
	cfg := &knowhereConfig{}

	tests := []struct {
		name           string
		vecFieldSize   uint64
		inputParams    map[string]string
		expectedParams map[string]string
	}{
		{
			name:         "Merge with resource",
			vecFieldSize: 1024 * 1024 * 1024,
			inputParams: map[string]string{
				"existing_key": "existing_value",
			},
			expectedParams: map[string]string{
				"existing_key":     "existing_value",
				BuildDramBudgetKey: "", // We can't predict the exact value, but it should exist
				VecFieldSizeKey:    "1.000000",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := cfg.MergeResourceParams(tt.vecFieldSize, BuildStage, tt.inputParams)
			assert.NoError(t, err)

			for key, expectedValue := range tt.expectedParams {
				if expectedValue != "" {
					assert.Equal(t, expectedValue, result[key], "The value for key %s should match the expected value", key)
				} else {
					assert.Contains(t, result, key, "The result should contain the key %s", key)
					assert.NotEmpty(t, result[key], "The value for key %s should not be empty", key)
				}
			}
		})
	}
}

func TestGetKeyFromSlice(t *testing.T) {
	indexParams := []*commonpb.KeyValuePair{
		{Key: "key1", Value: "value1"},
		{Key: "key2", Value: "value2"},
	}

	assert.Equal(t, "value1", GetKeyFromSlice(indexParams, "key1"))
	assert.Equal(t, "value2", GetKeyFromSlice(indexParams, "key2"))
	assert.Equal(t, "", GetKeyFromSlice(indexParams, "non_existent_key"))
}
