package indexparamcheck

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/config"
	"github.com/milvus-io/milvus/pkg/util/metric"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func Test_CheckIntByRange(t *testing.T) {
	params := map[string]string{
		"1":  strconv.Itoa(1),
		"2":  strconv.Itoa(2),
		"3":  strconv.Itoa(3),
		"s1": "s1",
		"s2": "s2",
		"s3": "s3",
	}

	cases := []struct {
		params map[string]string
		key    string
		min    int
		max    int
		want   bool
	}{
		{params, "1", 0, 4, true},
		{params, "2", 0, 4, true},
		{params, "3", 0, 4, true},
		{params, "1", 4, 5, false},
		{params, "2", 4, 5, false},
		{params, "3", 4, 5, false},
		{params, "4", 0, 4, false},
		{params, "5", 0, 4, false},
		{params, "6", 0, 4, false},
		{params, "s1", 0, 4, false},
		{params, "s2", 0, 4, false},
		{params, "s3", 0, 4, false},
		{params, "s4", 0, 4, false},
		{params, "s5", 0, 4, false},
		{params, "s6", 0, 4, false},
	}

	for _, test := range cases {
		if got := CheckIntByRange(test.params, test.key, test.min, test.max); got != test.want {
			t.Errorf("CheckIntByRange(%v, %v, %v, %v) = %v", test.params, test.key, test.min, test.max, test.want)
		}
	}
}

func Test_CheckStrByValues(t *testing.T) {
	params := map[string]string{
		"1": strconv.Itoa(1),
		"2": strconv.Itoa(2),
		"3": strconv.Itoa(3),
	}

	cases := []struct {
		params    map[string]string
		key       string
		container []string
		want      bool
	}{
		{params, "1", []string{"1", "2", "3"}, true},
		{params, "2", []string{"1", "2", "3"}, true},
		{params, "3", []string{"1", "2", "3"}, true},
		{params, "1", []string{"4", "5", "6"}, false},
		{params, "2", []string{"4", "5", "6"}, false},
		{params, "3", []string{"4", "5", "6"}, false},
		{params, "1", []string{}, false},
		{params, "2", []string{}, false},
		{params, "3", []string{}, false},
		{params, "4", []string{"1", "2", "3"}, false},
		{params, "5", []string{"1", "2", "3"}, false},
		{params, "6", []string{"1", "2", "3"}, false},
		{params, "4", []string{"4", "5", "6"}, false},
		{params, "5", []string{"4", "5", "6"}, false},
		{params, "6", []string{"4", "5", "6"}, false},
		{params, "4", []string{}, false},
		{params, "5", []string{}, false},
		{params, "6", []string{}, false},
	}

	for _, test := range cases {
		if got := CheckStrByValues(test.params, test.key, test.container); got != test.want {
			t.Errorf("CheckStrByValues(%v, %v, %v) = %v", test.params, test.key, test.container, test.want)
		}
	}
}

func Test_CheckAutoIndex(t *testing.T) {
	t.Run("index type not found", func(t *testing.T) {
		mgr := config.NewManager()
		mgr.SetConfig("autoIndex.params.build", `{"M": 30}`)
		p := &paramtable.AutoIndexConfig{
			IndexParams: paramtable.ParamItem{
				Key: "autoIndex.params.build",
			},
		}
		p.IndexParams.Init(mgr)
		assert.Panics(t, func() {
			CheckAutoIndexHelper(p.IndexParams.Key, p.IndexParams.GetAsJSONMap(), schemapb.DataType_FloatVector)
		})
	})

	t.Run("unsupported index type", func(t *testing.T) {
		mgr := config.NewManager()
		mgr.SetConfig("autoIndex.params.build", `{"index_type": "not supported"}`)
		p := &paramtable.AutoIndexConfig{
			IndexParams: paramtable.ParamItem{
				Key: "autoIndex.params.build",
			},
		}
		p.IndexParams.Init(mgr)
		assert.Panics(t, func() {
			CheckAutoIndexHelper(p.IndexParams.Key, p.IndexParams.GetAsJSONMap(), schemapb.DataType_FloatVector)
		})
	})

	t.Run("normal case, hnsw", func(t *testing.T) {
		mgr := config.NewManager()
		mgr.SetConfig("autoIndex.params.build", `{"M": 30,"efConstruction": 360,"index_type": "HNSW"}`)
		p := &paramtable.AutoIndexConfig{
			IndexParams: paramtable.ParamItem{
				Key:       "autoIndex.params.build",
				Formatter: paramtable.GetBuildParamFormatter(paramtable.FloatVectorDefaultMetricType, "autoIndex.params.build"),
			},
		}
		p.IndexParams.Init(mgr)

		assert.NotPanics(t, func() {
			CheckAutoIndexHelper(p.IndexParams.Key, p.IndexParams.GetAsJSONMap(), schemapb.DataType_FloatVector)
		})
		metricType, exist := p.IndexParams.GetAsJSONMap()[common.MetricTypeKey]
		assert.True(t, exist)
		assert.Equal(t, metric.COSINE, metricType)
	})

	t.Run("normal case, binary vector", func(t *testing.T) {
		mgr := config.NewManager()
		mgr.SetConfig("autoIndex.params.binary.build", `{"nlist": 1024, "index_type": "BIN_IVF_FLAT"}`)
		p := &paramtable.AutoIndexConfig{
			BinaryIndexParams: paramtable.ParamItem{
				Key:       "autoIndex.params.binary.build",
				Formatter: paramtable.GetBuildParamFormatter(paramtable.BinaryVectorDefaultMetricType, "autoIndex.params.binary.build"),
			},
		}
		p.BinaryIndexParams.Init(mgr)

		assert.NotPanics(t, func() {
			CheckAutoIndexHelper(p.BinaryIndexParams.Key, p.BinaryIndexParams.GetAsJSONMap(), schemapb.DataType_BinaryVector)
		})
		metricType, exist := p.BinaryIndexParams.GetAsJSONMap()[common.MetricTypeKey]
		assert.True(t, exist)
		assert.Equal(t, metric.HAMMING, metricType)
	})

	t.Run("normal case, sparse vector", func(t *testing.T) {
		mgr := config.NewManager()
		mgr.SetConfig("autoIndex.params.sparse.build", `{"index_type": "SPARSE_INVERTED_INDEX", "metric_type": "IP"}`)
		p := &paramtable.AutoIndexConfig{
			SparseIndexParams: paramtable.ParamItem{
				Key:       "autoIndex.params.sparse.build",
				Formatter: paramtable.GetBuildParamFormatter(paramtable.SparseFloatVectorDefaultMetricType, "autoIndex.params.sparse.build"),
			},
		}
		p.SparseIndexParams.Init(mgr)

		assert.NotPanics(t, func() {
			CheckAutoIndexHelper(p.SparseIndexParams.Key, p.SparseIndexParams.GetAsJSONMap(), schemapb.DataType_SparseFloatVector)
		})
		metricType, exist := p.SparseIndexParams.GetAsJSONMap()[common.MetricTypeKey]
		assert.True(t, exist)
		assert.Equal(t, metric.IP, metricType)
	})

	t.Run("normal case, ivf flat", func(t *testing.T) {
		mgr := config.NewManager()
		mgr.SetConfig("autoIndex.params.build", `{"nlist": 30, "index_type": "IVF_FLAT"}`)
		p := &paramtable.AutoIndexConfig{
			IndexParams: paramtable.ParamItem{
				Key:       "autoIndex.params.build",
				Formatter: paramtable.GetBuildParamFormatter(paramtable.FloatVectorDefaultMetricType, "autoIndex.params.build"),
			},
		}
		p.IndexParams.Init(mgr)

		assert.NotPanics(t, func() {
			CheckAutoIndexHelper(p.IndexParams.Key, p.IndexParams.GetAsJSONMap(), schemapb.DataType_FloatVector)
		})
		metricType, exist := p.IndexParams.GetAsJSONMap()[common.MetricTypeKey]
		assert.True(t, exist)
		assert.Equal(t, metric.COSINE, metricType)
	})

	t.Run("normal case, ivf flat", func(t *testing.T) {
		mgr := config.NewManager()
		mgr.SetConfig("autoIndex.params.build", `{"nlist": 30, "index_type": "IVF_FLAT"}`)
		p := &paramtable.AutoIndexConfig{
			IndexParams: paramtable.ParamItem{
				Key:       "autoIndex.params.build",
				Formatter: paramtable.GetBuildParamFormatter(paramtable.FloatVectorDefaultMetricType, "autoIndex.params.build"),
			},
		}
		p.IndexParams.Init(mgr)

		assert.NotPanics(t, func() {
			CheckAutoIndexHelper(p.IndexParams.Key, p.IndexParams.GetAsJSONMap(), schemapb.DataType_FloatVector)
		})
		metricType, exist := p.IndexParams.GetAsJSONMap()[common.MetricTypeKey]
		assert.True(t, exist)
		assert.Equal(t, metric.COSINE, metricType)
	})

	t.Run("normal case, diskann", func(t *testing.T) {
		mgr := config.NewManager()
		mgr.SetConfig("autoIndex.params.build", `{"index_type": "DISKANN"}`)
		p := &paramtable.AutoIndexConfig{
			IndexParams: paramtable.ParamItem{
				Key:       "autoIndex.params.build",
				Formatter: paramtable.GetBuildParamFormatter(paramtable.FloatVectorDefaultMetricType, "autoIndex.params.build"),
			},
		}
		p.IndexParams.Init(mgr)

		assert.NotPanics(t, func() {
			CheckAutoIndexHelper(p.IndexParams.Key, p.IndexParams.GetAsJSONMap(), schemapb.DataType_FloatVector)
		})
		metricType, exist := p.IndexParams.GetAsJSONMap()[common.MetricTypeKey]
		assert.True(t, exist)
		assert.Equal(t, metric.COSINE, metricType)
	})

	t.Run("normal case, bin flat", func(t *testing.T) {
		mgr := config.NewManager()
		mgr.SetConfig("autoIndex.params.build", `{"index_type": "BIN_FLAT"}`)
		p := &paramtable.AutoIndexConfig{
			IndexParams: paramtable.ParamItem{
				Key:       "autoIndex.params.build",
				Formatter: paramtable.GetBuildParamFormatter(paramtable.BinaryVectorDefaultMetricType, "autoIndex.params.binary.build"),
			},
		}
		p.IndexParams.Init(mgr)

		assert.NotPanics(t, func() {
			CheckAutoIndexHelper(p.IndexParams.Key, p.IndexParams.GetAsJSONMap(), schemapb.DataType_BinaryVector)
		})
		metricType, exist := p.IndexParams.GetAsJSONMap()[common.MetricTypeKey]
		assert.True(t, exist)
		assert.Equal(t, metric.HAMMING, metricType)
	})

	t.Run("normal case, bin ivf flat", func(t *testing.T) {
		mgr := config.NewManager()
		mgr.SetConfig("autoIndex.params.build", `{"nlist": 30, "index_type": "BIN_IVF_FLAT"}`)
		p := &paramtable.AutoIndexConfig{
			IndexParams: paramtable.ParamItem{
				Key:       "autoIndex.params.build",
				Formatter: paramtable.GetBuildParamFormatter(paramtable.BinaryVectorDefaultMetricType, "autoIndex.params.binary.build"),
			},
		}
		p.IndexParams.Init(mgr)

		assert.NotPanics(t, func() {
			CheckAutoIndexHelper(p.IndexParams.Key, p.IndexParams.GetAsJSONMap(), schemapb.DataType_BinaryVector)
		})
		metricType, exist := p.IndexParams.GetAsJSONMap()[common.MetricTypeKey]
		assert.True(t, exist)
		assert.Equal(t, metric.HAMMING, metricType)
	})
}
