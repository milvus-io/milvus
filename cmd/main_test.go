package main

import (
	"os"
	"testing"

	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/stretchr/testify/assert"
)

func Test_injectVariablesToEnv(t *testing.T) {
	t.Run("milvusConf is not empty", func(t *testing.T) {
		milvusConf = "test/conf"
		injectVariablesToEnv()
		assert.Equal(t, os.Getenv(metricsinfo.GitCommitEnvKey), GitCommit)
		assert.Equal(t, os.Getenv(metricsinfo.GitBuildTagsEnvKey), BuildTags)
		assert.Equal(t, os.Getenv(metricsinfo.MilvusBuildTimeEnvKey), BuildTime)
		assert.Equal(t, os.Getenv(metricsinfo.MilvusUsedGoVersion), GoVersion)
		assert.Equal(t, os.Getenv(paramtable.MilvusConfEnvKey), milvusConf)
	})
	t.Run("milvusConf is empty ", func(t *testing.T) {
		dummyPath := "path"
		milvusConf = ""
		os.Setenv(paramtable.MilvusConfEnvKey, dummyPath)
		injectVariablesToEnv()
		assert.Equal(t, os.Getenv(paramtable.MilvusConfEnvKey), dummyPath)
	})
}
