package util

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestGetAllTopicsFromConfiguration(t *testing.T) {
	paramtable.Init()
	topics := GetAllTopicsFromConfiguration()
	assert.Len(t, topics, 16)
	paramtable.Get().CommonCfg.PreCreatedTopicEnabled.SwapTempValue("true")
	paramtable.Get().CommonCfg.TopicNames.SwapTempValue("topic1,topic2,topic3")
	topics = GetAllTopicsFromConfiguration()
	assert.Len(t, topics, 3)
}
