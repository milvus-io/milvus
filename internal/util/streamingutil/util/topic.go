package util

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// GetAllTopicsFromConfiguration gets all topics from configuration.
// It's a utility function to fetch all topics from configuration.
func GetAllTopicsFromConfiguration() typeutil.Set[string] {
	var channels typeutil.Set[string]
	if paramtable.Get().CommonCfg.PreCreatedTopicEnabled.GetAsBool() {
		channels = typeutil.NewSet[string](paramtable.Get().CommonCfg.TopicNames.GetAsStrings()...)
	} else {
		channels = genChannelNames(paramtable.Get().CommonCfg.RootCoordDml.GetValue(), paramtable.Get().RootCoordCfg.DmlChannelNum.GetAsInt())
	}
	return channels
}

// genChannelNames generates channel names with prefix and number.
func genChannelNames(prefix string, num int) typeutil.Set[string] {
	results := typeutil.NewSet[string]()
	for idx := 0; idx < num; idx++ {
		result := fmt.Sprintf("%s_%d", prefix, idx)
		results.Insert(result)
	}
	return results
}
