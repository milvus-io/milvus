// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package reader

import (
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/cdc/core/config"
)

func CollectionInfoOption(collectionName string, positions map[string]*commonpb.KeyDataPair) config.Option[*MilvusCollectionReader] {
	return config.OptionFunc[*MilvusCollectionReader](func(object *MilvusCollectionReader) {
		object.collections = append(object.collections, CollectionInfo{
			collectionName: collectionName,
			positions:      positions,
		})
	})
}

func KafKaOption(options ...config.Option[*config.KafkaConfig]) config.Option[*MilvusCollectionReader] {
	return config.OptionFunc[*MilvusCollectionReader](func(object *MilvusCollectionReader) {
		object.mqConfig = config.MilvusMQConfig{Kafka: config.NewKafkaConfig(options...)}
	})
}

func PulsarOption(options ...config.Option[*config.PulsarConfig]) config.Option[*MilvusCollectionReader] {
	return config.OptionFunc[*MilvusCollectionReader](func(object *MilvusCollectionReader) {
		object.mqConfig = config.MilvusMQConfig{Pulsar: config.NewPulsarConfig(options...)}
	})
}

func MqOption(p config.PulsarConfig, k config.KafkaConfig) config.Option[*MilvusCollectionReader] {
	return config.OptionFunc[*MilvusCollectionReader](func(object *MilvusCollectionReader) {
		object.mqConfig = config.MilvusMQConfig{Pulsar: p, Kafka: k}
	})
}

func EtcdOption(c config.MilvusEtcdConfig) config.Option[*MilvusCollectionReader] {
	return config.OptionFunc[*MilvusCollectionReader](func(object *MilvusCollectionReader) {
		object.etcdConfig = c
	})
}

// MonitorOption the implement object of Monitor should include DefaultMonitor for the better compatibility
func MonitorOption(m Monitor) config.Option[*MilvusCollectionReader] {
	return config.OptionFunc[*MilvusCollectionReader](func(object *MilvusCollectionReader) {
		object.monitor = m
	})
}

func ChanLenOption(l int) config.Option[*MilvusCollectionReader] {
	return config.OptionFunc[*MilvusCollectionReader](func(object *MilvusCollectionReader) {
		if l > 0 {
			object.dataChanLen = l
		}
	})
}
