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

package config

type MilvusMQConfig struct {
	Pulsar PulsarConfig
	Kafka  KafkaConfig
}

type MilvusEtcdConfig struct {
	Endpoints   []string
	RootPath    string
	MetaSubPath string

	// the key of the collection meta, default: root-coord/collection
	CollectionKey string
	// default: root-coord/fields
	FiledKey string
}

func defaultMilvusEtcdConfig() MilvusEtcdConfig {
	return MilvusEtcdConfig{
		Endpoints:     []string{"localhost:2379"},
		RootPath:      "by-dev",
		MetaSubPath:   "meta",
		CollectionKey: "root-coord/collection",
		FiledKey:      "root-coord/fields",
	}
}

func NewMilvusEtcdConfig(options ...Option[*MilvusEtcdConfig]) MilvusEtcdConfig {
	c := defaultMilvusEtcdConfig()
	for _, option := range options {
		option.Apply(&c)
	}
	return c
}

func MilvusEtcdEndpointsOption(endpoints []string) Option[*MilvusEtcdConfig] {
	return OptionFunc[*MilvusEtcdConfig](func(object *MilvusEtcdConfig) {
		object.Endpoints = endpoints
	})
}

func MilvusEtcdRootPathOption(rootPath string) Option[*MilvusEtcdConfig] {
	return OptionFunc[*MilvusEtcdConfig](func(object *MilvusEtcdConfig) {
		object.RootPath = rootPath
	})
}

func MilvusEtcdMetaSubPathOption(metaSubPath string) Option[*MilvusEtcdConfig] {
	return OptionFunc[*MilvusEtcdConfig](func(object *MilvusEtcdConfig) {
		object.MetaSubPath = metaSubPath
	})
}
