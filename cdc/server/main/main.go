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

package main

import (
	"github.com/milvus-io/milvus/cdc/core/config"
	"github.com/milvus-io/milvus/cdc/server"
)

func main() {
	s := &server.CdcServer{}
	s.Run(&server.CdcServerConfig{
		Address: "localhost:8444",
		EtcdConfig: struct {
			Endpoints []string
			RootPath  string
		}{Endpoints: []string{"localhost:2379"}, RootPath: "cdc"},
		SourceConfig: struct {
			EtcdAddress     []string
			EtcdRootPath    string
			EtcdMetaSubPath string
			ReadChanLen     int
			Pulsar          config.PulsarConfig
			Kafka           config.KafkaConfig
		}{
			EtcdAddress:     []string{"localhost:2379"},
			EtcdRootPath:    "by-dev",
			EtcdMetaSubPath: "meta",
			ReadChanLen:     10,
			Pulsar: config.NewPulsarConfig(
				config.PulsarAddressOption("pulsar://localhost:6650", "6650"),
				config.PulsarWebAddressOption("", "80"),
				config.PulsarMaxMessageSizeOption(5242880),
				config.PulsarTenantOption("public", "default"),
			),
		},
		MaxNameLength: 256,
	})
}
