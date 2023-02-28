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

import "strconv"

var (
	Kafka  *KafkaConfig
	Pulsar *PulsarConfig
)

type KafkaConfig struct {
	Address string
}

func NewKafkaConfig(options ...Option[*KafkaConfig]) KafkaConfig {
	var k KafkaConfig
	for _, option := range options {
		option.Apply(&k)
	}
	return k
}

func KafkaAddressOption(address string) Option[*KafkaConfig] {
	return OptionFunc[*KafkaConfig](func(object *KafkaConfig) {
		object.Address = address
	})
}

type PulsarConfig struct {
	Address        string
	Port           string
	WebAddress     string
	WebPort        int
	MaxMessageSize string

	// support tenant
	Tenant    string
	Namespace string
}

func NewPulsarConfig(options ...Option[*PulsarConfig]) PulsarConfig {
	p := PulsarConfig{}
	for _, option := range options {
		option.Apply(&p)
	}
	return p
}

func PulsarAddressOption(address string) Option[*PulsarConfig] {
	return OptionFunc[*PulsarConfig](func(object *PulsarConfig) {
		object.Address = address
	})
}

func PulsarWebAddressOption(address string, port int) Option[*PulsarConfig] {
	return OptionFunc[*PulsarConfig](func(object *PulsarConfig) {
		object.WebAddress = address
		object.WebPort = port
	})
}

// PulsarMaxMessageSizeOption size unit: Bytes
func PulsarMaxMessageSizeOption(size int64) Option[*PulsarConfig] {
	return OptionFunc[*PulsarConfig](func(object *PulsarConfig) {
		object.MaxMessageSize = strconv.FormatInt(size, 10)
	})
}

func PulsarTenantOption(tenant string, namespace string) Option[*PulsarConfig] {
	return OptionFunc[*PulsarConfig](func(object *PulsarConfig) {
		object.Tenant = tenant
		object.Namespace = namespace
	})
}
