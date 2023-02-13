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
	// TODO fubang remove
	Kafka  *KafkaConfig
	Pulsar *PulsarConfig
)

type KafkaConfig struct {
	Address             string
	SaslUsername        string
	SaslPassword        string
	SaslMechanisms      string
	SecurityProtocol    string
	ConsumerExtraConfig map[string]string
	ProducerExtraConfig map[string]string
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

func KafkaSaslOption(username string, password string, mechanisms string) Option[*KafkaConfig] {
	return OptionFunc[*KafkaConfig](func(object *KafkaConfig) {
		object.SaslUsername = username
		object.SaslPassword = password
		object.SaslMechanisms = mechanisms
	})
}

func KafkaSecurityProtocolOption(protocol string) Option[*KafkaConfig] {
	return OptionFunc[*KafkaConfig](func(object *KafkaConfig) {
		object.SecurityProtocol = protocol
	})
}

func KafkaConsumerExtraConfigOption(extraConfig map[string]string) Option[*KafkaConfig] {
	return OptionFunc[*KafkaConfig](func(object *KafkaConfig) {
		object.ConsumerExtraConfig = extraConfig
	})
}

func KafkaProducerExtraConfigOption(extraConfig map[string]string) Option[*KafkaConfig] {
	return OptionFunc[*KafkaConfig](func(object *KafkaConfig) {
		object.ProducerExtraConfig = extraConfig
	})
}

type PulsarConfig struct {
	Address        string
	Port           string
	WebAddress     string
	WebPort        string
	MaxMessageSize string

	// support auth
	AuthPlugin string
	AuthParams string

	// support tenant
	Tenant    string
	Namespace string
}

func NewPulsarConfig(options ...Option[*PulsarConfig]) PulsarConfig {
	p := PulsarConfig{
		AuthParams: "{}",
	}
	for _, option := range options {
		option.Apply(&p)
	}
	return p
}

func PulsarAddressOption(address string, port string) Option[*PulsarConfig] {
	return OptionFunc[*PulsarConfig](func(object *PulsarConfig) {
		object.Address = address
		object.Port = port
	})
}

func PulsarWebAddressOption(address string, port string) Option[*PulsarConfig] {
	return OptionFunc[*PulsarConfig](func(object *PulsarConfig) {
		object.WebAddress = address
		object.Port = port
	})
}

// PulsarMaxMessageSizeOption size unit: Bytes
func PulsarMaxMessageSizeOption(size int64) Option[*PulsarConfig] {
	return OptionFunc[*PulsarConfig](func(object *PulsarConfig) {
		object.MaxMessageSize = strconv.FormatInt(size, 10)
	})
}

func PulsarAuthOption(plugin string, param string) Option[*PulsarConfig] {
	return OptionFunc[*PulsarConfig](func(object *PulsarConfig) {
		object.AuthPlugin = plugin
		// TODO fubang param should be json format
		object.AuthParams = param
	})
}

func PulsarTenantOption(tenant string, namespace string) Option[*PulsarConfig] {
	return OptionFunc[*PulsarConfig](func(object *PulsarConfig) {
		object.Tenant = tenant
		object.Namespace = namespace
	})
}

func PulsarEnable() bool {
	return Pulsar.Address != ""
}

func KafkaEnable() bool {
	return Kafka.Address != ""
}
