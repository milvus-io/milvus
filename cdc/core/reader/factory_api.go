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
	"strconv"

	"github.com/milvus-io/milvus/cdc/core/config"
	"github.com/milvus-io/milvus/cdc/core/util"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

//go:generate mockery --name=FactoryCreator --filename=factory_creator_mock.go --output=../mocks --with-expecter
type FactoryCreator interface {
	util.CDCMark
	NewPmsFactory(cfg *config.PulsarConfig) msgstream.Factory
	NewKmsFactory(cfg *config.KafkaConfig) msgstream.Factory
}

type DefaultFactoryCreator struct {
	util.CDCMark
}

func NewDefaultFactoryCreator() FactoryCreator {
	return &DefaultFactoryCreator{}
}

func (d *DefaultFactoryCreator) NewPmsFactory(cfg *config.PulsarConfig) msgstream.Factory {
	return msgstream.NewPmsFactory(&paramtable.PulsarConfig{
		Address:        config.NewParamItem(cfg.Address),
		WebAddress:     config.NewParamItem(cfg.WebAddress),
		WebPort:        config.NewParamItem(strconv.Itoa(cfg.WebPort)),
		MaxMessageSize: config.NewParamItem(cfg.MaxMessageSize),
		AuthPlugin:     config.NewParamItem(""),
		AuthParams:     config.NewParamItem("{}"),
		Tenant:         config.NewParamItem(cfg.Tenant),
		Namespace:      config.NewParamItem(cfg.Namespace),
	})
}

func (d *DefaultFactoryCreator) NewKmsFactory(cfg *config.KafkaConfig) msgstream.Factory {
	return msgstream.NewKmsFactory(&paramtable.KafkaConfig{
		Address:             config.NewParamItem(cfg.Address),
		SaslUsername:        config.NewParamItem(""),
		SaslPassword:        config.NewParamItem(""),
		SaslMechanisms:      config.NewParamItem(""),
		SecurityProtocol:    config.NewParamItem(""),
		ConsumerExtraConfig: config.NewParamGroup(),
		ProducerExtraConfig: config.NewParamGroup(),
	})
}
