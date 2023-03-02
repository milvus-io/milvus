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
	"github.com/milvus-io/milvus/cdc/core/config"
	"github.com/milvus-io/milvus/cdc/core/mq"
	"github.com/milvus-io/milvus/cdc/core/mq/api"
	"github.com/milvus-io/milvus/cdc/core/util"
)

//go:generate mockery --name=FactoryCreator --filename=factory_creator_mock.go --output=../mocks --with-expecter
type FactoryCreator interface {
	util.CDCMark
	NewPmsFactory(cfg *config.PulsarConfig) api.Factory
	NewKmsFactory(cfg *config.KafkaConfig) api.Factory
}

type DefaultFactoryCreator struct {
	util.CDCMark
}

func NewDefaultFactoryCreator() FactoryCreator {
	return &DefaultFactoryCreator{}
}

func (d *DefaultFactoryCreator) NewPmsFactory(cfg *config.PulsarConfig) api.Factory {
	return mq.NewPmsFactory(cfg)
}

func (d *DefaultFactoryCreator) NewKmsFactory(cfg *config.KafkaConfig) api.Factory {
	return mq.NewKmsFactory(cfg)
}
