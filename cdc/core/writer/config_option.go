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

package writer

import (
	"time"

	"github.com/milvus-io/milvus/cdc/core/config"
)

func AddressOption(address string) config.Option[*MilvusDataHandler] {
	return config.OptionFunc[*MilvusDataHandler](func(object *MilvusDataHandler) {
		object.address = address
	})
}

func UserOption(username string, password string) config.Option[*MilvusDataHandler] {
	return config.OptionFunc[*MilvusDataHandler](func(object *MilvusDataHandler) {
		object.username = username
		object.password = password
	})
}

func TlsOption(enable bool) config.Option[*MilvusDataHandler] {
	return config.OptionFunc[*MilvusDataHandler](func(object *MilvusDataHandler) {
		object.enableTls = enable
	})
}

func ConnectTimeoutOption(timeout int) config.Option[*MilvusDataHandler] {
	return config.OptionFunc[*MilvusDataHandler](func(object *MilvusDataHandler) {
		if timeout > 0 {
			object.connectTimeout = timeout
		}
	})
}

func IgnorePartitionOption(ignore bool) config.Option[*MilvusDataHandler] {
	return config.OptionFunc[*MilvusDataHandler](func(object *MilvusDataHandler) {
		object.ignorePartition = ignore
	})
}

func HandlerOption(handler CDCDataHandler) config.Option[*CdcWriterTemplate] {
	return config.OptionFunc[*CdcWriterTemplate](func(object *CdcWriterTemplate) {
		object.handler = handler
	})
}

func NoBufferOption() config.Option[*CdcWriterTemplate] {
	return config.OptionFunc[*CdcWriterTemplate](func(object *CdcWriterTemplate) {
		object.bufferConfig = NoBufferConfig
	})
}

func BufferOption(period time.Duration, size int64, positionFunc NotifyCollectionPositionChangeFunc) config.Option[*CdcWriterTemplate] {
	return config.OptionFunc[*CdcWriterTemplate](func(object *CdcWriterTemplate) {
		if period > 0 {
			object.bufferConfig.Period = period
		}
		if size > 0 {
			object.bufferConfig.Size = size
		}
		object.bufferUpdatePositionFunc = positionFunc
	})
}

func ErrorProtectOption(per int32, unit time.Duration) config.Option[*CdcWriterTemplate] {
	return config.OptionFunc[*CdcWriterTemplate](func(object *CdcWriterTemplate) {
		object.errProtect = NewErrorProtect(per, unit)
	})
}
