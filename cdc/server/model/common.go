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

package model

//go:generate easytags $GOFILE json,mapstructure

type MilvusConnectParam struct {
	Host            string `json:"host" mapstructure:"host"`
	Port            int    `json:"port" mapstructure:"port"`
	Username        string `json:"username" mapstructure:"username"`
	Password        string `json:"password" mapstructure:"password"`
	EnableTls       bool   `json:"enable_tls" mapstructure:"enable_tls"`
	IgnorePartition bool   `json:"ignore_partition" mapstructure:"ignore_partition"`
	// ConnectTimeout unit: s
	ConnectTimeout int `json:"connect_timeout" mapstructure:"connect_timeout"`
}

type CollectionInfo struct {
	Name string `json:"name" mapstructure:"name"`
}

type BufferConfig struct {
	Period int `json:"period" mapstructure:"period"`
	Size   int `json:"size" mapstructure:"size"`
}
