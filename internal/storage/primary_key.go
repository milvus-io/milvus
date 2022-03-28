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

package storage

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/milvus-io/milvus/internal/log"
)

type PrimaryKey interface {
	GT(key PrimaryKey) bool
	GE(key PrimaryKey) bool
	LT(key PrimaryKey) bool
	LE(key PrimaryKey) bool
	EQ(key PrimaryKey) bool
	MarshalJSON() ([]byte, error)
	UnmarshalJSON(data []byte) error
	SetValue(interface{}) error
}

type Int64PrimaryKey struct {
	Value int64 `json:"pkValue"`
}

func (ip *Int64PrimaryKey) GT(key PrimaryKey) bool {
	pk, ok := key.(*Int64PrimaryKey)
	if !ok {
		log.Warn("type of compared pk is not int64")
		return false
	}
	if ip.Value > pk.Value {
		return true
	}

	return false
}

func (ip *Int64PrimaryKey) GE(key PrimaryKey) bool {
	pk, ok := key.(*Int64PrimaryKey)
	if !ok {
		log.Warn("type of compared pk is not int64")
		return false
	}
	if ip.Value >= pk.Value {
		return true
	}

	return false
}

func (ip *Int64PrimaryKey) LT(key PrimaryKey) bool {
	pk, ok := key.(*Int64PrimaryKey)
	if !ok {
		log.Warn("type of compared pk is not int64")
		return false
	}

	if ip.Value < pk.Value {
		return true
	}

	return false
}

func (ip *Int64PrimaryKey) LE(key PrimaryKey) bool {
	pk, ok := key.(*Int64PrimaryKey)
	if !ok {
		log.Warn("type of compared pk is not int64")
		return false
	}

	if ip.Value <= pk.Value {
		return true
	}

	return false
}

func (ip *Int64PrimaryKey) EQ(key PrimaryKey) bool {
	pk, ok := key.(*Int64PrimaryKey)
	if !ok {
		log.Warn("type of compared pk is not int64")
		return false
	}

	if ip.Value == pk.Value {
		return true
	}

	return false
}

func (ip *Int64PrimaryKey) MarshalJSON() ([]byte, error) {
	ret, err := json.Marshal(ip.Value)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (ip *Int64PrimaryKey) UnmarshalJSON(data []byte) error {
	err := json.Unmarshal(data, &ip.Value)
	if err != nil {
		return err
	}

	return nil
}

func (ip *Int64PrimaryKey) SetValue(data interface{}) error {
	value, ok := data.(int64)
	if !ok {
		return fmt.Errorf("wrong type value when setValue for Int64PrimaryKey")
	}

	ip.Value = value
	return nil
}

type StringPrimaryKey struct {
	Value string
}

func (sp *StringPrimaryKey) GT(key PrimaryKey) bool {
	pk, ok := key.(*StringPrimaryKey)
	if !ok {
		log.Warn("type of compared pk is not string")
		return false
	}
	if strings.Compare(sp.Value, pk.Value) > 0 {
		return true
	}

	return false
}

func (sp *StringPrimaryKey) GE(key PrimaryKey) bool {
	pk, ok := key.(*StringPrimaryKey)
	if !ok {
		log.Warn("type of compared pk is not string")
		return false
	}
	if strings.Compare(sp.Value, pk.Value) >= 0 {
		return true
	}

	return false
}

func (sp *StringPrimaryKey) LT(key PrimaryKey) bool {
	pk, ok := key.(*StringPrimaryKey)
	if !ok {
		log.Warn("type of compared pk is not string")
		return false
	}
	if strings.Compare(sp.Value, pk.Value) < 0 {
		return true
	}

	return false
}

func (sp *StringPrimaryKey) LE(key PrimaryKey) bool {
	pk, ok := key.(*StringPrimaryKey)
	if !ok {
		log.Warn("type of compared pk is not string")
		return false
	}
	if strings.Compare(sp.Value, pk.Value) <= 0 {
		return true
	}

	return false
}

func (sp *StringPrimaryKey) EQ(key PrimaryKey) bool {
	pk, ok := key.(*StringPrimaryKey)
	if !ok {
		log.Warn("type of compared pk is not string")
		return false
	}
	if strings.Compare(sp.Value, pk.Value) == 0 {
		return true
	}

	return false
}

func (sp *StringPrimaryKey) MarshalJSON() ([]byte, error) {
	ret, err := json.Marshal(sp.Value)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (sp *StringPrimaryKey) UnmarshalJSON(data []byte) error {
	err := json.Unmarshal(data, &sp.Value)
	if err != nil {
		return err
	}

	return nil
}

func (sp *StringPrimaryKey) SetValue(data interface{}) error {
	value, ok := data.(string)
	if !ok {
		return fmt.Errorf("wrong type value when setValue for StringPrimaryKey")
	}

	sp.Value = value
	return nil
}
