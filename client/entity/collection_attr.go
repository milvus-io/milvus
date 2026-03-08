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

package entity

import (
	"strconv"

	"github.com/cockroachdb/errors"
)

const (
	// cakTTL const for collection attribute key TTL in seconds.
	cakTTL = `collection.ttl.seconds`
	// cakAutoCompaction const for collection attribute key autom compaction enabled.
	cakAutoCompaction = `collection.autocompaction.enabled`
)

// CollectionAttribute is the interface for altering collection attributes.
type CollectionAttribute interface {
	KeyValue() (string, string)
	Valid() error
}

type collAttrBase struct {
	key   string
	value string
}

// KeyValue implements CollectionAttribute.
func (ca collAttrBase) KeyValue() (string, string) {
	return ca.key, ca.value
}

type ttlCollAttr struct {
	collAttrBase
}

// Valid implements CollectionAttribute.
// checks ttl seconds is valid positive integer.
func (ca collAttrBase) Valid() error {
	val, err := strconv.ParseInt(ca.value, 10, 64)
	if err != nil {
		return errors.Wrap(err, "ttl is not a valid positive integer")
	}

	if val < 0 {
		return errors.New("ttl needs to be a positive integer")
	}

	return nil
}

// CollectionTTL returns collection attribute to set collection ttl in seconds.
func CollectionTTL(ttl int64) ttlCollAttr {
	ca := ttlCollAttr{}
	ca.key = cakTTL
	ca.value = strconv.FormatInt(ttl, 10)
	return ca
}

type autoCompactionCollAttr struct {
	collAttrBase
}

// Valid implements CollectionAttribute.
// checks collection auto compaction is valid bool.
func (ca autoCompactionCollAttr) Valid() error {
	_, err := strconv.ParseBool(ca.value)
	if err != nil {
		return errors.Wrap(err, "auto compaction setting is not valid boolean")
	}

	return nil
}

// CollectionAutoCompactionEnabled returns collection attribute to set collection auto compaction enabled.
func CollectionAutoCompactionEnabled(enabled bool) autoCompactionCollAttr {
	ca := autoCompactionCollAttr{}
	ca.key = cakAutoCompaction
	ca.value = strconv.FormatBool(enabled)
	return ca
}
