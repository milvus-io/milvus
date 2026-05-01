/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package chain

import (
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// =============================================================================
// Operator Registry
// =============================================================================

// OperatorFactory is a function that creates an Operator from OperatorRepr.
type OperatorFactory func(repr *OperatorRepr) (Operator, error)

var (
	operatorRegistryMu sync.RWMutex
	operatorRegistry   = make(map[string]OperatorFactory)
)

// RegisterOperator registers an operator factory.
// Returns an error if an operator with the same type is already registered.
func RegisterOperator(opType string, factory OperatorFactory) error {
	if opType == "" {
		return merr.WrapErrParameterInvalidMsg("operator type cannot be empty")
	}
	if factory == nil {
		return merr.WrapErrServiceInternal(fmt.Sprintf("operator factory cannot be nil for %q", opType))
	}

	operatorRegistryMu.Lock()
	defer operatorRegistryMu.Unlock()

	if _, exists := operatorRegistry[opType]; exists {
		return merr.WrapErrServiceInternal(fmt.Sprintf("operator %q already registered", opType))
	}
	operatorRegistry[opType] = factory
	return nil
}

// MustRegisterOperator registers an operator factory and panics on error.
// Use this in init() functions to fail fast on registration errors.
func MustRegisterOperator(opType string, factory OperatorFactory) {
	if err := RegisterOperator(opType, factory); err != nil {
		panic(fmt.Sprintf("failed to register operator: %v", err))
	}
}

// GetOperatorFactory returns the factory for the given operator type.
func GetOperatorFactory(opType string) (OperatorFactory, bool) {
	operatorRegistryMu.RLock()
	defer operatorRegistryMu.RUnlock()
	factory, ok := operatorRegistry[opType]
	return factory, ok
}
