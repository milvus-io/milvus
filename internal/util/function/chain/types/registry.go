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

package types

import (
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// =============================================================================
// Function Registry
// =============================================================================

// FunctionRegistry stores immutable function definitions by name.
type FunctionRegistry struct {
	mu          sync.RWMutex
	definitions map[string]FunctionDefinition
}

// NewFunctionRegistry creates a new FunctionRegistry.
func NewFunctionRegistry() *FunctionRegistry {
	return &FunctionRegistry{
		definitions: make(map[string]FunctionDefinition),
	}
}

// Register registers a function definition with the given name.
// Returns an error if a function with the same name is already registered.
func (r *FunctionRegistry) Register(name string, definition FunctionDefinition) error {
	if name == "" {
		return merr.WrapErrParameterMissingMsg("function name cannot be empty")
	}
	if definition.Factory == nil {
		return merr.WrapErrServiceInternalMsg("function factory cannot be nil for %q", name)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.definitions[name]; exists {
		return merr.WrapErrServiceInternalMsg("function %q already registered", name)
	}
	r.definitions[name] = definition
	return nil
}

// MustRegister registers a function definition and panics on error.
// Use this in init() functions to fail fast on registration errors.
func (r *FunctionRegistry) MustRegister(name string, definition FunctionDefinition) {
	if err := r.Register(name, definition); err != nil {
		panic(fmt.Sprintf("failed to register function: %v", err))
	}
}

// Get returns a copy of the function definition for the given name.
func (r *FunctionRegistry) Get(name string) (FunctionDefinition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	definition, ok := r.definitions[name]
	return definition, ok
}

// Create creates a FunctionExpr using the factory registered with the given name.
func (r *FunctionRegistry) Create(ctx FunctionBuildContext, cfg FunctionConfig) (FunctionExpr, error) {
	definition, ok := r.Get(cfg.Name)
	if !ok {
		return nil, merr.WrapErrParameterInvalidMsg("unknown function: %s", cfg.Name)
	}
	return definition.Factory(ctx, cfg)
}

// Has returns true if a function with the given name is registered.
func (r *FunctionRegistry) Has(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.definitions[name]
	return ok
}

// Names returns all registered function names.
func (r *FunctionRegistry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.definitions))
	for name := range r.definitions {
		names = append(names, name)
	}
	return names
}

// =============================================================================
// Global Registry
// =============================================================================

// globalRegistry is the global function registry.
var globalRegistry = NewFunctionRegistry()

// RegisterFunction registers a function definition in the global registry.
// Returns an error if a function with the same name is already registered.
func RegisterFunction(name string, definition FunctionDefinition) error {
	return globalRegistry.Register(name, definition)
}

// MustRegisterFunction registers a function definition and panics on error.
// Use this in init() functions to fail fast on registration errors.
func MustRegisterFunction(name string, definition FunctionDefinition) {
	globalRegistry.MustRegister(name, definition)
}

// GetFunctionFactory returns the function factory from the global registry.
func GetFunctionFactory(name string) (FunctionFactory, bool) {
	definition, ok := globalRegistry.Get(name)
	return definition.Factory, ok
}

// CreateFunction creates a FunctionExpr using the global registry.
func CreateFunction(ctx FunctionBuildContext, cfg FunctionConfig) (FunctionExpr, error) {
	return globalRegistry.Create(ctx, cfg)
}

// HasFunction returns true if a function with the given name is registered in the global registry.
func HasFunction(name string) bool {
	return globalRegistry.Has(name)
}

// FunctionNames returns all registered function names from the global registry.
func FunctionNames() []string {
	return globalRegistry.Names()
}

// GetFunctionDefinition returns the registered function capabilities.
func GetFunctionDefinition(name string) (FunctionDefinition, bool) {
	return globalRegistry.Get(name)
}
