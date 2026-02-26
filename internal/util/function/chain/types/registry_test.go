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
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dummyFunc is a minimal FunctionExpr implementation for testing.
type dummyFunc struct {
	name string
}

func (d *dummyFunc) Name() string { return d.name }
func (d *dummyFunc) OutputDataTypes() []arrow.DataType {
	return []arrow.DataType{arrow.PrimitiveTypes.Float64}
}

func (d *dummyFunc) Execute(_ *FuncContext, _ []*arrow.Chunked) ([]*arrow.Chunked, error) {
	return nil, nil
}
func (d *dummyFunc) IsRunnable(_ string) bool { return true }

// dummyFactory returns a FunctionFactory that produces a dummyFunc with the given name.
func dummyFactory(name string) FunctionFactory {
	return func(params map[string]interface{}) (FunctionExpr, error) {
		return &dummyFunc{name: name}, nil
	}
}

// =============================================================================
// Instance method tests
// =============================================================================

func TestFunctionRegistry_Register(t *testing.T) {
	r := NewFunctionRegistry()

	err := r.Register("my_func", dummyFactory("my_func"))
	assert.NoError(t, err)
	assert.True(t, r.Has("my_func"))
}

func TestFunctionRegistry_Register_EmptyName(t *testing.T) {
	r := NewFunctionRegistry()

	err := r.Register("", dummyFactory("x"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "function name cannot be empty")
}

func TestFunctionRegistry_Register_NilFactory(t *testing.T) {
	r := NewFunctionRegistry()

	err := r.Register("nil_factory", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "function factory cannot be nil")
}

func TestFunctionRegistry_Register_Duplicate(t *testing.T) {
	r := NewFunctionRegistry()

	err := r.Register("dup", dummyFactory("dup"))
	require.NoError(t, err)

	err = r.Register("dup", dummyFactory("dup"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already registered")
}

func TestFunctionRegistry_MustRegister(t *testing.T) {
	r := NewFunctionRegistry()

	// Should not panic on valid registration
	assert.NotPanics(t, func() {
		r.MustRegister("must_func", dummyFactory("must_func"))
	})
	assert.True(t, r.Has("must_func"))
}

func TestFunctionRegistry_MustRegister_PanicOnDuplicate(t *testing.T) {
	r := NewFunctionRegistry()

	r.MustRegister("panic_dup", dummyFactory("panic_dup"))

	assert.Panics(t, func() {
		r.MustRegister("panic_dup", dummyFactory("panic_dup"))
	})
}

func TestFunctionRegistry_MustRegister_PanicOnEmptyName(t *testing.T) {
	r := NewFunctionRegistry()

	assert.Panics(t, func() {
		r.MustRegister("", dummyFactory("x"))
	})
}

func TestFunctionRegistry_MustRegister_PanicOnNilFactory(t *testing.T) {
	r := NewFunctionRegistry()

	assert.Panics(t, func() {
		r.MustRegister("nil_panic", nil)
	})
}

func TestFunctionRegistry_Get(t *testing.T) {
	r := NewFunctionRegistry()
	r.MustRegister("get_func", dummyFactory("get_func"))

	factory, ok := r.Get("get_func")
	assert.True(t, ok)
	assert.NotNil(t, factory)

	// Verify the factory produces the expected function
	fn, err := factory(nil)
	require.NoError(t, err)
	assert.Equal(t, "get_func", fn.Name())
}

func TestFunctionRegistry_Get_NotFound(t *testing.T) {
	r := NewFunctionRegistry()

	factory, ok := r.Get("nonexistent")
	assert.False(t, ok)
	assert.Nil(t, factory)
}

func TestFunctionRegistry_Create(t *testing.T) {
	r := NewFunctionRegistry()
	r.MustRegister("create_func", dummyFactory("create_func"))

	fn, err := r.Create("create_func", nil)
	require.NoError(t, err)
	assert.Equal(t, "create_func", fn.Name())
}

func TestFunctionRegistry_Create_WithParams(t *testing.T) {
	r := NewFunctionRegistry()

	// Register a factory that reads params
	r.MustRegister("param_func", func(params map[string]interface{}) (FunctionExpr, error) {
		name, _ := params["name"].(string)
		return &dummyFunc{name: name}, nil
	})

	fn, err := r.Create("param_func", map[string]interface{}{"name": "custom"})
	require.NoError(t, err)
	assert.Equal(t, "custom", fn.Name())
}

func TestFunctionRegistry_Create_NotFound(t *testing.T) {
	r := NewFunctionRegistry()

	fn, err := r.Create("missing", nil)
	assert.Error(t, err)
	assert.Nil(t, fn)
	assert.Contains(t, err.Error(), "unknown function")
}

func TestFunctionRegistry_Has(t *testing.T) {
	r := NewFunctionRegistry()

	assert.False(t, r.Has("has_func"))

	r.MustRegister("has_func", dummyFactory("has_func"))
	assert.True(t, r.Has("has_func"))
}

func TestFunctionRegistry_Names(t *testing.T) {
	r := NewFunctionRegistry()

	// Empty registry should return empty slice
	names := r.Names()
	assert.Empty(t, names)

	r.MustRegister("alpha", dummyFactory("alpha"))
	r.MustRegister("beta", dummyFactory("beta"))
	r.MustRegister("gamma", dummyFactory("gamma"))

	names = r.Names()
	assert.Len(t, names, 3)
	assert.ElementsMatch(t, []string{"alpha", "beta", "gamma"}, names)
}

// =============================================================================
// Global registry tests (use unique names to avoid init() conflicts)
// =============================================================================

func TestGlobalRegistry_RegisterFunction(t *testing.T) {
	name := "test_global_register_xyz"

	err := RegisterFunction(name, dummyFactory(name))
	assert.NoError(t, err)
	assert.True(t, HasFunction(name))
}

func TestGlobalRegistry_RegisterFunction_Duplicate(t *testing.T) {
	name := "test_global_dup_xyz"

	err := RegisterFunction(name, dummyFactory(name))
	require.NoError(t, err)

	err = RegisterFunction(name, dummyFactory(name))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already registered")
}

func TestGlobalRegistry_MustRegisterFunction(t *testing.T) {
	name := "test_global_must_xyz"

	assert.NotPanics(t, func() {
		MustRegisterFunction(name, dummyFactory(name))
	})
	assert.True(t, HasFunction(name))
}

func TestGlobalRegistry_MustRegisterFunction_PanicOnDuplicate(t *testing.T) {
	name := "test_global_must_panic_xyz"

	MustRegisterFunction(name, dummyFactory(name))

	assert.Panics(t, func() {
		MustRegisterFunction(name, dummyFactory(name))
	})
}

func TestGlobalRegistry_GetFunctionFactory(t *testing.T) {
	name := "test_global_get_xyz"

	MustRegisterFunction(name, dummyFactory(name))

	factory, ok := GetFunctionFactory(name)
	assert.True(t, ok)
	assert.NotNil(t, factory)

	fn, err := factory(nil)
	require.NoError(t, err)
	assert.Equal(t, name, fn.Name())
}

func TestGlobalRegistry_GetFunctionFactory_NotFound(t *testing.T) {
	factory, ok := GetFunctionFactory("test_global_nonexistent_xyz")
	assert.False(t, ok)
	assert.Nil(t, factory)
}

func TestGlobalRegistry_CreateFunction(t *testing.T) {
	name := "test_global_create_xyz"

	MustRegisterFunction(name, dummyFactory(name))

	fn, err := CreateFunction(name, nil)
	require.NoError(t, err)
	assert.Equal(t, name, fn.Name())
}

func TestGlobalRegistry_CreateFunction_NotFound(t *testing.T) {
	fn, err := CreateFunction("test_global_create_missing_xyz", nil)
	assert.Error(t, err)
	assert.Nil(t, fn)
	assert.Contains(t, err.Error(), "unknown function")
}

func TestGlobalRegistry_HasFunction(t *testing.T) {
	name := "test_global_has_xyz"

	assert.False(t, HasFunction(name))

	MustRegisterFunction(name, dummyFactory(name))
	assert.True(t, HasFunction(name))
}

func TestGlobalRegistry_FunctionNames(t *testing.T) {
	// Register a unique function so we can verify it appears in the list
	name := "test_global_names_xyz"

	MustRegisterFunction(name, dummyFactory(name))

	names := FunctionNames()
	assert.Contains(t, names, name)
}
