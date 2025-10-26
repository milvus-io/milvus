package rerank

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"

	"github.com/bytecodealliance/wasmtime-go"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

const (
	WasmName      = "wasm"
	wasmCodeKey   = "wasm_code"   // base64-encoded WASM directly
	entryPointKey = "entry_point" // WASM function to call
)

// WASM ABI Contract:
// All WASM rerank functions MUST return f32 to avoid runtime type checks
//
// Memory Optimization Strategy:
// - Preallocated argument buffers (no per-call allocations)
// - Object pooling for result maps (zero allocations per call)
// - Reused slices to minimize GC pressure
// - Fast path inlining for simple cases
// - Minimal interface{} boxing (unavoidable due to wasmtime-go API)
// - Cached WASM function pointer (no string lookup per rerank call)
type WasmFunction[T PKType] struct {
	RerankBase

	instance   *wasmtime.Instance
	store      *wasmtime.Store
	entryPoint string

	// Cached WASM function pointer (no lookup per call)
	rerankFunc *wasmtime.Func

	// Preallocated buffers to reduce GC pressure
	argBuffer   []interface{} // Reusable argument buffer for multi-field calls (fallback)
	maxArgCount int           // Maximum arguments needed

	// Precomputed field arrays (cast once per rerank call)
	fieldArrays []interface{} // Cached typed field arrays

	// Object pools for zero-allocation map reuse
	scoreMapPool *sync.Pool // Pool of map[T]float32
	locMapPool   *sync.Pool // Pool of map[T]IDLoc
}

func newWasmFunction(collSchema *schemapb.CollectionSchema, funcSchema *schemapb.FunctionSchema) (Reranker, error) {
	base, err := newRerankBase(collSchema, funcSchema, WasmName, true)
	if err != nil {
		return nil, err
	}

	var wasmBytes []byte
	var entryPoint string

	for _, param := range funcSchema.Params {
		switch strings.ToLower(param.Key) {
		case wasmCodeKey:
			wasmBytes, err = base64.StdEncoding.DecodeString(param.Value)
			if err != nil {
				return nil, fmt.Errorf("failed to decode WASM bytecode: %w", err)
			}
		case entryPointKey:
			entryPoint = param.Value
		}
	}

	if len(wasmBytes) == 0 {
		return nil, fmt.Errorf("WASM bytecode not provided in params")
	}

	if entryPoint == "" {
		entryPoint = "rerank" // default
	}

	// Compile/instantiate WASM module
	store, instance, err := compileWasmModule(wasmBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to compile WASM module: %w", err)
	}

	// Get and cache the entry point function
	entryFunc := instance.GetFunc(store, entryPoint)
	if entryFunc == nil {
		return nil, fmt.Errorf("entry point '%s' not found in WASM module", entryPoint)
	}

	// Calculate max argument count: score + rank + max input fields
	maxArgs := 2 + len(base.GetInputFieldNames()) // score, rank, + field values
	if maxArgs < 8 {                              // Minimum reasonable buffer size
		maxArgs = 8
	}

	// Preallocate field arrays buffer
	maxFields := len(base.GetInputFieldNames())
	if maxFields == 0 {
		maxFields = 1 // At least one slot
	}

	if base.pkType == schemapb.DataType_Int64 {
		return &WasmFunction[int64]{
			RerankBase:  *base,
			instance:    instance,
			store:       store,
			entryPoint:  entryPoint,
			rerankFunc:  entryFunc,                    // Cached function pointer
			argBuffer:   make([]interface{}, maxArgs), // Preallocated argument buffer
			maxArgCount: maxArgs,
			fieldArrays: make([]interface{}, maxFields), // Preallocated field arrays
			scoreMapPool: &sync.Pool{
				New: func() interface{} { return make(map[int64]float32) },
			},
			locMapPool: &sync.Pool{
				New: func() interface{} { return make(map[int64]IDLoc) },
			},
		}, nil
	} else {
		return &WasmFunction[string]{
			RerankBase:  *base,
			instance:    instance,
			store:       store,
			entryPoint:  entryPoint,
			rerankFunc:  entryFunc,                    // Cached function pointer
			argBuffer:   make([]interface{}, maxArgs), // Preallocated argument buffer
			maxArgCount: maxArgs,
			fieldArrays: make([]interface{}, maxFields), // Preallocated field arrays
			scoreMapPool: &sync.Pool{
				New: func() interface{} { return make(map[string]float32) },
			},
			locMapPool: &sync.Pool{
				New: func() interface{} { return make(map[string]IDLoc) },
			},
		}, nil
	}
}

func compileWasmModule(wasmBytes []byte) (*wasmtime.Store, *wasmtime.Instance, error) {
	engine := wasmtime.NewEngine()
	store := wasmtime.NewStore(engine)

	module, err := wasmtime.NewModule(engine, wasmBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to compile WASM module: %w", err)
	}

	// Create instance with empty imports
	instance, err := wasmtime.NewInstance(store, module, []wasmtime.AsExtern{})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to instantiate WASM module: %w", err)
	}

	return store, instance, nil
}

// Process reranks search results using the WASM function with field data support
func (wf *WasmFunction[T]) Process(ctx context.Context, searchParams *SearchParams, inputs *rerankInputs) (*rerankOutputs, error) {
	outputs := newRerankOutputs(inputs, searchParams)

	for _, cols := range inputs.data {
		idScore, err := wf.processOneSearchData(ctx, searchParams, cols, inputs.idGroupValue)
		if err != nil {
			return nil, err
		}
		appendResult(inputs, outputs, idScore)
	}

	return outputs, nil
}

// processOneSearchData processes a single search result set with field data support
func (wf *WasmFunction[T]) processOneSearchData(ctx context.Context, searchParams *SearchParams, cols []*columns, idGroup map[any]any) (*IDScores[T], error) {
	if len(cols) == 0 {
		return newIDScores[T](map[T]float32{}, map[T]IDLoc{}, searchParams, true), nil
	}

	// Use cached WASM function pointer (no lookup needed)
	rerankFunc := wf.rerankFunc

	col := cols[0]
	if col.size == 0 {
		return newIDScores[T](map[T]float32{}, map[T]IDLoc{}, searchParams, true), nil
	}

	ids := col.ids.([]T)
	scores := col.scores
	// resultCount := len(ids)

	// Get pooled maps (zero allocations)
	rerankedScores := wf.scoreMapPool.Get().(map[T]float32)
	idLocations := wf.locMapPool.Get().(map[T]IDLoc)

	// Clear maps for reuse
	for k := range rerankedScores {
		delete(rerankedScores, k)
	}
	for k := range idLocations {
		delete(idLocations, k)
	}

	// Get input field types for field data access
	inputFieldTypes := wf.GetInputFieldTypes()

	// Fast path: No input fields (most common case) - inline everything
	if len(inputFieldTypes) == 0 {
		// Inline simple rerank calls for maximum performance
		for j, id := range ids {
			originalScore := scores[j]

			// Direct WASM call - interface{} boxing unavoidable due to wasmtime-go API
			result, err := rerankFunc.Call(wf.store, originalScore, int32(j))
			if err != nil {
				// Return maps to pool before error
				wf.scoreMapPool.Put(rerankedScores)
				wf.locMapPool.Put(idLocations)
				return nil, fmt.Errorf("failed to call WASM rerank function: %w", err)
			}

			newScore := result.(float32) // Fixed ABI: guaranteed f32
			idLocations[id] = IDLoc{batchIdx: 0, offset: j}
			rerankedScores[id] = newScore
		}
	} else {
		// Complex path: With input fields
		// Precompute/cast field arrays once per rerank call (major optimization)
		wf.precomputeFieldArrays(col, inputFieldTypes)

		for j, id := range ids {
			originalScore := scores[j]

			var newScore float32
			var err error

			if len(inputFieldTypes) == 1 {
				fieldValue := wf.getPrecomputedFieldValue(j, 0) // Use precomputed array
				result, err := rerankFunc.Call(wf.store, originalScore, int32(j), fieldValue)
				if err == nil {
					newScore = result.(float32) // Fixed ABI: guaranteed f32
				}
			} else {
				newScore, err = wf.callWasmWithMultipleFields(rerankFunc, originalScore, int32(j), j, inputFieldTypes)
			}

			if err != nil {
				// Return maps to pool before error
				wf.scoreMapPool.Put(rerankedScores)
				wf.locMapPool.Put(idLocations)
				return nil, fmt.Errorf("failed to call WASM rerank function: %w", err)
			}

			idLocations[id] = IDLoc{batchIdx: 0, offset: j}
			rerankedScores[id] = newScore
		}
	}

	var result *IDScores[T]
	var err error

	if searchParams.isGrouping() {
		result, err = newGroupingIDScores(rerankedScores, idLocations, searchParams, idGroup)
	} else {
		result = newIDScores(rerankedScores, idLocations, searchParams, true)
	}

	wf.scoreMapPool.Put(rerankedScores)
	wf.locMapPool.Put(idLocations)

	return result, err
}

// Call WASM with multiple fields - fixed f32 return ABI with preallocated buffer
func (wf *WasmFunction[T]) callWasmWithMultipleFields(
	rerankFunc *wasmtime.Func,
	score float32,
	rank int32,
	docIndex int,
	fieldTypes []schemapb.DataType,
) (float32, error) {
	args := wf.argBuffer[:2+len(fieldTypes)] // Slice to exact size needed
	args[0] = score
	args[1] = rank

	for i := range fieldTypes {
		args[2+i] = wf.getPrecomputedFieldValue(docIndex, i)
	}

	result, err := rerankFunc.Call(wf.store, args...)
	if err != nil {
		return 0, err
	}
	return result.(float32), nil // Fixed ABI: guaranteed f32
}

func (wf *WasmFunction[T]) extractFieldValueByIndex(col *columns, docIndex int, fieldType schemapb.DataType, fieldIndex int) interface{} {
	if fieldIndex >= len(col.data) {
		return nil
	}

	switch fieldType {
	case schemapb.DataType_Int32:
		if values, ok := col.data[fieldIndex].([]int32); ok && docIndex < len(values) {
			return values[docIndex]
		}
	case schemapb.DataType_Int64:
		if values, ok := col.data[fieldIndex].([]int64); ok && docIndex < len(values) {
			return values[docIndex]
		}
	case schemapb.DataType_Float:
		if values, ok := col.data[fieldIndex].([]float32); ok && docIndex < len(values) {
			return values[docIndex]
		}
	case schemapb.DataType_Double:
		if values, ok := col.data[fieldIndex].([]float64); ok && docIndex < len(values) {
			return values[docIndex]
		}
	case schemapb.DataType_VarChar:
		if values, ok := col.data[fieldIndex].([]string); ok && docIndex < len(values) {
			return values[docIndex]
		}
	}
	return nil
}

// precomputeFieldArrays casts and caches all field arrays once per rerank call
func (wf *WasmFunction[T]) precomputeFieldArrays(col *columns, fieldTypes []schemapb.DataType) {
	if len(fieldTypes) == 0 || len(col.data) == 0 {
		return
	}

	// Cast each field array once and store in fieldArrays
	for i, fieldType := range fieldTypes {
		if i >= len(col.data) {
			wf.fieldArrays[i] = nil
			continue
		}

		// Cast the entire array once based on type
		switch fieldType {
		case schemapb.DataType_Int32:
			if values, ok := col.data[i].([]int32); ok {
				wf.fieldArrays[i] = values
			} else {
				wf.fieldArrays[i] = nil
			}
		case schemapb.DataType_Int64:
			if values, ok := col.data[i].([]int64); ok {
				wf.fieldArrays[i] = values
			} else {
				wf.fieldArrays[i] = nil
			}
		case schemapb.DataType_Float:
			if values, ok := col.data[i].([]float32); ok {
				wf.fieldArrays[i] = values
			} else {
				wf.fieldArrays[i] = nil
			}
		case schemapb.DataType_Double:
			if values, ok := col.data[i].([]float64); ok {
				wf.fieldArrays[i] = values
			} else {
				wf.fieldArrays[i] = nil
			}
		case schemapb.DataType_VarChar:
			if values, ok := col.data[i].([]string); ok {
				wf.fieldArrays[i] = values
			} else {
				wf.fieldArrays[i] = nil
			}
		default:
			wf.fieldArrays[i] = nil
		}
	}
}

// getPrecomputedFieldValue retrieves field value from precomputed arrays (no casting, minimal checks)
func (wf *WasmFunction[T]) getPrecomputedFieldValue(docIndex, fieldIndex int) interface{} {
	// Assume valid indices (caller responsibility) - remove redundant bounds checks
	arr := wf.fieldArrays[fieldIndex]
	if arr == nil {
		return nil
	}

	switch typedArr := arr.(type) {
	case []int32:
		return typedArr[docIndex]
	case []int64:
		return typedArr[docIndex]
	case []float32:
		return typedArr[docIndex]
	case []float64:
		return typedArr[docIndex]
	case []string:
		return typedArr[docIndex]
	}
	return nil
}

func (wf *WasmFunction[T]) GetInputFieldNames() []string {
	return wf.RerankBase.GetInputFieldNames()
}

func (wf *WasmFunction[T]) GetInputFieldIDs() []int64 {
	return wf.RerankBase.GetInputFieldIDs()
}

func (wf *WasmFunction[T]) IsSupportGroup() bool {
	return wf.RerankBase.IsSupportGroup()
}

func (wf *WasmFunction[T]) GetRankName() string {
	return WasmName
}
