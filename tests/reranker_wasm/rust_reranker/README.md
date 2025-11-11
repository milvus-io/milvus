# Rust WASM Reranker Example

This is an example WASM reranker module written in Rust that can be used with Milvus.

## Prerequisites

```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Add wasm32 target
rustup target add wasm32-unknown-unknown
```

## Building

```bash
# Build the WASM module
cargo build --target wasm32-unknown-unknown --release

# The output will be at:
# target/wasm32-unknown-unknown/release/rust_reranker.wasm
```

## Optimizing Size

```bash
# Install wasm-opt (part of binaryen)
# On macOS:
brew install binaryen

# On Linux:
sudo apt-get install binaryen

# Optimize the WASM module
wasm-opt -Oz -o rust_reranker_opt.wasm target/wasm32-unknown-unknown/release/rust_reranker.wasm
```

## Using with Milvus

```python
import base64
from pymilvus import Collection, Function, CollectionSchema

# Read and encode the WASM module
with open('target/wasm32-unknown-unknown/release/rust_reranker.wasm', 'rb') as f:
    wasm_bytes = f.read()
    wasm_base64 = base64.b64encode(wasm_bytes).decode('utf-8')

# Create reranker function
rerank_func = Function(
    name="rust_rerank",
    function_type="Rerank",
    input_field_names=[],
    params={
        "reranker": "wasm",
        "wasm_code": wasm_base64,
        "entry_point": "rerank"  # or "time_decay_rerank", "log_boost_rerank", etc.
    }
)

# Use in collection schema
schema = CollectionSchema(
    fields=[...],
    functions=[rerank_func]
)

collection = Collection("my_collection", schema=schema)
```

## Available Functions

1. **`rerank`** - Position-based decay (default)
   - Reduces scores for items further down the list
   - Decay factor: 1 / (1 + 0.05 * rank)

2. **`time_decay_rerank`** - Exponential time decay
   - Simulates time-based relevance decay
   - Uses exponential function: e^(-0.01 * rank)

3. **`log_boost_rerank`** - Logarithmic position boost
   - More dramatic changes for top results
   - Uses logarithmic scaling

4. **`simple_boost`** - Simple score multiplier
   - Multiplies all scores by 1.5
   - Good for testing

5. **`nonlinear_rerank`** - Complex non-linear transformation
   - Applies sigmoid-like transformation
   - Clamps results to [0, 1] range

## Testing

```bash
cargo test
```

The compiled WASM module is very small:
- Unoptimized: ~2-3 KB
- Optimized with `wasm-opt -Oz`: ~1 KB

