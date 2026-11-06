# Model Tokenizer

The `model` tokenizer runs an ONNX graph on text and decodes its named outputs
into terms with UTF-8 byte offsets, positions, and position lengths. The model
package supplies both the graph and the decoder configuration.

## Package and runtime contract

The model package contains package-local files only:

```text
manifest.json
model.onnx
decoder.json
```

Configure a local package directory in the analyzer:

```json
{
  "tokenizer": {
    "type": "model",
    "package": {"type": "local", "path": "/models/my_tokenizer"},
    "session_pool_size": 4
  }
}
```

For a File Resource, use
`"package": {"type": "remote", "resource_name": "my_tokenizer"}`.
The resource helper resolves the root-level `manifest.json` and records the
resource ID for the package.

The root `manifest.json` binds the model and its decoder by package-local file
names:

```json
{
  "model": "model.onnx",
  "decoder": "decoder.json"
}
```

The manifest and decoder schemas reject unknown fields. The analyzer parameter
`session_pool_size` defaults to 4 and must be between 1 and 128.

The process must have an ONNX Runtime CPU shared library supporting C API 23.
Set `ORT_DYLIB_PATH` to its path, or install it under the platform library
name (`libonnxruntime.so` on Linux). The runtime is loaded when the model
tokenizer is created.

The ONNX graph has exactly one input named `text`, with type
`tensor(string)` and shape `[dynamic_batch]` or `[1]`. Each invocation supplies
one text. The graph must include the preprocessing needed by its model,
including normalization, vocabulary lookup, and feature extraction.

The tokenizer creates all configured ONNX Runtime sessions before loading
returns; partial initialization failure drops the sessions already created.
Analyzer clones share the pool and decoder through an `Arc`, without loading
models or reserving sessions. At inference time the caller borrows one
session, runs and decodes synchronously, and returns it before exposing the
token stream. A full pool queues callers in FIFO order using one-shot handoff
channels; a returned session goes to the oldest waiter before new callers.
A lease returns the session on success, error, or Rust stack unwinding.
Neither model execution nor decoding holds the pool mutex. Different sessions
run concurrently on their calling threads.

Embedded top-level FP32/INT64 initializers are extracted once using the
existing Protobuf dependency and shared via ORT `AddInitializer`. Supported
CPU kernels also share a prepacked-weights container. Other initializer types,
subgraph-local parameters, external tensor data, and overridable inputs retain
ORT's normal loading behavior. Each session uses its own execution state and
arena, with one intra-op and one inter-op thread, independent of any global
ORT thread pool. Sessions keep the shared
initializer and packed-buffer owners alive. The final analyzer clone releases
the pool and its sessions.

The session cap applies to one loaded tokenizer and its clones, not every
independently loaded analyzer in the process. Reopening the same package
creates a separate pool. There is no timeout/cancellation API or global
CPU/memory admission control. Model-package files must remain unchanged during
loading; reloading constructs a new pool.

## Decoder schema

`decoder.json` has four explicit layers:

1. `outputs` maps semantic roles used by the remaining configuration to real
   ONNX output names.
2. `representation` declares the tensor's training-time meaning and its
   alignment to the source text.
3. `decoder` turns model scores into discrete labels or states.
4. `projection` maps labels or states into term boundaries, or accepts final
   term segments directly.

### Categorical unit scores

Categorical scores have shape `[batch, units, classes]`. The example below
uses four BMES classes. The class order must match the model output channels.
An additional offset output maps model units, such as subwords, to spans in
the source text:

```json
{
  "outputs": {
    "scores": "boundary_logits",
    "offsets": "unit_offsets"
  },
  "representation": {
    "type": "unit_scores",
    "output": "scores",
    "alignment": {
      "type": "offsets",
      "output": "offsets",
      "offset_unit": "unicode_scalar",
      "columns": {"start": 0, "end": 1}
    },
    "distribution": {
      "type": "categorical",
      "classes": ["B", "E", "S", "M"]
    }
  },
  "decoder": {"type": "argmax"},
  "projection": {
    "type": "action_sequence",
    "actions": {
      "B": "begin",
      "E": "end",
      "S": "single",
      "M": "continue"
    }
  }
}
```

Replacing `argmax` with the following decoder applies Viterbi decoding. CRF
parameters are embedded directly in `decoder.json`; no separate parameter file
is needed. Export the model's learned parameters in the same order as
`classes`, with `transitions[from][to]` describing each label transition. The
following values illustrate the structure only:

```json
{
  "type": "linear_chain_crf",
  "start_transitions": [0.0, 0.0, 0.0, 0.0],
  "end_transitions": [0.0, 0.0, 0.0, 0.0],
  "transitions": [
    [0.0, 0.0, 0.0, 0.0],
    [0.0, 0.0, 0.0, 0.0],
    [0.0, 0.0, 0.0, 0.0],
    [0.0, 0.0, 0.0, 0.0]
  ]
}
```

The start and end arrays must each contain one score per class, and the
transition matrix must be square with the same class count. For BMES this is
only 24 scores, so embedding them keeps the model package self-contained.

### Independent Bernoulli heads

Independent Bernoulli logits have shape `[batch, units, heads]`. A state
table defines allowed joint assignments of the named heads. The runtime
selects the state with the greatest sum of Bernoulli log likelihoods. This
example uses token-boundary, sentence-boundary, and MWT heads:

```json
{
  "outputs": {"scores": "boundary_logits"},
  "representation": {
    "type": "unit_scores",
    "output": "scores",
    "alignment": {"type": "unicode_scalar"},
    "distribution": {
      "type": "independent_bernoulli",
      "heads": ["token", "sentence", "mwt"]
    }
  },
  "decoder": {
    "type": "state_table",
    "states": [
      {"name": "inside", "when": {"token": false, "sentence": false, "mwt": false}},
      {"name": "token_end", "when": {"token": true, "sentence": false, "mwt": false}},
      {"name": "sentence_end", "when": {"token": true, "sentence": true, "mwt": false}},
      {"name": "mwt_end", "when": {"token": true, "sentence": false, "mwt": true}},
      {"name": "sentence_mwt_end", "when": {"token": true, "sentence": true, "mwt": true}}
    ]
  },
  "projection": {
    "type": "action_sequence",
    "actions": {
      "inside": "continue",
      "token_end": "end",
      "sentence_end": "end",
      "mwt_end": "end",
      "sentence_mwt_end": "end"
    },
    "skip_whitespace": true
  }
}
```

Implicit alignment can use either `unicode_scalar` or `utf8_byte`. Its number
of source units must not exceed the score sequence length; a longer score
sequence permits model padding. Explicit offsets are shaped
`[batch, units, width]`, use signed 64-bit integers, and stop at a negative
start offset.

### Final term segments

Models or conversion graphs may return final segments as signed 64-bit values
with shape `[batch, terms, width]`; a negative start offset terminates the row.
This path supports synonyms and search-mode
tokens through explicit `position` and `position_length` columns:

```json
{
  "outputs": {"terms": "term_segments"},
  "representation": {
    "type": "segments",
    "output": "terms",
    "offset_unit": "unicode_scalar",
    "columns": {"start": 0, "end": 1, "position": 2, "position_length": 3}
  },
  "decoder": {"type": "identity"},
  "projection": {"type": "term_spans"}
}
```

## Validation and failure behavior

Package loading rejects unknown JSON fields, unused or missing output roles,
duplicate labels, invalid decoder/representation combinations, unsafe
non-local file names, CRF dimension mismatches, and incompatible ONNX output
types or ranks. Runtime decoding checks batch and channel dimensions, offset
bounds and UTF-8 boundaries, ordered alignment starts and term positions, and
positive position lengths.

Malformed configuration and initializer metadata are invalid arguments.
ONNX Runtime load, inference, and tensor-construction failures are internal
errors. Tantivy's tokenizer trait cannot return an error from `token_stream`;
an inference failure is logged and produces an empty stream.
Inference and decode errors affect only their own request; the session lease
is returned so subsequent requests can proceed.
