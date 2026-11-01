# SGLang text embedding example

This example uses the existing OpenAI-compatible `TextEmbedding` provider. It
does not add an SGLang-specific provider.

It pins SGLang to `v0.5.21` and the model to
`Qwen/Qwen3-Embedding-0.6B@f57bff507cba2367b3afbfbc9847736d3f2479cb`. The
model produces 1024-dimensional embeddings, so the Milvus output field must
also have dimension 1024.

## Start the services

The example requires Docker with an NVIDIA GPU runtime, Docker Compose v2, and
an available GPU. Set a persistent Hugging Face cache before starting:

```shell
cd deployments/docker/standalone
export HF_HOME="$HOME/.cache/huggingface"
mkdir -p "$HF_HOME"
docker compose -f docker-compose.yml -f docker-compose-sglang.yml up -d
```

`milvus-sglang.yaml` configures the existing `openai` provider to call
`http://sglang:30000/v1/embeddings` on the Compose network. SGLang ignores the
non-empty `local-sglang` credential unless it is started with `--api-key`; if
you enable SGLang API-key authentication, replace that value with the same
secret.

Wait until SGLang reports HTTP 200 from its health endpoint, then verify the
embedding endpoint directly:

```shell
until curl -fsS http://localhost:30000/health >/dev/null; do sleep 2; done
curl -fsS http://localhost:30000/v1/embeddings \
  -H 'Content-Type: application/json' \
  -d '{"model":"Qwen/Qwen3-Embedding-0.6B","input":"Milvus is a vector database."}'
```

## Run the end-to-end workflow

Install PyMilvus in the environment that will run the example, then execute:

```shell
python3 -m pip install pymilvus
python3 sglang_embedding_example.py
```

The script creates a collection with a `TextEmbedding` function, inserts text,
builds an index, and searches with raw text. Collection creation calls SGLang
to validate the function. If SGLang is unavailable, creation fails with a
retriable service-unavailable error. If SGLang returns a vector whose dimension
is not 1024, creation fails with the expected and returned dimensions.

Stop the example services with:

```shell
docker compose -f docker-compose.yml -f docker-compose-sglang.yml down
```
