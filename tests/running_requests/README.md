# Running request test suite

These cases exercise List and Cancel against a dedicated Milvus deployment. The
single-Proxy smoke tests live in the Python, Go, Java, and REST SDK suites.
Run the distributed cases explicitly because some tests change authorization or
restart a Proxy. Use an isolated deployment and an operator credential supplied
by the test environment.

For the normal distributed cases, set `PROXY_A_URI`, `PROXY_B_URI`,
`PROXY_A_ID`, `PROXY_B_ID`, `MILVUS_TOKEN`, and `MILVUS_TEST_USER`.
Set `MILVUS_L1_NAMESPACE` and `MILVUS_L1_RELEASE` for cases that collect Pod
evidence or change the deployment. Use the two direct Proxy endpoints.

```sh
cd tests/running_requests
python -m pytest -q
```

Build the optional Java and Go test workers with `python native_sdk/build.py`.
The Java worker requires JDK 17 and Maven. The build helper pins both SDKs.
The legacy Python case uses the interpreter in `MILVUS_OLD_PYTHON`.

T11 uses a separate two-Deployment instance. Enable it with
`MILVUS_T11_FORWARD=1` and supply `MILVUS_L1_OLD_IMAGE`,
`MILVUS_L1_NEW_IMAGE`, `MILVUS_T11_CORE_DEPLOYMENTS`, and a provider with
`hold_proxy_search`. Run the T11 test file explicitly.

For the T11 transport parity cases, point the deployment inputs at an existing
mixed-version instance with newer Proxy A and older Proxy B, and set
`MILVUS_T11_MIXED_TRANSPORT=1`. The tests exercise the older Proxy's
`unimplemented` response using Java, Go and REST management calls.

The T12 Proxy checkpoints live in `t12-failpoint/`. Apply the overlay to a
compatible disposable source checkout with `python t12-failpoint/apply.py
/path/to/milvus` before building the test server. Enable
`github.com/milvus-io/milvus/internal/proxy/reqregistry/t12InternalStage=return(true)`
in `GO_FAILPOINTS` and export fully sampled Proxy traces to stdout. Use
`MILVUS_L1_CHECKPOINT_PROVIDER=t12_provider`, add the provider directory to
`PYTHONPATH`, and set `RR_LOCAL_T04_PROXY_A` to the Docker container that
serves the target Search. The provider requires an isolated workload collection.
