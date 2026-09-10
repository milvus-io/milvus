
## How to run the test cases

install milvus with authentication enabled

```bash
pip install -r requirements.txt
pytest testcases --tags L0 L1 -n 6 -v --endpoint http://127.0.0.1:19530 --minio_host 127.0.0.1
```
