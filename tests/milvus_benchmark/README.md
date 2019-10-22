# Requirements

- python 3.6+
- pip install -r requirements.txt

# How to use this Test Project

This project is used to test performance / accuracy /  stability of milvus server

1. update your test configuration in suites_*.yaml
2. run command

```shell
### docker mode:
python main.py --image=milvusdb/milvus:latest --run-count=2 --run-type=performance

### local mode:
python main.py --local --run-count=2 --run-type=performance --ip=127.0.0.1 --port=19530
```

# Contribution getting started

- Follow PEP-8 for naming and black for formatting.