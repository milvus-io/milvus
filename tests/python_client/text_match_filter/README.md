

## step

```bash

# 1. Create dataset
# array element is integer
python3 create_dataset.py --data_size 1000000 --hit_rate 0.005
# array element is string
python3 create_dataset_varchar.py --data_size 1000000 --hit_rate 0.005

# 2. Prepare dataset
python3 prepare_dataset.py --host 127.0.0.1 --minio_host 19530 --element_datatype "int64"
# or
python3 prepare_dataset.py --host 127.0.0.1 --minio_host 19530 --element_datatype "varchar"

# 3. Run array_filter query test
locust -f filter_query.py

```
