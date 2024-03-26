

# how to use

1. prepare data for search and query perf test, and create an empty collection for insert perf test
```shell
python3 prepare_data.py --host "xxx" --port 19530 --data_size 1000000
python3 create_collection.py --host "xxx"
```

2. run perf test compare with sdk and http
```shell
python3 test_perf_with_sdk_vs_rest.py --host "xxx"
````

3. run perf test with locust
```shell
locust -f test_xxx_perf.py
```

