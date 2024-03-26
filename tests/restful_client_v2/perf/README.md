

# how to use

1. prepare data for search and query perf test, and create an empty collection for insert perf test
```shell
python3 prepare_data.py --host "127.0.0.1" --data_size 1000000
python3 prepare_collection_for_insert_perf.py --host "127.0.0.1"
```

2. run perf test compare with sdk and http
```shell
python3 test_perf_with_sdk_vs_rest.py --host "127.0.0.1"
````

3. run perf test with locust
```shell
locust -f test_xxx_perf.py -H "127.0.0.1:19530" --headless
```

