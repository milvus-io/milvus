from pymilvus import DataType

success = "success"


class GPU_HNSW:
    # GPU_HNSW supports the same vector types as HNSW, except BinaryVector
    supported_vector_types = [
        DataType.FLOAT_VECTOR,
        DataType.FLOAT16_VECTOR,
        DataType.BFLOAT16_VECTOR,
        DataType.INT8_VECTOR,
    ]

    supported_metrics = ["L2", "IP", "COSINE"]

    build_params = [
        # M params test
        {"description": "Minimum Boundary Test", "params": {"M": 2}, "expected": success},
        {
            # GPU max M is 512 (not the CPU HNSW 2048): the search kernel stages
            # next_pow2(2*M) candidates in one CUDA block (<=1024 threads), so
            # M>512 would build but never be GPU-searchable.
            "description": "Maximum Boundary Test",
            "params": {"M": 512},
            "expected": success,
        },
        {
            # Negative M is rejected by knowhere's stock HNSW JSON validator
            # (range [2, 2048]) before the Go GPU checker runs, so the message
            # here shows [2, 2048], not the GPU-honest [2, 512] shown for M=513.
            # Either way M<2 is refused.
            "description": "Out of Range Test - Negative",
            "params": {"M": -1},
            "expected": {"err_code": 1100, "err_msg": "param 'M' (-1) should be in range [2, 2048]"},
        },
        {
            # 513 is within knowhere's [2, 2048], so it passes the JSON validator
            # and is caught by the Go GPU checker's honest gpuHnswMaxM=512 bound.
            "description": "Out of Range Test - Too Large",
            "params": {"M": 513},
            "expected": {"err_code": 1100, "err_msg": "param 'M' (513) should be in range [2, 512]"},
        },
        {"description": "String Type Test will ignore the wrong type", "params": {"M": "16"}, "expected": success},
        {
            "description": "Float Type Test",
            "params": {"M": 16.0},
            "expected": {"err_code": 999, "err_msg": "wrong data type in json"},
        },
        {
            "description": "Boolean Type Test",
            "params": {"M": True},
            "expected": {
                "err_code": 999,
                "err_msg": "invalid integer value, key: 'M', value: 'True': invalid parameter",
            },
        },
        {"description": "None Type Test, use default value", "params": {"M": None}, "expected": success},
        {
            "description": "List Type Test",
            "params": {"M": [16]},
            "expected": {
                "err_code": 999,
                "err_msg": "invalid integer value, key: 'M', value: '[16]': invalid parameter",
            },
        },
        # efConstruction params test
        {"description": "Minimum Boundary Test", "params": {"efConstruction": 1}, "expected": success},
        {"description": "Large Value Test", "params": {"efConstruction": 10000}, "expected": success},
        {
            "description": "Out of Range Test - Negative",
            "params": {"efConstruction": -1},
            "expected": {"err_code": 999, "err_msg": "param 'efConstruction' (-1) should be in range [1, 2147483647]"},
        },
        {
            "description": "String Type Test will ignore the wrong type",
            "params": {"efConstruction": "100"},
            "expected": success,
        },
        {
            "description": "Float Type Test",
            "params": {"efConstruction": 100.0},
            "expected": {"err_code": 999, "err_msg": "wrong data type in json"},
        },
        {
            "description": "Boolean Type Test",
            "params": {"efConstruction": True},
            "expected": {
                "err_code": 999,
                "err_msg": "invalid integer value, key: 'efConstruction', value: 'True': invalid parameter",
            },
        },
        {"description": "None Type Test, use default value", "params": {"efConstruction": None}, "expected": success},
        {
            "description": "List Type Test",
            "params": {"efConstruction": [100]},
            "expected": {
                "err_code": 999,
                "err_msg": "invalid integer value, key: 'efConstruction', value: '[100]': invalid parameter",
            },
        },
        # combination params test
        {
            "description": "Optimal Performance Combination Test",
            "params": {"M": 16, "efConstruction": 200},
            "expected": success,
        },
        {"description": "empty dict params", "params": {}, "expected": success},
        {
            "description": "not_defined_param in the dict params",
            "params": {"M": 16, "efConstruction": 200, "not_defined_param": "nothing"},
            "expected": success,
        },
    ]

    search_params = [
        # ef params test
        {
            "description": "Minimum Boundary Test",
            "params": {"ef": 1},
            "expected": {"err_code": 999, "err_msg": "ef(1) should be larger than k(10)"},  # assume default limit=10
        },
        {"description": "Large Value Test", "params": {"ef": 10000}, "expected": success},
        {
            "description": "Out of Range Test - Negative",
            "params": {"ef": -1},
            "expected": {"err_code": 999, "err_msg": "param 'ef' (-1) should be in range [1, 2147483647]"},
        },
        {"description": "String Type Test, not check data type", "params": {"ef": "32"}, "expected": success},
        {
            "description": "Float Type Test",
            "params": {"ef": 32.0},
            "expected": {"err_code": 999, "err_msg": "Type conflict in json: param 'ef' (32.0) should be integer"},
        },
        {
            "description": "Boolean Type Test",
            "params": {"ef": True},
            "expected": {"err_code": 999, "err_msg": "Type conflict in json: param 'ef' (true) should be integer"},
        },
        {
            "description": "None Type Test",
            "params": {"ef": None},
            "expected": {"err_code": 999, "err_msg": "Type conflict in json: param 'ef' (null) should be integer"},
        },
        {
            "description": "List Type Test",
            "params": {"ef": [32]},
            "expected": {"err_code": 999, "err_msg": "param 'ef' ([32]) should be integer"},
        },
        # combination params test
        {"description": "Optimal Performance Combination Test", "params": {"ef": 64}, "expected": success},
        {"description": "empty dict params", "params": {}, "expected": success},
    ]
