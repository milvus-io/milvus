from pymilvus import DataType

success = "success"


class GPU_HNSW_SQ:
    # GPU_HNSW with SQ quantization supports same types as HNSW_SQ (no BinaryVector)
    supported_vector_types = [
        DataType.FLOAT_VECTOR,
        DataType.FLOAT16_VECTOR,
        DataType.BFLOAT16_VECTOR,
        DataType.INT8_VECTOR
    ]

    supported_metrics = ['L2', 'IP', 'COSINE']

    build_params = [
        # M params test
        {
            "description": "Minimum Boundary Test",
            "params": {"M": 2},
            "expected": success
        },
        {
            # GPU max M is 512 (not the CPU HNSW 2048): the search kernel stages
            # next_pow2(2*M) candidates in one CUDA block (<=1024 threads), so
            # M>512 would build but never be GPU-searchable.
            "description": "Maximum Boundary Test",
            "params": {"M": 512},
            "expected": success
        },
        {
            # Negative M is rejected by knowhere's stock HNSW JSON validator
            # (range [2, 2048]) before the Go GPU checker runs, so the message
            # here shows [2, 2048], not the GPU-honest [2, 512] shown for M=513.
            # Either way M<2 is refused.
            "description": "Out of Range Test - Negative",
            "params": {"M": -1},
            "expected": {"err_code": 1100, "err_msg": "param 'M' (-1) should be in range [2, 2048]"}
        },
        {
            # 513 is within knowhere's [2, 2048], so it passes the JSON validator
            # and is caught by the Go GPU checker's honest gpuHnswMaxM=512 bound.
            "description": "Out of Range Test - Too Large",
            "params": {"M": 513},
            "expected": {"err_code": 1100, "err_msg": "param 'M' (513) should be in range [2, 512]"}
        },
        {
            "description": "String Type Test will ignore the wrong type",
            "params": {"M": "16"},
            "expected": success
        },
        {
            "description": "Float Type Test",
            "params": {"M": 16.0},
            "expected": {"err_code": 1100, "err_msg": "wrong data type in json"}
        },
        {
            "description": "Boolean Type Test",
            "params": {"M": True},
            "expected": {"err_code": 1100, "err_msg": "invalid integer value, key: 'M', value: 'True': invalid parameter"}
        },
        {
            "description": "None Type Test, use default value",
            "params": {"M": None},
            "expected": success
        },
        {
            "description": "List Type Test",
            "params": {"M": [16]},
            "expected": {"err_code": 1100, "err_msg": "invalid integer value, key: 'M', value: '[16]': invalid parameter"}
        },
        {
            "description": "Nested dict in params",
            "params": {"M": {"value": 16}},
            "expected": {"err_code": 1100, "err_msg": "invalid integer value"}
        },
        # efConstruction params test
        {
            "description": "Minimum Boundary Test",
            "params": {"efConstruction": 1},
            "expected": success
        },
        {
            "description": "Large Value Test",
            "params": {"efConstruction": 10000},
            "expected": success
        },
        {
            "description": "Out of Range Test - Negative",
            "params": {"efConstruction": -1},
            "expected": {"err_code": 1100, "err_msg": "param 'efConstruction' (-1) should be in range [1, 2147483647]"}
        },
        {
            "description": "String Type Test will ignore the wrong type",
            "params": {"efConstruction": "100"},
            "expected": success
        },
        {
            "description": "Float Type Test",
            "params": {"efConstruction": 100.0},
            "expected": {"err_code": 1100, "err_msg": "wrong data type in json"}
        },
        {
            "description": "Boolean Type Test",
            "params": {"efConstruction": True},
            "expected": {"err_code": 1100, "err_msg": "invalid integer value, key: 'efConstruction', value: 'True': invalid parameter"}
        },
        {
            "description": "None Type Test, use default value",
            "params": {"efConstruction": None},
            "expected": success
        },
        {
            "description": "List Type Test",
            "params": {"efConstruction": [100]},
            "expected": {"err_code": 1100, "err_msg": "invalid integer value, key: 'efConstruction', value: '[100]': invalid parameter"}
        },
        # sq_type params test
        {
            "description": "SQ8 quantization type",
            "params": {"sq_type": "SQ8"},
            "expected": success
        },
        {
            "description": "SQ6 quantization type",
            "params": {"sq_type": "SQ6"},
            "expected": success
        },
        {
            "description": "FP16 quantization type",
            "params": {"sq_type": "FP16"},
            "expected": success
        },
        {
            "description": "BF16 quantization type",
            "params": {"sq_type": "BF16"},
            "expected": success
        },
        {
            "description": "Invalid quantization type",
            "params": {"sq_type": "SQ4"},
            "expected": {"err_code": 1100, "err_msg": "invalid sq_type"}
        },
        # refine params test
        {
            "description": "Refine enabled with FP16",
            "params": {"sq_type": "SQ8", "refine": True, "refine_type": "FP16"},
            "expected": success
        },
        {
            "description": "Refine enabled with FP32",
            "params": {"sq_type": "SQ8", "refine": True, "refine_type": "FP32"},
            "expected": success
        },
        {
            "description": "Invalid refine_type using vector data type",
            "params": {"sq_type": "SQ8", "refine": True, "refine_type": "INT8"},
            "expected": {"err_code": 1100, "err_msg": "invalid refine type"}
        },
        # combination params test
        {
            "description": "empty dict params",
            "params": {},
            "expected": success
        },
        {
            "description": "All optional parameters None",
            "params": {"M": None, "efConstruction": None, "sq_type": None, "refine": None, "refine_type": None},
            "expected": success
        },
        {
            "description": "Typical valid combination",
            "params": {"M": 16, "efConstruction": 200, "sq_type": "SQ8", "refine": True, "refine_type": "FP16"},
            "expected": success
        },
        {
            "description": "Minimum boundary combination",
            "params": {"M": 2, "efConstruction": 1, "sq_type": "SQ6"},
            "expected": success,
            "relaxed_limit": True
        },
        {
            "description": "Maximum boundary combination",
            "params": {"M": 512, "efConstruction": 10000, "sq_type": "FP16", "refine": True, "refine_type": "FP32"},
            "expected": success
        },
    ]

    search_params = [
        # ef params test
        {
            "description": "Boundary Test - ef equals k",
            "params": {"ef": 10},
            "expected": success
        },
        {
            "description": "Minimum Boundary Test",
            "params": {"ef": 1},
            "expected": {"err_code": 65535, "err_msg": "ef(1) should be larger than k(10)"}   # assume default limit=10
        },
        {
            "description": "Large Value Test",
            "params": {"ef": 10000},
            "expected": success
        },
        {
            "description": "Out of Range Test - Negative",
            "params": {"ef": -1},
            "expected": {"err_code": 65535, "err_msg": "param 'ef' (-1) should be in range [1, 2147483647]"}
        },
        {
            "description": "String Type Test, not check data type",
            "params": {"ef": "32"},
            "expected": success
        },
        {
            "description": "Float Type Test",
            "params": {"ef": 32.0},
            "expected": {"err_code": 65535, "err_msg": "Type conflict in json: param 'ef' (32.0) should be integer"}
        },
        {
            "description": "Boolean Type Test",
            "params": {"ef": True},
            "expected": {"err_code": 65535, "err_msg": "Type conflict in json: param 'ef' (true) should be integer"}
        },
        {
            "description": "None Type Test",
            "params": {"ef": None},
            "expected": {"err_code": 65535, "err_msg": "Type conflict in json: param 'ef' (null) should be integer"}
        },
        {
            "description": "List Type Test",
            "params": {"ef": [32]},
            "expected": {"err_code": 65535, "err_msg": "param 'ef' ([32]) should be integer"}
        },
        # refine_k params test
        {
            "description": "refine_k default boundary",
            "params": {"refine_k": 1},
            "expected": success
        },
        {
            "description": "refine_k valid float",
            "params": {"refine_k": 2.5},
            "expected": success
        },
        {
            "description": "refine_k out of range",
            "params": {"refine_k": 0},
            "expected": {"err_code": 65535, "err_msg": "Out of range in json"}
        },
        {
            "description": "refine_k integer type",
            "params": {"refine_k": 20},
            "expected": success
        },
        # combination test
        {
            "description": "Optimal combination",
            "params": {"ef": 64, "refine_k": 2},
            "expected": success
        },
        {
            "description": "empty dict params",
            "params": {},
            "expected": success
        },
    ]
