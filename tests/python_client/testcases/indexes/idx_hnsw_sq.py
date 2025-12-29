from pymilvus import DataType

success = "success"


class HNSW_SQ:
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
            "description": "Maximum Boundary Test",
            "params": {"M": 2048},
            "expected": success
        },
        {
            "description": "Out of Range Test - Negative",
            "params": {"M": -1},
            "expected": {"err_code": 1100, "err_msg": "param 'M' (-1) should be in range [2, 2048]"}
        },
        {
            "description": "Out of Range Test - Too Large",
            "params": {"M": 2049},
            "expected": {"err_code": 1100, "err_msg": "param 'M' (2049) should be in range [2, 2048]"}
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
            "description": "Valid sq_type - SQ6",
            "params": {"sq_type": "SQ6"},
            "expected": success
        },
        {
            "description": "Valid sq_type - SQ8",
            "params": {"sq_type": "SQ8"},
            "expected": success
        },
        {
            "description": "Valid sq_type - BF16",
            "params": {"sq_type": "BF16"},
            "expected": success
        },
        {
            "description": "Valid sq_type - FP16",
            "params": {"sq_type": "FP16"},
            "expected": success
        },
        {
            "description": "Out of Range Test - Unknown String",
            "params": {"sq_type": "FP32"},
            "expected": {"err_code": 1100, "err_msg": "invalid scalar quantizer type: invalid parameter"}
        },
        {
            "description": "Integer Type Test",
            "params": {"sq_type": 8},
            "expected": {"err_code": 1100, "err_msg": "invalid scalar quantizer type: invalid parameter"}
        },
        {
            "description": "Float Type Test",
            "params": {"sq_type": 8.0},
            "expected": {"err_code": 1100, "err_msg": "invalid scalar quantizer type: invalid parameter"}
        },
        {
            "description": "Boolean Type Test",
            "params": {"sq_type": True},
            "expected": {"err_code": 1100, "err_msg": "invalid scalar quantizer type: invalid parameter"}
        },
        {
            "description": "None Type Test, use default value",
            "params": {"sq_type": None},
            "expected": success
        },
        {
            "description": "List Type Test",
            "params": {"sq_type": ["SQ8"]},
            "expected": {"err_code": 1100, "err_msg": "invalid scalar quantizer type: invalid parameter"}
        },

        # refine params test
        {
            "description": "refine = True",
            "params": {"refine": True},
            "expected": success
        },
        {
            "description": "String Type Test",
            "params": {"refine": "true"},
            "expected": success
        },
        {
            "description": "String Type Test",
            "params": {"refine": "test"},
            "expected": {"err_code": 1100, "err_msg": "should be a boolean: invalid parameter"}

        },
        {
            "description": "Integer Type Test",
            "params": {"refine": 1},
            "expected": {"err_code": 1100, "err_msg": "should be a boolean: invalid parameter"}
        },
        {
            "description": "Float Type Test",
            "params": {"refine": 1.0},
            "expected": {"err_code": 1100, "err_msg": "should be a boolean: invalid parameter"}
        },
        {
            "description": "List Type Test",
            "params": {"refine": [True]},
            "expected": {"err_code": 1100, "err_msg": "should be a boolean: invalid parameter"}
        },
        {
            "description": "None Type Test, use default value",
            "params": {"refine": None},
            "expected": success
        },

        # refine_type params test
        {
            "description": "Valid refine_type - SQ6",
            "params": {"refine_type": "SQ6"},
            "expected": success
        },
        {
            "description": "Valid refine_type - SQ8",
            "params": {"refine_type": "SQ8"},
            "expected": success
        },
        {
            "description": "Valid refine_type - BF16",
            "params": {"refine_type": "BF16"},
            "expected": success
        },
        {
            "description": "Valid refine_type - FP16",
            "params": {"refine_type": "FP16"},
            "expected": success
        },
        {
            "description": "Valid refine_type - FP32",
            "params": {"refine_type": "FP32"},
            "expected": success
        },
        {
            "description": "Out of Range Test - unknown value",
            "params": {"refine_type": "INT8"},
            "expected": {"err_code": 1100, "err_msg": "invalid refine type : INT8, optional types are [sq6, sq8, fp16, bf16, fp32, flat]: invalid parameter"}
        },
        {
            "description": "Integer Type Test",
            "params": {"refine_type": 1},
            "expected": {"err_code": 1100, "err_msg": "invalid refine type : 1, optional types are [sq6, sq8, fp16, bf16, fp32, flat]: invalid parameter"}
        },
        {
            "description": "Float Type Test",
            "params": {"refine_type": 1.0},
            "expected": {"err_code": 1100, "err_msg": "invalid refine type : 1.0, optional types are [sq6, sq8, fp16, bf16, fp32, flat]: invalid parameter"}
        },
        {
            "description": "List Type Test",
            "params": {"refine_type": ["FP16"]},
            "expected": {"err_code": 1100, "err_msg": "['FP16'], optional types are [sq6, sq8, fp16, bf16, fp32, flat]: invalid parameter"}
        },
        {
            "description": "None Type Test, use default value",
            "params": {"refine_type": None},
            "expected": success
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
            "expected": success
        },
        {
            "description": "Maximum boundary combination",
            "params": {"M": 2048, "efConstruction": 10000, "sq_type": "FP16", "refine": True, "refine_type": "FP32"},
            "expected": success
        },
        {
            "description": "Unknown extra parameter in combination",
            "params": {"M": 16, "efConstruction": 200, "sq_type": "SQ8", "refine": True, "refine_type": "FP16", "unknown_param": "nothing"},
            "expected": success
        },
        {
            "description": "Partial parameters set (M + sq_type only)",
            "params": {"M": 32, "sq_type": "BF16"},
            "expected": success
        },
        {
            "description": "Partial parameters set (efConstruction + refine only)",
            "params": {"efConstruction": 500,"refine": True},
            "expected": success
        },
        {
            "description": "Invalid refine_type using vector data type",
            "params": {"sq_type": "SQ8", "refine": True, "refine_type": "INT8"},
            "expected": {"err_code": 1100, "err_msg": "invalid refine type"}
        }

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
        {
            "description": "String Type Test, not check data type",
            "params": {"refine_k": "2.5"},
            "expected": success
        },
        {
            "description": "empty string type",
            "params": {"refine_k": ""},
            "expected": {"err_code": 65535, "err_msg": "invalid float value"}
        },
        {
            "description": "refine_k boolean type",
            "params": {"refine_k": True},
            "expected": {"err_code": 65535, "err_msg": "Type conflict in json: param 'refine_k' (true) should be a number"}
        },
        {
            "description": "None Type Test",
            "params": {"refine_k": None},
            "expected": {"err_code": 65535, "err_msg": "Type conflict in json"}
        },
        {
            "description": "List Type Test",
            "params": {"refine_k": [15]},
            "expected": {"err_code": 65535, "err_msg":"Type conflict in json"}
        },

        # combination params test
        {
            "description": "HNSW ef + SQ refine_k combination",
            "params": {"ef": 64, "refine_k": 2},
            "expected": success
        },
        {
            "description": "Valid ef with invalid refine_k",
            "params": {"ef": 64, "refine_k": 0},
            "expected": {"err_code": 65535, "err_msg":"Out of range in json"}
        },
        {
            "description": "empty dict params",
            "params": {},
            "expected": success
        },

    ]
