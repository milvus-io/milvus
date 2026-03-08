from pymilvus import DataType

success = "success"


class HNSW_PRQ:
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
        {
            "description": "Nested List in Params",
            "params": {"efConstruction": [[100]]},
            "expected": {"err_code": 1100, "err_msg": "invalid integer value"}
        },
        # m params test
        {
            "description": "Minimum Boundary Test",
            "params": {"m": 1},
            "expected": success
        },
        # timeout
        # {
        #     "description": "Half of Dimension Value Test",
        #     "params": {"m": 64},
        #     "expected": success
        # },
        # timeout
        # {
        #     "description": "Maximum Boundary Test (Dimension)",
        #     "params": {"m": 128},
        #     "expected": success
        # },
        {
            "description": "Negative Value Test",
            "params": {"m": -1},
            "expected": {
                "err_code": 1100,
                "err_msg": "Out of range in json: param 'm' (-1) should be in range [1, 65536]"
            }
        },
        {
            "description": "Larger Value Test",
            "params": {"m": 256},
            "expected": {
                "err_code": 1100,
                "err_msg": "The dimension of a vector (dim) should be a multiple of the number of subquantizers (m)."
            }
        },
        {
            "description": "Not Divisible by Dimension Value Test",
            "params": {"m": 7},
            "expected": {
                "err_code": 1100,
                "err_msg": "The dimension of a vector (dim) should be a multiple of the number of subquantizers (m)."
            }
        },
        {
            "description": "String Type Test",
            "params": {"m": "16"},
            "expected": success
        },
        {
            "description": "Float Type Test",
            "params": {"m": 16.0},
            "expected": {
                "err_code": 1100,
                "err_msg": "wrong data type in json, key: 'm', value: '16.0': invalid parameter"
            }
        },
        {
            "description": "Boolean Type Test",
            "params": {"m": True},
            "expected": {
                "err_code": 1100,
                "err_msg": "invalid integer value"
            }
        },
        {
            "description": "List Type Test",
            "params": {"m": [16]},
            "expected": {
                "err_code": 1100,
                "err_msg": "invalid integer value"
            }
        },
        {
            "description": "None Type Test",
            "params": {"m": None},
            "expected": success
        },
        # nbits params test
        {
            "description": "Minimum Boundary Test",
            "params": {"nbits": 1},
            "expected": success
        },
        {
            "description": "Default Value Test",
            "params": {"nbits": 8},
            "expected": success
        },
        {
            "description": "Maximum Boundary Test",
            "params": {"nbits": 10},
            "expected": success
        },
        {
            "description": "Negative Value Test",
            "params": {"nbits": -1},
            "expected": {
                "err_code": 1100,
                "err_msg": "Out of range in json: param 'nbits' (-1) should be in range [1, 24]: invalid parameter"
            }
        },
        {
            "description": "Large Value Test",
            "params": {"nbits": 25},
            "expected": {
                "err_code": 1100,
                "err_msg": "Out of range in json: param 'nbits' (25) should be in range [1, 24]"
            }
        },
        {
            "description": "String Type Test",
            "params": {"nbits": "8"},
            "expected": success
        },
        {
            "description": "Float Type Test",
            "params": {"nbits": 8.0},
            "expected": {
                "err_code": 1100,
                "err_msg": "wrong data type in json, key: 'nbits', value: '8.0'"
            }
        },
        {
            "description": "Boolean Type Test",
            "params": {"nbits": True},
            "expected": {
                "err_code": 1100,
                "err_msg": "invalid integer value"
            }
        },
        {
            "description": "List Type Test",
            "params": {"nbits": [8]},
            "expected": {
                "err_code": 1100,
                "err_msg": "invalid integer value"
            }
        },
        {
            "description": "None Type Test",
            "params": {"nbits": None},
            "expected": success
        },
        # nrq params test
        {
            "description": "Minimum Boundary Test",
            "params": {"nrq": 1},
            "expected": success
        },
        {
            "description": "Default Value Test",
            "params": {"nrq": 2},
            "expected": success
        },
        {
            "description": "Maximum Boundary Test",
            "params": {"nrq": 16},
            "expected": success
        },
        {
            "description": "Negative Value Test",
            "params": {"nrq": -1},
            "expected": {
                "err_code": 1100,
                "err_msg": "Out of range in json: param 'nrq' (-1) should be in range [1, 16]"
            }
        },
        {
            "description": "Larger Value Test",
            "params": {"nrq": 17},
            "expected": {
                "err_code": 1100,
                "err_msg": "Out of range in json: param 'nrq' (17) should be in range [1, 16]"
            }
        },
        {
            "description": "String Type Test",
            "params": {"nrq": "4"},
            "expected": success
        },
        {
            "description": "Float Type Test",
            "params": {"nrq": 4.0},
            "expected": {
                "err_code": 1100,
                "err_msg": "wrong data type in json, key: 'nrq', value: '4.0': invalid parameter"
            }
        },
        {
            "description": "Boolean Type Test",
            "params": {"nrq": True},
            "expected": {
                "err_code": 1100,
                "err_msg": "invalid integer value, key: 'nrq', value: 'True': invalid parameter"
            }
        },
        {
            "description": "None Type Test",
            "params": {"nrq": None},
            "expected": success
        },
        {
            "description": "List Type Test",
            "params": {"nrq": [2]},
            "expected": {
                "err_code": 1100,
                "err_msg": "invalid integer value, key: 'nrq', value: '[2]': invalid parameter"
            }
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
            "description": "Invalid String Type Test",
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
        {
            "description": "refine_type lower precision than sq_type but refine disabled",
            "params": {"sq_type": "FP16", "refine_type": "SQ8"},
            "expected": success
        },
        {
            "description": "refine_type lower than sq_type",
            "params": {"sq_type": "FP16", "refine_type": "SQ8", "refine": True},
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
            "params": {"M": None, "efConstruction": None, "m": None, "nbits":None, "nrq":None,"refine": None, "refine_type": None},
            "expected": success
        },
        {
            "description": "Typical valid combination",
            "params": {"M": 16, "efConstruction": 200, "m": 32, "nbits": 8, "nrq":1,"refine": True, "refine_type": "FP16"},
            "expected": success
        },
        {
            "description": "Refine Disabled",
            "params": {"M": 16, "efConstruction": 200, "m": 32, "nbits": 8,"nrq": 1},
            "expected": success
        },
        {
            "description": "Minimum Boundary Combination",
            "params": {"M": 2, "efConstruction": 1, "m": 1, "nbits": 1, "nrq":1, "refine": True, "refine_type": "SQ8"},
            "expected": success
        },
        {
            "description": "Maximum Boundary Combination",
            "params": {"M": 2048, "efConstruction": 10000, "m": 128, "nbits": 8, "nrq":1, "refine": True, "refine_type": "FP32"},
            "expected": success
        },
        {
            "description": "Unknown extra parameter in combination",
            "params": {"M": 16, "efConstruction": 200, "m": 32, "nbits": 8, "nrq":1, "refine": True, "refine_type": "FP16", "unknown_param": "nothing"},
            "expected": success
        },
        {
            "description": "Partial parameters set (M + m only)",
            "params": {"M": 32, "m": 32},
            "expected": success
        },
        {
            "description": "Partial parameters set (efConstruction + nbits only)",
            "params": {"efConstruction": 500,"nbits": 8},
            "expected": success
        },
        {
            "description": "Invalid m (not divisor of dimension)",
            "params": {"M": 16,"efConstruction": 200,"m": 7, "nbits": 8, "refine": True, "refine_type": "FP32"},
            "expected": {"err_code": 1100, "err_msg": "The dimension of a vector (dim) should be a multiple of the number of subquantizers (m)."}
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
            "expected": success
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
