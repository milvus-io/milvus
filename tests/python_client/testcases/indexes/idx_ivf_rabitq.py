from pymilvus import DataType
from common import common_type as ct

success = "success"


class IVF_RABITQ:

    supported_vector_types = [
        DataType.FLOAT_VECTOR,
        DataType.FLOAT16_VECTOR,
        DataType.BFLOAT16_VECTOR
    ]

    supported_metrics = ['L2', 'IP', 'COSINE']

    build_params = [
        # nlist params test
        {
            "description": "Minimum Boundary Test",
            "params": {"nlist": 1},
            "expected": success
        },
        {
            "description": "Maximum Boundary Test",
            "params": {"nlist": 65536},
            "expected": success
        },
        {
            "description": "Out of Range Test - Negative",
            "params": {"nlist": -1},
            "expected": {"err_code": 999, "err_msg": "param 'nlist' (-1) should be in range [1, 65536]"}
        },
        {
            "description": "Out of Range Test - Too Large",
            "params": {"nlist": 65537},
            "expected": {"err_code": 999, "err_msg": "param 'nlist' (65537) should be in range [1, 65536]"}
        },
        {
            "description": "String Type Test will ignore the wrong type",
            "params": {"nlist": "128"},
            "expected": success
        },
        {
            "description": "Float Type Test",
            "params": {"nlist": 128.0},
            "expected": {"err_code": 999,
                         "err_msg": "wrong data type in json"}
        },
        {
            "description": "Boolean Type Test",
            "params": {"nlist": True},
            "expected": {"err_code": 999,
                         "err_msg": "invalid integer value, key: 'nlist', value: 'True': invalid parameter"}
        },
        {
            "description": "None Type Test, use default value",
            "params": {"nlist": None},
            "expected": success
        },
        {
            "description": "List Type Test",
            "params": {"nlist": [128]},
            "expected": {"err_code": 999,
                         "err_msg": "invalid integer value, key: 'nlist', value: '[128]': invalid parameter"}
        },
        
        # refine params test
        {
            "description": "Enable Refine Test",
            "params": {"refine": 'true'},     # to be fixed: #41760
            "expected": success
        },
        {
            "description": "Disable Refine Test",
            "params": {"refine": 'false'},  # to be fixed: #41760
            "expected": success
        },

        # refine_type test
        {
            "description": "Refine Type Test",
            "params": {"refine_type": "PQ"},
            "expected": {"err_code": 999,
                         "err_msg": "invalid refine type : PQ, optional types are [sq6, sq8, fp16, bf16, fp32, flat]"}
        },
        {
            "description": "SQ6 Test",
            "params": {"refine": 'true', "refine_type": "SQ6"},
            "expected": success
        },
        {
            "description": "SQ8 Test",
            "params": {"refine": 'TRUE', "refine_type": "SQ8"},
            "expected": success
        },
        {
            "description": "FP16 Test",
            "params": {"refine": True, "refine_type": "FP16"},
            "expected": success
        },
        {
            "description": "BF16 Test",
            "params": {"refine": 'True', "refine_type": "BF16"},
            "expected": success
        },
        {
            "description": "FP32 Test",
            "params": {"refine": True, "refine_type": "FP32"},
            "expected": success
        },
        {
            "description": "Invalid Refine Type Test",
            "params": {"refine": 'true', "refine_type": "INVALID"},
            "expected": {"err_code": 999,
                         "err_msg": "invalid refine type : INVALID, optional types are [sq6, sq8, fp16, bf16, fp32, flat]"}
        },
        {
            "description": "Integer Type Test",
            "params": {"refine": 1},
            "expected": {"err_code": 999,
                         "err_msg": "Type conflict in json: param 'refine' (\"1\") should be a boolean"}
        },
        {
            "description": "None Type Test will success with default value",
            "params": {"refine": None},
            "expected": success
        },
        {
            "description": "Lowercase String Test",
            "params": {"refine": True, "refine_type": "sq6"},
            "expected": success
        },
        {
            "description": "Mixed Case String Test",
            "params": {"refine": True, "refine_type": "Sq8.0"},
            "expected": {"err_code": 999,
                         "err_msg": "invalid refine type : Sq8.0, optional types are [sq6, sq8, fp16, bf16, fp32, flat]"}
        },
        {
            "description": "Whitespace String Test",
            "params": {"refine_type": " SQ8 "},
            "expected": {"err_code": 999,
                         "err_msg": "invalid refine type :  SQ8 , optional types are [sq6, sq8, fp16, bf16, fp32, flat]"}
        },
        {
            "description": "Integer Type Test",
            "params": {"refine": True, "refine_type": 8},
            "expected": {"err_code": 999,
                         "err_msg": "invalid refine type : 8, optional types are [sq6, sq8, fp16, bf16, fp32, flat]"}
        },
        {
            "description": "None Type Test",
            "params": {"refine": True, "refine_type": None},
            "expected": success
        },

        # combination params test
        {
            "description": "Optimal Performance Combination Test",
            "params": {"nlist": 128, "refine": 'true', "refine_type": "SQ8"},
            "expected": success
        },
        {
            "description": "not refine with refine_type",
            "params": {"nlist": 127, "refine": 'false', "refine_type": "fp16"},
            "expected": success
        },
        {
            "description": "empty dict params",
            "params": {},
            "expected": success
        },
        {
            "description": "not_defined_param in the dict params",
            "params": {"nlist": 127, "refine": 'true', "not_defined_param": "nothing"},
            "expected": success
        },
        
    ]

    search_params = [
        # nprobe params test
        {
            "description": "Minimum Boundary Test",
            "params": {"nprobe": 1},
            "expected": success
        },
        {
            "description": "Equal to nlist Test",
            "params": {"nprobe": 128},  # Assuming nlist=128
            "expected": success
        },
        {
            "description": "Exceed nlist Test",
            "params": {"nprobe": 129},  # Assuming nlist=128
            "expected": success      # to be fixed: #41765
        },
        {
            "description": "Negative Value Test",
            "params": {"nprobe": -1},
            "expected": {"err_code": 999,
                         "err_msg": "Out of range in json: param 'nprobe' (-1) should be in range [1, 65536]"}
        },
        {
            "description": "String Type Test, not check data type",
            "params": {"nprobe": "32"},
            "expected": success         # to be fixed: #41767
        },
        {
            "description": "Float Type Test",
            "params": {"nprobe": 32.0},
            "expected": {"err_code": 999,
                         "err_msg": "Type conflict in json: param 'nprobe' (32.0) should be integer"}
        },
        {
            "description": "Boolean Type Test",
            "params": {"nprobe": True},
            "expected": {"err_code": 999,
                         "err_msg": "Type conflict in json: param 'nprobe' (true) should be integer"}
        },
        {
            "description": "None Type Test",
            "params": {"nprobe": None},
            "expected": {"err_code": 999,
                         "err_msg": "Type conflict in json: param 'nprobe' (null) should be integer"}
        },

        # rbq_bits_query test
       {
            "description": "Default Value Test",
            "params": {"rbq_bits_query": 0},
            "expected": success
        },
        {
            "description": "Maximum Value Test",
            "params": {"rbq_bits_query": 8},
            "expected": success
        },
        {
            "description": "Recommended Value Test - 6bit",
            "params": {"rbq_bits_query": 6},
            "expected": success
        },
        {
            "description": "Out of Range Test",
            "params": {"rbq_bits_query": 9},
            "expected": {"err_code": 999,
                         "err_msg": "Out of range in json: param 'rbq_bits_query' (9) should be in range [0, 8]"}
        },
        {
            "description": "Negative Value Test",
            "params": {"rbq_bits_query": -1},
            "expected": {"err_code": 999,
                         "err_msg": "Out of range in json: param 'rbq_bits_query' (-1) should be in range [0, 8]"}
        },
        {
            "description": "String Type Test",
            "params": {"rbq_bits_query": "6"},
            "expected": success        # to be fixed: #41767
        },
        {
            "description": "Float Type Test",
            "params": {"rbq_bits_query": 6.0},
            "expected": {"err_code": 999,
                         "err_msg": "Type conflict in json: param 'rbq_bits_query' (6.0) should be integer"}
        },
        {
            "description": "Boolean Type Test",
            "params": {"rbq_bits_query": True},
            "expected": {"err_code": 999,
                         "err_msg": "Type conflict in json: param 'rbq_bits_query' (true) should be integer"}
        },
        {
            "description": "None Type Test",
            "params": {"rbq_bits_query": None},
            "expected": {"err_code": 999,
                         "err_msg": "Type conflict in json: param 'rbq_bits_query' (null) should be integer"}
        },

        # refine_k test
        {
            "description": "Default Value Test",
            "params": {"refine_k": 1.0},
            "expected": success
        },
        {
            "description": "Recommended Value Test - 2",
            "params": {"refine_k": 2.0},
            "expected": success
        },
        {
            "description": "Recommended Value Test - 5",
            "params": {"refine_k": 5.0},
            "expected": success
        },
        {
            "description": "Less Than One Test",
            "params": {"refine_k": 0.5},
            "expected": {"err_code": 999,
                         "err_msg": "Out of range in json: param 'refine_k' (0.5) should be in range [1.000000, 340282346638528859811704183484516925440.000000]"}
        },
        {
            "description": "Negative Value Test",
            "params": {"refine_k": -1.0},
            "expected": {"err_code": 999,
                         "err_msg": "Out of range in json: param 'refine_k' (-1.0) should be in range [1.000000, 340282346638528859811704183484516925440.000000]"}
        },
        {
            "description": "String Type Test",
            "params": {"refine_k": "2.0"},
            "expected": success      # to be fixed: #41767
        },
        {
            "description": "Integer Type Test",
            "params": {"refine_k": 2},
            "expected": success     
        },
        {
            "description": "Boolean Type Test",
            "params": {"refine_k": True},
            "expected": {"err_code": 999,
                         "err_msg": "Type conflict in json: param 'refine_k' (true) should be a number"}
        },
        {
            "description": "None Type Test",
            "params": {"refine_k": None},
            "expected": {"err_code": 999,
                         "err_msg": "Type conflict in json: param 'refine_k' (null) should be a number"}
        },

        # combination params test
        {
            "description": "Optimal Performance Combination Test",
            "params": { "nprobe": 32, "rbq_bits_query": 6, "refine_k": 2.0},
            "expected": success
        },
        {
            "description": "Highest Recall Combination Test",
            "params": { "nprobe": 128, "rbq_bits_query": 0, "refine_k": 5.0},
            "expected": success
        },
        {
            "description": "empty dict params",
            "params": {},
            "expected": success
        },
        
    ]