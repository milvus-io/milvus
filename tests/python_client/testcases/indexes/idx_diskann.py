from pymilvus import DataType
from common import common_type as ct

success = "success"


class DISKANN:
    supported_vector_types = [
        DataType.FLOAT_VECTOR,
        DataType.FLOAT16_VECTOR,
        DataType.BFLOAT16_VECTOR
    ]

    supported_metrics = ['L2', 'IP', 'COSINE']

    build_params = [
        # search_list_size
        # Type: Integer Range: [1, int_max]
        # Default value: 100
        {"description": "Minimum Boundary Test", "params": {"search_list_size": 1}, "expected": success},
        {"description": "Large Value Test", "params": {"search_list_size": 10000}, "expected": success},
        {"description": "Out of Range Test - Negative", "params": {"search_list_size": -1}, "expected": success},
        {"description": "String Type Test", "params": {"search_list_size": "100"}, "expected": success},
        {"description": "Float Type Test", "params": {"search_list_size": 100.0}, "expected": success},
        {"description": "Boolean Type Test", "params": {"search_list_size": True}, "expected": success},
        {"description": "None Type Test", "params": {"search_list_size": None}, "expected": success},
        # search_cache_budget_gb_ratio
        # Type: Float Range: [0.0, 0.3)
        # Default value: 0.10
        # TODO: runt he minium bourndary test after issue #43176 fixed
        # {"description": "Minimum Boundary Test", "params": {"search_cache_budget_gb_ratio": 0.0}, "expected": success},
        {"description": "Maximum Boundary Test", "params": {"search_cache_budget_gb_ratio": 0.3}, "expected": success},
        {"description": "Default value Test", "params": {"search_cache_budget_gb_ratio": 0.1}, "expected": success},
        {"description": "Out of Range Test - Negative", "params": {"search_cache_budget_gb_ratio": -0.1}, "expected": success},
        {"description": "Out of Range Test - Too Large", "params": {"search_cache_budget_gb_ratio": 0.31}, "expected": success},
        {"description": "String Type Test", "params": {"search_cache_budget_gb_ratio": "0.2"}, "expected": success},
        {"description": "Boolean Type Test", "params": {"search_cache_budget_gb_ratio": True}, "expected": success},
        {"description": "None Type Test", "params": {"search_cache_budget_gb_ratio": None}, "expected": success},
        # pq_code_budget_gb_ratio
        # Type: Float Range: (0.0, 0.25]
        # Default value: 0.125
        {"description": "Minimum Boundary Test", "params": {"pq_code_budget_gb_ratio": 0.0001}, "expected": success},
        {"description": "Maximum Boundary Test", "params": {"pq_code_budget_gb_ratio": 0.25}, "expected": success},
        {"description": "Default value Test", "params": {"pq_code_budget_gb_ratio": 0.125}, "expected": success},
        {"description": "Out of Range Test - Negative", "params": {"pq_code_budget_gb_ratio": -0.1}, "expected": success},
        {"description": "Out of Range Test - Too Large", "params": {"pq_code_budget_gb_ratio": 0.26}, "expected": success},
        {"description": "String Type Test", "params": {"pq_code_budget_gb_ratio": "0.1"}, "expected": success},
        {"description": "Boolean Type Test", "params": {"pq_code_budget_gb_ratio": True}, "expected": success},
        {"description": "None Type Test", "params": {"pq_code_budget_gb_ratio": None}, "expected": success},
        # max_degree
        # Type: Integer Range: [1, 512]
        # Default value: 56
        {"description": "Minimum Boundary Test", "params": {"max_degree": 1}, "expected": success},
        {"description": "Maximum Boundary Test", "params": {"max_degree": 512}, "expected": success},
        {"description": "Default value Test", "params": {"max_degree": 56}, "expected": success},
        {"description": "Large Value Test", "params": {"max_degree": 128}, "expected": success},
        {"description": "Out of Range Test - Negative", "params": {"max_degree": -1}, "expected": success},
        {"description": "String Type Test", "params": {"max_degree": "32"}, "expected": success},
        {"description": "Float Type Test", "params": {"max_degree": 32.0}, "expected": success},
        {"description": "Boolean Type Test", "params": {"max_degree": True}, "expected": success},
        {"description": "None Type Test", "params": {"max_degree": None}, "expected": success},
        # 组合参数
        {"description": "Optimal Performance Combination Test", "params": {"search_list_size": 100, "beamwidth": 10, "search_cache_budget_gb_ratio": 0.5, "pq_code_budget_gb_ratio": 0.5}, "expected": success},
        {"description": "empty dict params", "params": {}, "expected": success},
        {"description": "not_defined_param in the dict params", "params": {"search_list_size": 100, "not_defined_param": "nothing"}, "expected": success},

    ]

    search_params = [
        # beam_width_ratio
        # Type: Float Range: [1, max(128 / CPU number, 16)]
        # Default value: 4.0
        {"description": "Minimum Boundary Test", "params": {"beam_width_ratio": 1.0}, "expected": success},
        {"description": "Maximum Boundary Test", "params": {"beam_width_ratio": 16.0}, "expected": success},
        {"description": "Default value Test", "params": {"beam_width_ratio": 4.0}, "expected": success},
        {"description": "Out of Range Test - Negative", "params": {"beam_width_ratio": -0.1}, "expected": success},
        {"description": "Out of Range Test - Too Large", "params": {"beam_width_ratio": 17.0}, "expected": success},
        {"description": "String Type Test", "params": {"beam_width_ratio": "2.0"}, "expected": success},
        {"description": "Boolean Type Test", "params": {"beam_width_ratio": True}, "expected": success},
        {"description": "None Type Test", "params": {"beam_width_ratio": None}, "expected": success},
        # search_list_size
        # Type: Integer Range: [1, int_max]
        # Default value: 100
        {"description": "Minimum Boundary Test", "params": {"search_list_size": 1}, "expected": {"err_code": 999, "err_msg": "search_list_size(1) should be larger than k(10)"}},
        {"description": "Large Value Test", "params": {"search_list_size": 1000}, "expected": success},
        {"description": "Default value Test", "params": {"search_list_size": 100}, "expected": success},
        {"description": "Out of Range Test - Negative", "params": {"search_list_size": -1}, "expected": {"err_code": 999, "err_msg": "param 'search_list_size' (-1) should be in range [1, 2147483647]"}},
        {"description": "String Type Test", "params": {"search_list_size": "100"}, "expected": success},
        {"description": "Float Type Test", "params": {"search_list_size": 100.0}, "expected": {"err_code": 999, "err_msg": "Type conflict in json: param 'search_list_size' (100.0) should be integer"}},
        {"description": "Boolean Type Test", "params": {"search_list_size": True}, "expected": {"err_code": 999, "err_msg": "Type conflict in json: param 'search_list_size' (true) should be integer"}},
        {"description": "None Type Test", "params": {"search_list_size": None}, "expected": {"err_code": 999, "err_msg": "Type conflict in json: param 'search_list_size' (null) should be integer"}},
        # mix params
        {"description": "mix params", "params": {"search_list_size": 100, "beam_width_ratio": 0.5}, "expected": success},
        {"description": "mix params", "params": {}, "expected": success},
    ] 