from pymilvus import DataType
from common import common_type as ct

success = "success"

class NGRAM:
    supported_field_types = [
        DataType.VARCHAR,
        DataType.JSON
    ]

    # Test parameter configurations
    build_params = [
        # min_gram parameter tests
        {
            "description": "min_gram value only specified",
            "params": {"min_gram": 2},
            "expected": {"err_code": 999, "err_msg": "Ngram index must specify both min_gram and max_gram"}
        },
        {
            "description": "min_gram - negative value",
            "params": {"min_gram": -1},
            "expected": {"err_code": 999, "err_msg": "Ngram index must specify both min_gram and max_gram"}
        },
        {
            "description": "min_gram - zero value",
            "params": {"min_gram": 0},
            "expected": {"err_code": 999, "err_msg": "Ngram index must specify both min_gram and max_gram"}
        },
        {
            "description": "Invalid min_gram - string type",
            "params": {"min_gram": "2"},
            "expected": {"err_code": 999, "err_msg": "Ngram index must specify both min_gram and max_gram"}
        },
        {
            "description": "Invalid min_gram - float type",
            "params": {"min_gram": 2.5},
            "expected": {"err_code": 999, "err_msg": "Ngram index must specify both min_gram and max_gram"}
        },
        {
            "description": "Invalid min_gram - None value",
            "params": {"min_gram": None},
            "expected": {"err_code": 999, "err_msg": "Ngram index must specify both min_gram and max_gram"}
        },
        
        # max_gram parameter tests
        {
            "description": "max_gram value only specified",
            "params": {"max_gram": 5},
            "expected": {"err_code": 999, "err_msg": "Ngram index must specify both min_gram and max_gram"}
        },
        {
            "description": "max_gram - negative value",
            "params": {"max_gram": -1},
            "expected": {"err_code": 999, "err_msg": "Ngram index must specify both min_gram and max_gram"}
        },
        {
            "description": "max_gram - zero value",
            "params": {"max_gram": 0},
            "expected": {"err_code": 999, "err_msg": "Ngram index must specify both min_gram and max_gram"}
        },
        {
            "description": "max_gram - string type",
            "params": {"max_gram": "3"},
            "expected": {"err_code": 999, "err_msg": "Ngram index must specify both min_gram and max_gram"}
        },
        {
            "description": "max_gram - float type",
            "params": {"max_gram": 2.5},
            "expected": {"err_code": 999, "err_msg": "Ngram index must specify both min_gram and max_gram"}
        },
        {
            "description": "max_gram - None value",
            "params": {"max_gram": None},
            "expected": {"err_code": 999, "err_msg": "Ngram index must specify both min_gram and max_gram"}
        },
        
        # min_gram and max_gram combination tests
        {
            "description": "min_gram equals max_gram",
            "params": {"min_gram": 2, "max_gram": 2},
            "expected": success
        },
        {
            "description": "min_gram less than max_gram",
            "params": {"min_gram": 2, "max_gram": 4},
            "expected": success
        },  
        {
            "description": "max_gram equals a large value",
            "params": {"min_gram": 2, "max_gram": 1000000000},
            "expected": success
        },  
        {
            "description": "min_gram greater than max_gram",
            "params": {"min_gram": 5, "max_gram": 3},
            "expected": {"err_code": 999, "err_msg": "invalid min_gram or max_gram value for Ngram index"}
        },
        # min_gram invalid with both specified
        {
            "description": "Invalid min_gram - negative value (both specified)",
            "params": {"min_gram": -1, "max_gram": 3},
            "expected": {"err_code": 999, "err_msg": "invalid min_gram or max_gram value for Ngram index"}
        },
        {
            "description": "Invalid min_gram - zero value (both specified)",
            "params": {"min_gram": 0, "max_gram": 3},
            "expected": {"err_code": 999, "err_msg": "Ngram index must specify both min_gram and max_gram"}
        },
        {
            "description": "Invalid min_gram - string type (both specified)",
            "params": {"min_gram": "2", "max_gram": 3},
            "expected": success
        },
        {
            "description": "Invalid min_gram - float type (both specified)",
            "params": {"min_gram": 2.5, "max_gram": 3},
            "expected": {"err_code": 999, "err_msg": "min_gram for Ngram index must be an integer, got: 2.5"}
        },
        {
            "description": "Invalid max_gram - string type (both specified)",
            "params": {"min_gram": 2, "max_gram": "3"},
            "expected": success
        },
        {
            "description": "Both parameters missing",
            "params": {},
            "expected": {"err_code": 999, "err_msg": "Ngram index must specify both min_gram and max_gram"}
        },
        
        # JSON field special parameter tests
        {
            "description": "JSON field with json_path parameter",
            "params": {
                "min_gram": 2, 
                "max_gram": 3,
                "json_path": "json_field['body']",
                "json_cast_type": "varchar"
            },
            "expected": success
        },
        {
            "description": "JSON field with enteir json field",
            "params": {
                "min_gram": 2, 
                "max_gram": 3,
                "json_path": "json_field",
                "json_cast_type": "varchar"
            },
            "expected": success
        },
        {
            "description": "JSON field with not existing path",
            "params": {
                "min_gram": 2, 
                "max_gram": 3,
                "json_path": "json_field['not_existing_path']",
                "json_cast_type": "varchar"
            },
            "expected": success
        },
        # skip for https://github.com/milvus-io/milvus/issues/43934
        # {
        #     "description": "JSON field with invalid json_cast_type",
        #     "params": {
        #         "min_gram": 2,
        #         "max_gram": 3,
        #         "json_path": "json_field['body']",
        #         "json_cast_type": "double"
        #     },
        #     "expected": {"err_code": 999, "err_msg": "json_cast_type must be varchar for NGRAM index"}
        # },
        {
            "description": "JSON field missing json_cast_type",
            "params": {
                "min_gram": 2, 
                "max_gram": 3,
                "json_path": "json_field['body']"
            },
            "expected": {"err_code": 999, "err_msg": "JSON field with ngram index must specify json_cast_type"}
        },
        {
            "description": "JSON field missing json_path",
            "params": {
                "min_gram": 2, 
                "max_gram": 3,
                "json_cast_type": "varchar"
            },
            "expected": success
        }
    ]
