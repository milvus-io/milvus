from pymilvus import DataType

success = "success"


class FMINDEX:
    # FM-index is an exact byte-level substring index for VARCHAR. JSON string
    # paths, TEXT and ARRAY are follow-ups (rejected at create_index for now).
    supported_field_types = [
        DataType.VARCHAR,
    ]

    # Build-parameter test configurations. fm_sa_sample_rate is optional
    # (defaults to 8) and, when present, must be an integer in [4, 256].
    build_params = [
        {
            "description": "no optional params (defaults applied)",
            "params": {},
            "expected": success,
        },
        {
            "description": "default sample rate",
            "params": {"fm_sa_sample_rate": 8},
            "expected": success,
        },
        {
            "description": "minimum sample rate",
            "params": {"fm_sa_sample_rate": 4},
            "expected": success,
        },
        {
            "description": "maximum sample rate",
            "params": {"fm_sa_sample_rate": 256},
            "expected": success,
        },
        {
            "description": "sample rate below minimum",
            "params": {"fm_sa_sample_rate": 3},
            "expected": {"err_code": 1100, "err_msg": "fm_sa_sample_rate for FM-index must be in"},
        },
        {
            "description": "sample rate above maximum",
            "params": {"fm_sa_sample_rate": 257},
            "expected": {"err_code": 1100, "err_msg": "fm_sa_sample_rate for FM-index must be in"},
        },
        {
            "description": "sample rate not an integer",
            "params": {"fm_sa_sample_rate": "abc"},
            "expected": {"err_code": 1100, "err_msg": "fm_sa_sample_rate for FM-index must be an integer"},
        },
        # fm_block_bytes is optional (rank-directory block granularity, default
        # 64) and, when present, must be a power of two in [8, 128].
        {
            "description": "default block bytes",
            "params": {"fm_block_bytes": 64},
            "expected": success,
        },
        {
            "description": "minimum block bytes",
            "params": {"fm_block_bytes": 8},
            "expected": success,
        },
        {
            "description": "maximum block bytes",
            "params": {"fm_block_bytes": 128},
            "expected": success,
        },
        {
            "description": "block bytes not a power of two",
            "params": {"fm_block_bytes": 24},
            "expected": {"err_code": 1100, "err_msg": "fm_block_bytes for FM-index must be a power of two"},
        },
        {
            "description": "block bytes above maximum",
            "params": {"fm_block_bytes": 256},
            "expected": {"err_code": 1100, "err_msg": "fm_block_bytes for FM-index must be a power of two"},
        },
    ]
