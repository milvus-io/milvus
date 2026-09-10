from pymilvus import DataType

success = "success"


class FAISS:
    supported_vector_types = [
        DataType.FLOAT_VECTOR,
        DataType.BINARY_VECTOR,
    ]

    supported_metrics = ["L2", "IP", "COSINE"]

    build_params = [
        {
            "description": "Flat float index",
            "params": {"faiss_index_name": "Flat"},
            "expected": success,
        },
        {
            "description": "IVF Flat float index",
            "params": {"faiss_index_name": "IVF64,Flat"},
            "expected": success,
        },
        {
            "description": "HNSW Flat float index",
            "params": {"faiss_index_name": "HNSW16,Flat"},
            "expected": success,
        },
        {
            "description": "OPQ IVF PQ float index",
            "params": {"faiss_index_name": "OPQ16,IVF64,PQ16x4"},
            "expected": success,
        },
        {
            "description": "IVF PQ RFlat float index",
            "params": {"faiss_index_name": "IVF64,PQ8x4,RFlat"},
            "expected": success,
        },
        {
            "description": "PQ float index",
            "params": {"faiss_index_name": "PQ8x4"},
            "searchable": False,
            "expected": success,
        },
        {
            "description": "Binary flat index",
            "params": {"faiss_index_name": "BFlat"},
            "vector_data_type": DataType.BINARY_VECTOR,
            "metric_type": "HAMMING",
            "expected": success,
        },
    ]

    search_params = [
        {
            "description": "IVF Flat nprobe",
            "build_params": {"faiss_index_name": "IVF64,Flat"},
            "search_params": {"nprobe": 8},
            "expected": success,
        },
        {
            "description": "IVF Flat stringified nprobe",
            "build_params": {"faiss_index_name": "IVF64,Flat"},
            "search_params": {"nprobe": "8"},
            "expected": success,
        },
        {
            "description": "IVF Flat invalid nprobe string",
            "build_params": {"faiss_index_name": "IVF64,Flat"},
            "search_params": {"nprobe": "invalid"},
            "expected": {"err_code": 999, "err_msg": "expects a number"},
        },
        {
            "description": "HNSW Flat efSearch",
            "build_params": {"faiss_index_name": "HNSW16,Flat"},
            "search_params": {"efSearch": 64},
            "expected": success,
        },
        {
            "description": "HNSW Flat invalid efSearch string",
            "build_params": {"faiss_index_name": "HNSW16,Flat"},
            "search_params": {"efSearch": "invalid"},
            "expected": {"err_code": 999, "err_msg": "expects a number"},
        },
        {
            "description": "IVF PQ RFlat rerank",
            "build_params": {"faiss_index_name": "IVF64,PQ8x4,RFlat"},
            "search_params": {"nprobe": 8, "k_factor": 4},
            "expected": success,
        },
        {
            "description": "IVF PQ RFlat invalid k_factor string",
            "build_params": {"faiss_index_name": "IVF64,PQ8x4,RFlat"},
            "search_params": {"nprobe": 8, "k_factor": "invalid"},
            "expected": {"err_code": 999, "err_msg": "expects a number"},
        },
    ]

    metric_factories = [
        {"faiss_index_name": "IVF64,Flat"},
        {"faiss_index_name": "HNSW16,Flat"},
    ]
