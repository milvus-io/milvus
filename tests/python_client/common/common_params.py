from dataclasses import dataclass
from typing import List, Dict

""" Define param names"""


class IndexName:
    # Vector
    AUTOINDEX = "AUTOINDEX"
    FLAT = "FLAT"
    IVF_FLAT = "IVF_FLAT"
    IVF_SQ8 = "IVF_SQ8"
    IVF_PQ = "IVF_PQ"
    IVF_HNSW = "IVF_HNSW"
    HNSW = "HNSW"
    DISKANN = "DISKANN"
    SCANN = "SCANN"
    # binary
    BIN_FLAT = "BIN_FLAT"
    BIN_IVF_FLAT = "BIN_IVF_FLAT"
    # Sparse
    SPARSE_WAND = "SPARSE_WAND"
    SPARSE_INVERTED_INDEX = "SPARSE_INVERTED_INDEX"
    # GPU
    GPU_IVF_FLAT = "GPU_IVF_FLAT"
    GPU_IVF_PQ = "GPU_IVF_PQ"
    GPU_CAGRA = "GPU_CAGRA"
    GPU_BRUTE_FORCE = "GPU_BRUTE_FORCE"

    # Scalar
    INVERTED = "INVERTED"
    BITMAP = "BITMAP"
    Trie = "Trie"
    STL_SORT = "STL_SORT"


class MetricType:
    L2 = "L2"
    IP = "IP"
    COSINE = "COSINE"
    JACCARD = "JACCARD"


""" expressions """


@dataclass
class ExprBase:
    expr: str

    @property
    def subset(self):
        return f"({self.expr})"

    def __repr__(self):
        return self.expr


class Expr:
    # BooleanConstant: 'true' | 'True' | 'TRUE' | 'false' | 'False' | 'FALSE'

    @staticmethod
    def LT(left, right):
        return ExprBase(expr=f"{left} < {right}")

    @staticmethod
    def LE(left, right):
        return ExprBase(expr=f"{left} <= {right}")

    @staticmethod
    def GT(left, right):
        return ExprBase(expr=f"{left} > {right}")

    @staticmethod
    def GE(left, right):
        return ExprBase(expr=f"{left} >= {right}")

    @staticmethod
    def EQ(left, right):
        return ExprBase(expr=f"{left} == {right}")

    @staticmethod
    def NE(left, right):
        return ExprBase(expr=f"{left} != {right}")

    @staticmethod
    def like(left, right):
        return ExprBase(expr=f'{left} like "{right}"')

    @staticmethod
    def LIKE(left, right):
        return ExprBase(expr=f'{left} LIKE "{right}"')

    @staticmethod
    def exists(name):
        return ExprBase(expr=f'exists {name}')

    @staticmethod
    def EXISTS(name):
        return ExprBase(expr=f'EXISTS {name}')

    @staticmethod
    def ADD(left, right):
        return ExprBase(expr=f"{left} + {right}")

    @staticmethod
    def SUB(left, right):
        return ExprBase(expr=f"{left} - {right}")

    @staticmethod
    def MUL(left, right):
        return ExprBase(expr=f"{left} * {right}")

    @staticmethod
    def DIV(left, right):
        return ExprBase(expr=f"{left} / {right}")

    @staticmethod
    def MOD(left, right):
        return ExprBase(expr=f"{left} % {right}")

    @staticmethod
    def POW(left, right):
        return ExprBase(expr=f"{left} ** {right}")

    @staticmethod
    def SHL(left, right):
        # Note: not supported
        return ExprBase(expr=f"{left}<<{right}")

    @staticmethod
    def SHR(left, right):
        # Note: not supported
        return ExprBase(expr=f"{left}>>{right}")

    @staticmethod
    def BAND(left, right):
        # Note: not supported
        return ExprBase(expr=f"{left} & {right}")

    @staticmethod
    def BOR(left, right):
        # Note: not supported
        return ExprBase(expr=f"{left} | {right}")

    @staticmethod
    def BXOR(left, right):
        # Note: not supported
        return ExprBase(expr=f"{left} ^ {right}")

    @staticmethod
    def AND(left, right):
        return ExprBase(expr=f"{left} && {right}")

    @staticmethod
    def And(left, right):
        return ExprBase(expr=f"{left} and {right}")

    @staticmethod
    def OR(left, right):
        return ExprBase(expr=f"{left} || {right}")

    @staticmethod
    def Or(left, right):
        return ExprBase(expr=f"{left} or {right}")

    @staticmethod
    def BNOT(name):
        # Note: not supported
        return ExprBase(expr=f"~{name}")

    @staticmethod
    def NOT(name):
        return ExprBase(expr=f"!{name}")

    @staticmethod
    def Not(name):
        return ExprBase(expr=f"not {name}")

    @staticmethod
    def In(left, right):
        return ExprBase(expr=f"{left} in {right}")

    @staticmethod
    def Nin(left, right):
        return ExprBase(expr=f"{left} not in {right}")

    @staticmethod
    def json_contains(left, right):
        return ExprBase(expr=f"json_contains({left}, {right})")

    @staticmethod
    def JSON_CONTAINS(left, right):
        return ExprBase(expr=f"JSON_CONTAINS({left}, {right})")

    @staticmethod
    def json_contains_all(left, right):
        return ExprBase(expr=f"json_contains_all({left}, {right})")

    @staticmethod
    def JSON_CONTAINS_ALL(left, right):
        return ExprBase(expr=f"JSON_CONTAINS_ALL({left}, {right})")

    @staticmethod
    def json_contains_any(left, right):
        return ExprBase(expr=f"json_contains_any({left}, {right})")

    @staticmethod
    def JSON_CONTAINS_ANY(left, right):
        return ExprBase(expr=f"JSON_CONTAINS_ANY({left}, {right})")

    @staticmethod
    def array_contains(left, right):
        return ExprBase(expr=f"array_contains({left}, {right})")

    @staticmethod
    def ARRAY_CONTAINS(left, right):
        return ExprBase(expr=f"ARRAY_CONTAINS({left}, {right})")

    @staticmethod
    def array_contains_all(left, right):
        return ExprBase(expr=f"array_contains_all({left}, {right})")

    @staticmethod
    def ARRAY_CONTAINS_ALL(left, right):
        return ExprBase(expr=f"ARRAY_CONTAINS_ALL({left}, {right})")

    @staticmethod
    def array_contains_any(left, right):
        return ExprBase(expr=f"array_contains_any({left}, {right})")

    @staticmethod
    def ARRAY_CONTAINS_ANY(left, right):
        return ExprBase(expr=f"ARRAY_CONTAINS_ANY({left}, {right})")

    @staticmethod
    def array_length(name):
        return ExprBase(expr=f"array_length({name})")

    @staticmethod
    def ARRAY_LENGTH(name):
        return ExprBase(expr=f"ARRAY_LENGTH({name})")


"""" Define pass in params """


@dataclass
class BasePrams:
    @property
    def to_dict(self):
        return {k: v for k, v in vars(self).items() if v is not None}


@dataclass
class FieldParams(BasePrams):
    description: str = None

    # varchar
    max_length: int = None

    # array
    max_capacity: int = None

    # for vector
    dim: int = None

    # scalar
    is_primary: bool = None
    # auto_id: bool = None
    is_partition_key: bool = None
    is_clustering_key: bool = None


@dataclass
class IndexPrams(BasePrams):
    index_type: str = None
    params: dict = None
    metric_type: str = None


""" Define default params """


class DefaultVectorIndexParams:

    @staticmethod
    def FLAT(field: str, metric_type=MetricType.L2):
        return {field: IndexPrams(index_type=IndexName.FLAT, params={}, metric_type=metric_type)}

    @staticmethod
    def IVF_FLAT(field: str, nlist: int = 1024, metric_type=MetricType.L2):
        return {
            field: IndexPrams(index_type=IndexName.IVF_FLAT, params={"nlist": nlist}, metric_type=metric_type)
        }

    @staticmethod
    def IVF_SQ8(field: str, nlist: int = 1024, metric_type=MetricType.L2):
        return {
            field: IndexPrams(index_type=IndexName.IVF_SQ8, params={"nlist": nlist}, metric_type=metric_type)
        }

    @staticmethod
    def HNSW(field: str, m: int = 8, ef: int = 200, metric_type=MetricType.L2):
        return {
            field: IndexPrams(index_type=IndexName.HNSW, params={"M": m, "efConstruction": ef}, metric_type=metric_type)
        }

    @staticmethod
    def DISKANN(field: str, metric_type=MetricType.L2):
        return {field: IndexPrams(index_type=IndexName.DISKANN, params={}, metric_type=metric_type)}

    @staticmethod
    def BIN_FLAT(field: str, nlist: int = 1024, metric_type=MetricType.JACCARD):
        return {
            field: IndexPrams(index_type=IndexName.BIN_FLAT, params={"nlist": nlist}, metric_type=metric_type)
        }

    @staticmethod
    def BIN_IVF_FLAT(field: str, nlist: int = 1024, metric_type=MetricType.JACCARD):
        return {
            field: IndexPrams(index_type=IndexName.BIN_IVF_FLAT, params={"nlist": nlist},
                              metric_type=metric_type)
        }

    @staticmethod
    def SPARSE_WAND(field: str, drop_ratio_build: int = 0.2, metric_type=MetricType.IP):
        return {
            field: IndexPrams(index_type=IndexName.SPARSE_WAND, params={"drop_ratio_build": drop_ratio_build},
                              metric_type=metric_type)
        }

    @staticmethod
    def SPARSE_INVERTED_INDEX(field: str, drop_ratio_build: int = 0.2, metric_type=MetricType.IP):
        return {
            field: IndexPrams(index_type=IndexName.SPARSE_INVERTED_INDEX, params={"drop_ratio_build": drop_ratio_build},
                              metric_type=metric_type)
        }


class DefaultScalarIndexParams:

    @staticmethod
    def Default(field: str):
        return {field: IndexPrams()}

    @staticmethod
    def Trie(field: str):
        return {field: IndexPrams(index_type=IndexName.Trie)}

    @staticmethod
    def STL_SORT(field: str):
        return {field: IndexPrams(index_type=IndexName.STL_SORT)}

    @staticmethod
    def INVERTED(field: str):
        return {field: IndexPrams(index_type=IndexName.INVERTED)}

    @staticmethod
    def BITMAP(field: str):
        return {field: IndexPrams(index_type=IndexName.BITMAP)}

    @staticmethod
    def list_bitmap(fields: List[str]) -> Dict[str, IndexPrams]:
        return {n: IndexPrams(index_type=IndexName.BITMAP) for n in fields}
