import copy
from .utils import *
from .constants import *

uid = "describe_collection"


class TestDescribeCollection:
    """
    ******************************************************************
      The following cases are used to test `describe_collection` function
    ******************************************************************
    """
    def test_describe_collection(self, connect):
        '''
        target: test describe collection
        method: create collection then describe the same collection
        expected: returned value is the same
        '''
        collection_name = gen_unique_str(uid)
        df = copy.deepcopy(default_fields)
        df["fields"].append({"name": "int32", "type": DataType.INT32})

        connect.create_collection(collection_name, df)
        info = connect.describe_collection(collection_name)
        assert info.get("collection_name") == collection_name
        assert len(info.get("fields")) == 4

        for field in info.get("fields"):
            assert field.get("name") in ["int32", "int64", "float", "float_vector"]
            if field.get("name") == "float_vector":
                assert field.get("params").get("dim") == str(default_dim)
