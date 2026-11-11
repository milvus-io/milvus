"""Small PyUDF fixtures shared by CI tests."""

import io
import zipfile


def build_wheel(package, module, *, entry_points=None):
    """Build a wheel containing only the supplied UDF module and entry point."""
    output = io.BytesIO()
    files = {
        f"{package}/__init__.py": module,
        f"{package}-0.0.1.dist-info/entry_points.txt": (
            f"[milvus.pyudf]\nmain = {package}:create_udf\n" if entry_points is None else entry_points
        ),
        f"{package}-0.0.1.dist-info/METADATA": f"Metadata-Version: 2.1\nName: {package}\nVersion: 0.0.1\n",
    }
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_STORED) as wheel:
        for name, content in files.items():
            wheel.writestr(zipfile.ZipInfo(name), content)
    return output.getvalue()


def assert_pyudf_deadline(call):
    import grpc
    import pytest
    from pymilvus import MilvusException

    with pytest.raises((MilvusException, grpc.RpcError)) as caught:
        call()
    if isinstance(caught.value, grpc.RpcError):
        assert caught.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED, caught.value
    else:
        assert caught.value.code == 10001, caught.value
