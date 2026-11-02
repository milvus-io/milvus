"""Small PyUDF fixtures shared by CI tests."""

import errno
import io
import socket
import zipfile


def build_wheel(package, module):
    """Build a wheel containing only the supplied UDF module and entry point."""
    output = io.BytesIO()
    files = {
        f"{package}/__init__.py": module,
        f"{package}-0.0.1.dist-info/entry_points.txt": f"[milvus.pyudf]\nmain = {package}:create_udf\n",
        f"{package}-0.0.1.dist-info/METADATA": f"Metadata-Version: 2.1\nName: {package}\nVersion: 0.0.1\n",
    }
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_STORED) as wheel:
        for name, content in files.items():
            wheel.writestr(zipfile.ZipInfo(name), content)
    return output.getvalue()


def probe_endpoint(host, public_port=19530, worker_port=19090):
    """Check only the supplied address; a Service address does not prove Pod isolation."""
    observed = {}
    for label, port in (("public_api", public_port), ("worker_rpc", worker_port)):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as connection:
            connection.settimeout(2)
            observed[label] = {"port": port, "tcp_connect_code": connection.connect_ex((host, port))}
    assert observed["public_api"]["tcp_connect_code"] == 0, observed
    assert observed["worker_rpc"]["tcp_connect_code"] in {
        errno.ECONNREFUSED,
        errno.ETIMEDOUT,
        errno.EHOSTUNREACH,
        errno.ENETUNREACH,
        errno.EAGAIN,
    }, observed
    return {"observation": observed}
