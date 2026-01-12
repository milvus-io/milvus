"""
Mock TEI (Text Embeddings Inference) Server for testing.

This module provides utilities to mock TEI API using pytest-httpserver.
It can be used to test scenarios where the embedding service becomes unavailable
after a collection function has been created.

TEI API Reference:
- POST /embed: Generate embeddings for input texts
  - Request: {"inputs": ["text1", "text2"], "truncate": true, "truncation_direction": "Left"}
  - Response: [[0.1, 0.2, ...], [0.3, 0.4, ...]]

Usage with pytest-httpserver (recommended):
    @pytest.fixture
    def mock_tei(httpserver):
        return MockTEIHandler(httpserver, dim=768)

    def test_example(mock_tei):
        mock_tei.setup_embed()
        endpoint = mock_tei.endpoint
        # use endpoint...

        # Simulate error
        mock_tei.setup_error(503, "Service unavailable")

Usage with standalone server (for environments without pytest-httpserver):
    server = MockTEIServer(dim=768)
    server.start()
    endpoint = server.endpoint
    server.set_error_mode(True)
    server.stop()
"""

import json
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional, Callable, Any
import socket


def generate_mock_embedding(text: str, dim: int) -> list:
    """Generate a deterministic mock embedding based on text content."""
    hash_val = hash(text) & 0xFFFFFFFF
    embedding = []
    for i in range(dim):
        val = ((hash_val * (i + 1)) % 10000) / 10000.0 * 2 - 1
        embedding.append(round(val, 6))
    return embedding


# =============================================================================
# pytest-httpserver based implementation (recommended)
# =============================================================================

class MockTEIHandler:
    """
    TEI mock handler for pytest-httpserver.

    This is the recommended way to mock TEI in pytest tests.

    Example:
        def test_with_tei(httpserver):
            tei = MockTEIHandler(httpserver, dim=768)
            tei.setup_embed()

            # Your test code using tei.endpoint
            ...

            # Simulate service failure
            tei.setup_error(503, "Model integration is not active")
    """

    def __init__(self, httpserver, dim: int = 768):
        """
        Initialize TEI handler.

        Args:
            httpserver: pytest-httpserver's HTTPServer fixture
            dim: Embedding dimension
        """
        self.httpserver = httpserver
        self.dim = dim

    @property
    def endpoint(self) -> str:
        """Get the server endpoint URL."""
        return self.httpserver.url_for("")

    def setup_embed(self, permanent: bool = True):
        """
        Setup /embed endpoint to return mock embeddings.

        Args:
            permanent: If True, handler persists across requests
        """
        def handle_embed(request):
            data = request.json
            inputs = data.get("inputs", [])
            embeddings = [generate_mock_embedding(text, self.dim) for text in inputs]
            return json.dumps(embeddings)

        handler = self.httpserver.expect_request(
            "/embed",
            method="POST"
        )
        if permanent:
            handler = handler.respond_with_handler(handle_embed)
        else:
            handler = handler.respond_with_handler(handle_embed)

        return self

    def setup_error(self, status_code: int = 500, message: str = "Service unavailable"):
        """
        Setup server to return errors for all requests.

        Args:
            status_code: HTTP status code
            message: Error message
        """
        self.httpserver.clear()

        error_response = json.dumps({"error": message})

        self.httpserver.expect_request(
            "/embed",
            method="POST"
        ).respond_with_data(
            error_response,
            status=status_code,
            content_type="application/json"
        )

        return self

    def setup_health(self):
        """Setup /health endpoint."""
        self.httpserver.expect_request(
            "/health",
            method="GET"
        ).respond_with_json({"status": "ok"})

        return self

    def clear(self):
        """Clear all handlers."""
        self.httpserver.clear()
        return self


# =============================================================================
# Standalone server implementation (fallback for environments without pytest-httpserver)
# =============================================================================

def create_handler_class(server_state: dict):
    """Create a handler class with instance-specific state."""

    class _StandaloneHandler(BaseHTTPRequestHandler):
        """HTTP request handler for standalone mock TEI server."""

        def log_message(self, format, *args):
            pass

        def _send_json(self, data, status: int = 200):
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(data).encode('utf-8'))

        def do_POST(self):
            if server_state.get('error_mode', False):
                self._send_json(
                    {"error": server_state.get('error_message', 'Service unavailable')},
                    server_state.get('error_status_code', 500)
                )
                return

            if self.path == '/embed':
                content_length = int(self.headers.get('Content-Length', 0))
                body = json.loads(self.rfile.read(content_length).decode('utf-8'))
                inputs = body.get('inputs', [])
                dim = server_state.get('dim', 768)
                embeddings = [generate_mock_embedding(text, dim) for text in inputs]
                self._send_json(embeddings)
            else:
                self._send_json({"error": "Not found"}, 404)

        def do_GET(self):
            if server_state.get('error_mode', False):
                self._send_json(
                    {"error": server_state.get('error_message', 'Service unavailable')},
                    server_state.get('error_status_code', 500)
                )
                return

            if self.path == '/health':
                self._send_json({"status": "ok"})
            else:
                self._send_json({"error": "Not found"}, 404)

    return _StandaloneHandler


def get_local_ip() -> str:
    """
    Get the local IP address that can be accessed from external hosts.
    Returns the first non-loopback IPv4 address.
    """
    # Method 1: get IP from socket connection to external host
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        s.close()
        if not ip.startswith('127.'):
            return ip
    except Exception:
        pass

    # Method 2: get from hostname
    try:
        hostname = socket.gethostname()
        ip = socket.gethostbyname(hostname)
        if not ip.startswith('127.'):
            return ip
    except Exception:
        pass

    return '127.0.0.1'


def get_docker_host() -> str:
    """
    Get the hostname that Docker containers can use to access the host machine.

    - macOS/Windows Docker Desktop: host.docker.internal
    - Linux: returns the host's IP address (containers need --add-host or host network)
    """
    import platform
    system = platform.system().lower()

    if system in ('darwin', 'windows'):
        # Docker Desktop provides this special DNS name
        return 'host.docker.internal'
    else:
        # Linux: use host IP
        return get_local_ip()


class MockTEIServer:
    """
    Standalone mock TEI server.

    Use this when pytest-httpserver is not available.
    For pytest tests, prefer using MockTEIHandler with httpserver fixture.

    Example:
        with MockTEIServer(dim=768) as server:
            endpoint = server.endpoint
            # use endpoint...

            server.set_error_mode(True, 503, "Service unavailable")

    For remote Milvus access, use external_host parameter:
        # Auto-detect external IP
        server = MockTEIServer(dim=768, host='0.0.0.0', external_host='auto')

        # For Docker container access (macOS/Windows)
        server = MockTEIServer(dim=768, host='0.0.0.0', external_host='docker')

        # Or specify explicit IP
        server = MockTEIServer(dim=768, host='0.0.0.0', external_host='192.168.1.100')
    """

    def __init__(self, port: int = 0, dim: int = 768, host: str = '127.0.0.1', external_host: str = None):
        self.host = host
        self.port = port
        self.dim = dim
        self._external_host = external_host
        self._server: Optional[HTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False
        # Instance-specific state (not shared between servers)
        self._state = {
            'dim': dim,
            'error_mode': False,
            'error_status_code': 500,
            'error_message': 'Service unavailable'
        }

    @property
    def endpoint(self) -> str:
        if self._server is None:
            raise RuntimeError("Server not started")
        # Use external_host for endpoint URL if specified
        if self._external_host:
            if self._external_host == 'auto':
                host = get_local_ip()
            elif self._external_host == 'docker':
                host = get_docker_host()
            else:
                host = self._external_host
        else:
            host = self.host
        return f"http://{host}:{self._server.server_address[1]}"

    def start(self) -> str:
        if self._running:
            return self.endpoint

        # Create handler class with instance-specific state
        handler_class = create_handler_class(self._state)

        self._server = HTTPServer((self.host, self.port), handler_class)
        self.port = self._server.server_address[1]

        self._thread = threading.Thread(target=self._server.serve_forever)
        self._thread.daemon = True
        self._thread.start()
        self._running = True

        self._wait_for_server()
        return self.endpoint

    def _wait_for_server(self, timeout: float = 5.0):
        start = time.time()
        while time.time() - start < timeout:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1)
                if sock.connect_ex((self.host, self.port)) == 0:
                    sock.close()
                    return
                sock.close()
            except Exception:
                pass
            time.sleep(0.1)
        raise RuntimeError(f"Server failed to start within {timeout}s")

    def stop(self):
        if self._server:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None
        self._running = False

    def set_error_mode(self, enabled: bool, status_code: int = 500, message: str = "Service unavailable"):
        self._state['error_mode'] = enabled
        self._state['error_status_code'] = status_code
        self._state['error_message'] = message

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()
        return False


# =============================================================================
# Pytest fixtures
# =============================================================================

def pytest_httpserver_fixture(dim: int = 768):
    """
    Create a pytest fixture for MockTEIHandler.

    Usage in conftest.py:
        from common.mock_tei_server import pytest_httpserver_fixture

        @pytest.fixture
        def mock_tei(httpserver):
            handler = MockTEIHandler(httpserver, dim=768)
            handler.setup_embed()
            yield handler
    """
    def fixture(httpserver):
        handler = MockTEIHandler(httpserver, dim=dim)
        handler.setup_embed()
        return handler
    return fixture


if __name__ == '__main__':
    import urllib.request
    import urllib.error

    print("Testing standalone MockTEIServer...")
    with MockTEIServer(port=8080, dim=768) as server:
        print(f"Server: {server.endpoint}")

        # Test embed
        req = urllib.request.Request(
            f"{server.endpoint}/embed",
            data=json.dumps({"inputs": ["Hello", "World"]}).encode(),
            headers={'Content-Type': 'application/json'}
        )
        with urllib.request.urlopen(req) as resp:
            result = json.loads(resp.read())
            print(f"Embed: {len(result)} vectors, dim={len(result[0])}")

        # Test error mode
        server.set_error_mode(True, 503, "Model integration is not active")
        try:
            urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            print(f"Error mode: {e.code} - {json.loads(e.read())}")

    print("Done!")
