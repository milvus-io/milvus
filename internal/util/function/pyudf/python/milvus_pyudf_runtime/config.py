# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Only explicit startup arguments are consumed; no Milvus YAML or env reader."""

from __future__ import annotations

import argparse
import ipaddress
from dataclasses import dataclass

MAX_ERROR_MESSAGE_BYTES = 8 << 10
MAX_PARAM_NESTING_DEPTH = 64
MAX_INT32 = (1 << 31) - 1


def require_int(name: str, value: int, minimum: int, maximum: int) -> None:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer in [{minimum}, {maximum}]")


def validate_address(address: str) -> None:
    if not isinstance(address, str) or address.count(":") != 1:
        raise ValueError("address must be a concrete IPv4 literal and port")
    host, port = address.split(":")
    ip = ipaddress.IPv4Address(host)
    if ip.is_unspecified or ip.is_multicast or str(ip) == "255.255.255.255":
        raise ValueError("address must be a concrete IPv4 literal")
    if not port.isascii() or not port.isdecimal() or str(int(port)) != port or not 1 <= int(port) <= 65535:
        raise ValueError("address port must be a canonical integer in 1..65535")


@dataclass(frozen=True)
class ServerConfig:
    address: str
    worker_count: int
    grpc_concurrency: int
    max_concurrent_rpcs: int
    max_message_bytes: int
    shutdown_timeout_ms: int

    def validate(self) -> None:
        validate_address(self.address)
        require_int("worker_count", self.worker_count, 1, MAX_INT32)
        require_int("grpc_concurrency", self.grpc_concurrency, 1, MAX_INT32)
        require_int("max_concurrent_rpcs", self.max_concurrent_rpcs, 1, MAX_INT32)
        require_int("max_message_bytes", self.max_message_bytes, 1 << 20, 1 << 30)
        require_int("shutdown_timeout_ms", self.shutdown_timeout_ms, 5000, 60000)


def parse_startup_args(argv: list[str]) -> ServerConfig:
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--address", required=True)
    for name in (
        "worker-count",
        "grpc-concurrency",
        "max-concurrent-rpcs",
        "max-message-bytes",
        "shutdown-timeout-ms",
    ):
        parser.add_argument(f"--{name}", type=int, required=True)
    config = ServerConfig(**vars(parser.parse_args(argv)))
    try:
        config.validate()
    except (TypeError, ValueError) as exc:
        parser.error(str(exc))
    return config
