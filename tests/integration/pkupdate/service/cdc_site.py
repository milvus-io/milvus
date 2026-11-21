"""Start a separate CDC role and a retained secondary in distinct loopback namespaces."""

import argparse
import json
import os
from pathlib import Path
from unittest.mock import patch

from cluster_site import create, free_ports, launch

BODY = "github.com/milvus-io/milvus/internal/proxy/replicate/pkReplicatedTxnBody"


def start(primary, root, binary):
    root.mkdir(exist_ok=False)
    m = json.loads((primary / "site.json").read_text())
    cdc = root / "cdc"
    cdc.mkdir()
    m["metrics"] = {"cdc": free_ports(1)[0]}
    m["binary"] = str(binary)
    (cdc / "site.json").write_text(json.dumps(m, indent=2))
    launch(cdc, "cdc")
    with patch.dict(os.environ, {"GO_FAILPOINTS": f'{BODY}=return("{root}/body.json")'}):
        create(root / "secondary", binary, Path(m["configs"]))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("primary", type=Path)
    p.add_argument("root", type=Path)
    p.add_argument("binary", type=Path)
    a = p.parse_args()
    start(a.primary.resolve(), a.root.resolve(), a.binary.resolve(strict=True))
