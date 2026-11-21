"""Run the public acceptance suite against a fresh, retained cluster_site.py directory."""

import argparse
from pathlib import Path

from acceptance import Fixture
from metadata import capture
from recovery import run as recover
from remap import check as reject_remap
from schema import run as schema
from sealed import check as sealed


def run(site):
    f = Fixture(site)
    f.run()
    sealed(f, f.candidate, f.mirror)
    cid = f.client.describe_collection(f.candidate, timeout=30)["collection_id"]
    files = capture(site, f.candidate + "-source")
    path = next(
        x["file"] for x in files if x.get("layout", {}).get("CollectionID") == cid and "/delta_log/" in x["file"]
    )
    reject_remap(Fixture(site), str(Path(path).parent))
    schema(Fixture(site))
    recover(Fixture(site), f.mirror)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("site", type=Path)
    run(parser.parse_args().site.resolve())
