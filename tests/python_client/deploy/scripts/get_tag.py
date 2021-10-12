import requests
import json

milvus_dev = "https://registry.hub.docker.com/v2/repositories/milvusdb/milvus-dev/tags?ordering=last_updated"
milvus = "https://registry.hub.docker.com/v2/repositories/milvusdb/milvus/tags?ordering=last_updated"


def get_tag(url):
    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    res = response.json()["results"]
    tags = [r["name"] for r in res]
    return tags


latest_tag = "master-latest"
latest_rc_tag = [tag for tag in sorted(get_tag(milvus)) if "rc" and "v" in tag][-1]
release_version = "-".join(latest_rc_tag.split("-")[:-2])
print(release_version)
print(latest_tag, latest_rc_tag)

data = {
    "latest_tag": latest_tag,
    "latest_rc_tag": latest_rc_tag[1:],
    "release_version": release_version
}
print(data)
with open("tag_info.json", "w") as f:
    f.write(json.dumps(data))
