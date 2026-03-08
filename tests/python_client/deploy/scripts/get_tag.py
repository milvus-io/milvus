import requests
import json

milvus_dev = "https://registry.hub.docker.com/v2/repositories/milvusdb/milvus/tags?ordering=last_updated"
milvus = "https://registry.hub.docker.com/v2/repositories/milvusdb/milvus/tags?ordering=last_updated"


def get_tag(url):
    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    res = response.json()["results"]
    sorted_r = sorted(res, key=lambda k: k['last_updated'])
    tags = [r["name"] for r in sorted_r]
    return tags


latest_tag = [tag for tag in get_tag(milvus_dev) if "latest" not in tag][-1]
latest_rc_tag = [tag for tag in get_tag(milvus) if "v" in tag][-1]
# release_version = "-".join(latest_rc_tag.split("-")[:-2])
# print(release_version)
print(latest_tag, latest_rc_tag)

data = {
    "latest_tag": latest_tag,
    "latest_rc_tag": latest_rc_tag,
    # "release_version": release_version
}
print(data)
with open("tag_info.json", "w") as f:
    f.write(json.dumps(data))
