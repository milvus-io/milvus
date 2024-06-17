import requests
import argparse
from tenacity import retry, stop_after_attempt

@retry(stop=stop_after_attempt(7))
def get_image_tag_by_short_name(tag, arch):

    prefix = tag.split("-")[0]
    url = f"https://harbor.milvus.io/api/v2.0/projects/milvus/repositories/milvus/artifacts?with_tag=true&q=tags%253D~{prefix}-&page_size=100&page=1"

    payload = {}
    response = requests.request("GET", url, data=payload)
    rsp = response.json()
    tag_list = []
    for r in rsp:
        tags = r["tags"]
        for tag in tags:
            tag_list.append(tag["name"])
    tag_candidates = []
    for t in tag_list:
        r = t.split("-")
        if len(r) == 4 and arch in t:
            tag_candidates.append(t)
    tag_candidates.sort()
    if len(tag_candidates) == 0:
        return tag
    else:
        return tag_candidates[-1]

if __name__ == "__main__":
    argparse = argparse.ArgumentParser()
    argparse.add_argument("--tag", type=str, default="master-latest")
    argparse.add_argument("--arch", type=str, default="amd64")
    args = argparse.parse_args()
    res = get_image_tag_by_short_name(args.tag, args.arch)
    print(res)
