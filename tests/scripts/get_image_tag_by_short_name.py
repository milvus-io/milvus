import requests
import argparse
from tenacity import retry, stop_after_attempt

@retry(stop=stop_after_attempt(7))
def get_image_tag_by_short_name(repository, tag, arch):

    # Send API request to get all tags start with prefix
    # ${branch}-latest  means the tag is a dev build
        # master-latest -> master-$date-$commit
        # 2.3.0-latest -> 2.3.0-$date-$commit
    # latest means the tag is a release build
        # latest -> v$version
    splits = tag.split("-")
    prefix = f"{splits[0]}-" if len(splits) > 1 else "v"
    url = f"https://hub.docker.com/v2/repositories/{repository}/tags?name={prefix}&ordering=last_updated"
    response = requests.get(url)
    data = response.json()

    # Get the latest tag with the same arch and prefix
    sorted_images = sorted(data["results"], key=lambda x: x["last_updated"], reverse=True)
    # print(sorted_images)
    candidate_tag = None
    for tag_info in sorted_images:
        # print(tag_info)
        if tag == "2.2.0-latest": # special case for 2.2.0-latest, for 2.2.0 branch, there is no arm amd and gpu as suffix
            candidate_tag = tag_info["name"]
        else:
            if arch in tag_info["name"]:
                candidate_tag = tag_info["name"]
            else:
                continue
        if candidate_tag == tag:
            continue
        else:
            return candidate_tag


if __name__ == "__main__":
    argparse = argparse.ArgumentParser()
    argparse.add_argument("--repository", type=str, default="milvusdb/milvus")
    argparse.add_argument("--tag", type=str, default="master-latest")
    argparse.add_argument("--arch", type=str, default="amd64")
    args = argparse.parse_args()
    res = get_image_tag_by_short_name(args.repository, args.tag, args.arch)
    print(res)
