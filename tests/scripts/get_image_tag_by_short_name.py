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
    candidate_tag = None
    for tag_info in sorted_images:
        # print(tag_info)
        if arch in [x["architecture"] for x in tag_info["images"]]:
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
                # print(f"candidate_tag: {candidate_tag}")
                break
    # Get the DIGEST of the short tag
    url = f"https://hub.docker.com/v2/repositories/{repository}/tags/{tag}"
    response = requests.get(url)
    cur_tag_info = response.json()
    digest = cur_tag_info["images"][0]["digest"]
    res = []
    # Iterate through all tags and find the ones with the same DIGEST
    for tag_info in data["results"]:
        try:
            if "digest" in tag_info["images"][0] and tag_info["images"][0]["digest"] == digest:
                # Extract the image name
                image_name = tag_info["name"].split(":")[0]
                if image_name != tag and arch in [x["architecture"] for x in tag_info["images"]]:
                    res.append(image_name)
        except Exception as e:
            continue
    # In case of no match, try to find the latest tag with the same arch
    # there is a case: push master-xxx-arm64 and master-latest, but master-latest-amd64 is not pushed,
    # then there will be no tag matched, so we need to find the latest tag with the same arch even it is not the latest tag
    for tag_info in data["results"]:
        image_name = tag_info["name"].split(":")[0]
        if image_name != tag and arch in image_name:
            res.append(image_name)
    # print(res)
    if len(res) == 0 or (candidate_tag is not None and candidate_tag > res[0]):
        if candidate_tag is None:
            return tag
        return candidate_tag
    else:
        return res[0]


if __name__ == "__main__":
    argparse = argparse.ArgumentParser()
    argparse.add_argument("--repository", type=str, default="milvusdb/milvus")
    argparse.add_argument("--tag", type=str, default="master-latest")
    argparse.add_argument("--arch", type=str, default="amd64")
    args = argparse.parse_args()
    res = get_image_tag_by_short_name(args.repository, args.tag, args.arch)
    print(res)
