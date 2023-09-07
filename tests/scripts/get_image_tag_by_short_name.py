import requests
import argparse

def get_image_tag_by_short_name(repository, tag):

    # Send API request to get all tags start with prefix
    # ${branch}-latest  means the tag is a dev build
        # master-latest -> master-$date-$commit
        # 2.3.0-latest -> 2.3.0-$date-$commit
    # latest means the tag is a release build
        # latest -> v$version
    splits = tag.split("-")
    prefix = splits[0] if len(splits) > 1 else "v"
    url = f"https://hub.docker.com/v2/repositories/{repository}/tags?name={prefix}&ordering=last_updated"
    response = requests.get(url)
    data = response.json()
    # Get the DIGEST of the short tag
    digest = ""
    url = f"https://hub.docker.com/v2/repositories/{repository}/tags/{tag}"
    response = requests.get(url)
    cur_tag_info = response.json()
    digest = cur_tag_info["images"][0]["digest"]
    res = []        
    # Iterate through all tags and find the ones with the same DIGEST
    for tag_info in data["results"]:
        if "digest" in tag_info["images"][0] and tag_info["images"][0]["digest"] == digest:
            # Extract the image name
            image_name = tag_info["name"].split(":")[0]
            if image_name != tag:
                res.append(image_name)
    assert len(res) > 0
    return res[0]

if __name__ == "__main__":
    argparse = argparse.ArgumentParser()
    argparse.add_argument("--repository", type=str, default="milvusdb/milvus")
    argparse.add_argument("--tag", type=str, default="master-latest")
    args = argparse.parse_args()
    res = get_image_tag_by_short_name(args.repository, args.tag)
    print(res)
