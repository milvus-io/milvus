import requests
import argparse

def get_image_tag_by_short_name(repository, tag):

    # Send API request to get all tags
    url = f"https://hub.docker.com/v2/repositories/{repository}/tags"
    response = requests.get(url)
    data = response.json()
    # Get the DIGEST of the "master-latest" tag
    digest = ""
    for tag_info in data["results"]:
        if tag_info["name"] == tag:
            digest = tag_info["images"][0]["digest"]
            break
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
