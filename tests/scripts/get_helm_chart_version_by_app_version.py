import subprocess
import json
def get_chart_version(repo = "milvus/milvus", app_version="2.2.0"):
    """
    Get helm chart version by app version
    """
    cmd = f"helm search repo {repo} -l -o json"
    result = subprocess.check_output(cmd, shell=True)
    result = json.loads(result)
    all_chart_versions = []
    for item in result:
        if item["app_version"] == app_version:
            all_chart_versions.append(item["version"])
    if len(all_chart_versions) == 0:
        raise Exception(f"Cannot find helm chart version for app version {app_version}")
    all_chart_versions.sort()
    result = all_chart_versions[-1]
    return result

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Get helm charts version by app version")
    parser.add_argument("--app-version", type=str, default="2.2.0", help="app version")
    parser.add_argument("--repo", type=str, default="milvus/milvus", help="helm repo")
    args = parser.parse_args()
    repo = args.repo
    app_version = args.app_version
    chart_version = get_chart_version(repo=repo, app_version=app_version)
    print(chart_version)